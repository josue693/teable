/* eslint-disable @typescript-eslint/naming-convention */
import { InjectQueue, OnWorkerEvent, Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import {
  FieldKeyType,
  FieldType,
  getActionTriggerChannel,
  getTableImportChannel,
} from '@teable/core';
import type { IImportColumn } from '@teable/openapi';
import { Job, Queue } from 'bullmq';
import { toString } from 'lodash';
import { ClsService } from 'nestjs-cls';
import type { CreateOp } from 'sharedb';
import type { LocalPresence } from 'sharedb/lib/client';
import { EventEmitterService } from '../../../event-emitter/event-emitter.service';
import { Events } from '../../../event-emitter/events';
import { ShareDbService } from '../../../share-db/share-db.service';
import type { IClsStore } from '../../../types/cls';
import { NotificationService } from '../../notification/notification.service';
import { RecordOpenApiService } from '../../record/open-api/record-open-api.service';
import { parseBoolean } from './import.class';

interface ITableImportCsvJob {
  baseId: string;
  userId: string;
  chunk: Record<string, unknown[][]>;
  sheetKey: string;
  columnInfo?: IImportColumn[];
  fields: { id: string; type: FieldType }[];
  sourceColumnMap?: Record<string, number | null>;
  table: { id: string; name: string };
  range: [number, number];
  notification?: boolean;
  lastChunk?: boolean;
}

export const TABLE_IMPORT_CSV_QUEUE = 'import-table-csv-queue';

@Injectable()
@Processor(TABLE_IMPORT_CSV_QUEUE)
export class ImportTableCsvQueueProcessor extends WorkerHost {
  public static readonly JOB_ID_PREFIX = 'import-table-csv';

  private logger = new Logger(ImportTableCsvQueueProcessor.name);
  private timer: NodeJS.Timeout | null = null;
  private readonly TIMER_INTERVAL = 5000;

  constructor(
    private readonly recordOpenApiService: RecordOpenApiService,
    private readonly shareDbService: ShareDbService,
    private readonly notificationService: NotificationService,
    private readonly eventEmitterService: EventEmitterService,
    private readonly cls: ClsService<IClsStore>,
    @InjectQueue(TABLE_IMPORT_CSV_QUEUE) public readonly queue: Queue<ITableImportCsvJob>
  ) {
    super();
  }

  private startQueueTimer() {
    if (this.timer) {
      return;
    }
    this.logger.log(`Starting import table queue timer with interval: ${this.TIMER_INTERVAL}ms`);
    this.timer = setInterval(async () => await this.refreshTableRowCount(), this.TIMER_INTERVAL);
  }

  private async refreshTableRowCount() {
    // it means there still processing, so they need update rowCount
    const waitList = [
      ...new Set(
        (await this.queue.getJobs('waiting'))
          .filter((job) => job.id?.startsWith(ImportTableCsvQueueProcessor.JOB_ID_PREFIX))
          .map((job) => job.data.table.id)
          .filter((id) => id)
      ),
    ];

    if (waitList.length) {
      waitList.forEach((tableId) => {
        this.updateRowCount(tableId);
      });
    }
  }

  public async process(job: Job<ITableImportCsvJob>) {
    const jobId = String(job.id);

    const { table, notification, baseId, userId, lastChunk, sourceColumnMap, range } = job.data;
    const localPresence = this.createImportPresence(table.id);

    try {
      await this.handleImportChunkCsv(job);
      if (lastChunk) {
        notification &&
          this.notificationService.sendImportResultNotify({
            baseId,
            tableId: table.id,
            toUserId: userId,
            message: `🎉 ${table.name} ${sourceColumnMap ? 'inplace' : ''} imported successfully`,
          });

        this.eventEmitterService.emitAsync(Events.IMPORT_TABLE_COMPLETE, {
          baseId,
          tableId: table.id,
        });
        this.setImportStatus(localPresence, false);
        this.updateRowCount(table.id);
      }
    } catch (error) {
      const err = error as Error;
      notification &&
        this.notificationService.sendImportResultNotify({
          baseId,
          tableId: table.id,
          toUserId: userId,
          message: `❌ ${table.name} import aborted: ${err.message} fail row range: [${range}]. Please check the data for this range and retry.`,
        });

      await this.cleanRelativeTask(jobId);

      throw err;
    }
  }

  private async cleanRelativeTask(jobId: string) {
    const [sameBatchJobPrefix] = jobId.split('_');
    const waitingJobs = await this.queue.getJobs(['waiting', 'active']);
    await Promise.all(
      waitingJobs.filter((job) => job.id?.startsWith(sameBatchJobPrefix)).map((job) => job.remove())
    );
  }

  private async handleImportChunkCsv(job: Job<ITableImportCsvJob>) {
    await this.cls.run(async () => {
      this.cls.set('user.id', job.data.userId);
      const { chunk, sheetKey, columnInfo, fields, sourceColumnMap, table } = job.data;
      const currentResult = chunk[sheetKey];
      // fill data
      const records = currentResult.map((row) => {
        const res: { fields: Record<string, unknown> } = {
          fields: {},
        };
        // import new table
        if (columnInfo) {
          columnInfo.forEach((col, index) => {
            const { sourceColumnIndex, type } = col;
            // empty row will be return void row value
            const value = Array.isArray(row) ? row[sourceColumnIndex] : null;
            res.fields[fields[index].id] =
              type === FieldType.Checkbox ? parseBoolean(value) : value?.toString();
          });
        }
        // inplace records
        if (sourceColumnMap) {
          for (const [key, value] of Object.entries(sourceColumnMap)) {
            if (value !== null) {
              const { type } = fields.find((f) => f.id === key) || {};
              // link value should be string
              res.fields[key] = type === FieldType.Link ? toString(row[value]) : row[value];
            }
          }
        }
        return res;
      });
      if (records.length === 0) {
        return;
      }
      try {
        const createFn = columnInfo
          ? this.recordOpenApiService.createRecordsOnlySql.bind(this.recordOpenApiService)
          : this.recordOpenApiService.multipleCreateRecords.bind(this.recordOpenApiService);
        await createFn(table.id, {
          fieldKeyType: FieldKeyType.Id,
          typecast: true,
          records,
        });
      } catch (e: unknown) {
        this.logger.error(e);
        throw e;
      }
    });
  }

  private updateRowCount(tableId: string) {
    const channel = getActionTriggerChannel(tableId);
    const presence = this.shareDbService.connect().getPresence(channel);
    const localPresence = presence.create(tableId);
    localPresence.submit([{ actionKey: 'addRecord' }], (error) => {
      error && this.logger.error(error);
    });

    const updateEmptyOps = {
      src: 'unknown',
      seq: 1,
      m: {
        ts: Date.now(),
      },
      create: {
        type: 'json0',
        data: undefined,
      },
      v: 0,
    } as CreateOp;
    this.shareDbService.publishRecordChannel(tableId, updateEmptyOps);
  }

  private setImportStatus(presence: LocalPresence<unknown>, loading: boolean) {
    presence.submit(
      {
        loading,
      },
      (error) => {
        error && this.logger.error(error);
      }
    );
  }

  private createImportPresence(tableId: string) {
    const channel = getTableImportChannel(tableId);
    const presence = this.shareDbService.connect().getPresence(channel);
    return presence.create(channel);
  }

  @OnWorkerEvent('active')
  onWorkerEvent(job: Job) {
    const { table, range } = job.data;
    this.logger.log(`import data to ${table.id} job started, range: [${range}]`);
    this.startQueueTimer();
  }

  @OnWorkerEvent('error')
  onError(job: Job) {
    const { table, range } = job.data;
    this.logger.error(`import data to ${table.id} job failed, range: [${range}]`);
  }

  @OnWorkerEvent('completed')
  async onCompleted(job: Job) {
    const { table, range } = job.data;
    this.logger.log(`import data to ${table.id} job completed, range: [${range}]`);

    const allJobs = (await this.queue.getJobs(['waiting', 'active'])).filter((job) =>
      job.id?.startsWith(ImportTableCsvQueueProcessor.JOB_ID_PREFIX)
    );

    if (!allJobs.length && this.timer) {
      this.logger.log('No more import tasks, clearing timer...');
      // last task, clear timer
      clearInterval(this.timer);
    }
  }
}
