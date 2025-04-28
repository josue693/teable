import { Injectable, Logger, BadRequestException, ForbiddenException } from '@nestjs/common';
import type { IFieldRo } from '@teable/core';
import {
  FieldType,
  getTableImportChannel,
  getRandomString,
  getActionTriggerChannel,
} from '@teable/core';
import { PrismaService } from '@teable/db-main-prisma';
import type {
  IAnalyzeRo,
  IImportOptionRo,
  IInplaceImportOptionRo,
  IImportColumn,
  ITableFullVo,
} from '@teable/openapi';
import { difference } from 'lodash';
import { ClsService } from 'nestjs-cls';
import type { CreateOp } from 'sharedb';
import type { LocalPresence } from 'sharedb/lib/client';

import { ShareDbService } from '../../../share-db/share-db.service';
import type { IClsStore } from '../../../types/cls';
import { NotificationService } from '../../notification/notification.service';
import { DEFAULT_VIEWS, DEFAULT_FIELDS } from '../../table/constant';
import { TableOpenApiService } from '../../table/open-api/table-open-api.service';
import { TABLE_IMPORT_CSV_QUEUE, ImportTableCsvQueueProcessor } from './import-csv.processor';
import { importerFactory } from './import.class';
import type { CsvImporter, ExcelImporter } from './import.class';

@Injectable()
export class ImportOpenApiService {
  private logger = new Logger(ImportOpenApiService.name);
  constructor(
    private readonly tableOpenApiService: TableOpenApiService,
    private readonly cls: ClsService<IClsStore>,
    private readonly prismaService: PrismaService,
    private readonly notificationService: NotificationService,
    private readonly shareDbService: ShareDbService,
    private readonly importTableCsvQueueProcessor: ImportTableCsvQueueProcessor
  ) {}

  async analyze(analyzeRo: IAnalyzeRo) {
    const { attachmentUrl, fileType } = analyzeRo;

    const importer = importerFactory(fileType, {
      url: attachmentUrl,
      type: fileType,
    });

    return await importer.genColumns();
  }

  async createTableFromImport(baseId: string, importRo: IImportOptionRo, maxRowCount?: number) {
    const userId = this.cls.get('user.id');
    const { attachmentUrl, fileType, worksheets, notification = false, tz } = importRo;

    const importer = importerFactory(fileType, {
      url: attachmentUrl,
      type: fileType,
      maxRowCount,
    });

    // only record base table info, not include records
    const tableResult = [];

    for (const [sheetKey, value] of Object.entries(worksheets)) {
      const { importData, useFirstRowAsHeader, columns, name } = value;

      const columnInfo = columns.length ? columns : [...DEFAULT_FIELDS];
      const fieldsRo = columnInfo.map((col, index) => {
        const result: IFieldRo & {
          isPrimary?: boolean;
        } = {
          ...col,
        };

        if (index === 0) {
          result.isPrimary = true;
        }

        // Date Field should have default tz
        if (col.type === FieldType.Date) {
          result.options = {
            formatting: {
              timeZone: tz,
              date: 'YYYY-MM-DD',
              time: 'None',
            },
          };
        }

        return result;
      });

      let table: ITableFullVo;

      try {
        // create table with column
        table = await this.tableOpenApiService.createTable(baseId, {
          name: name,
          fields: fieldsRo,
          views: DEFAULT_VIEWS,
          records: [],
        });

        tableResult.push(table);
      } catch (e) {
        this.logger.error(e);
        throw e;
      }

      const { fields } = table;

      // if columns is empty, then skip import data
      importData &&
        columns.length &&
        this.importRecords(
          baseId,
          table,
          userId,
          importer,
          { skipFirstNLines: useFirstRowAsHeader ? 1 : 0, sheetKey, notification },
          {
            columnInfo: columns,
            fields: fields.map((f) => ({ id: f.id, type: f.type })),
          }
        );
    }
    return tableResult;
  }

  async inplaceImportTable(
    baseId: string,
    tableId: string,
    inplaceImportRo: IInplaceImportOptionRo,
    maxRowCount?: number,
    projection?: string[]
  ) {
    const userId = this.cls.get('user.id');
    const { attachmentUrl, fileType, insertConfig, notification = false } = inplaceImportRo;

    const { sourceColumnMap, sourceWorkSheetKey, excludeFirstRow } = insertConfig;

    const tableRaw = await this.prismaService.tableMeta
      .findUnique({
        where: { id: tableId, deletedTime: null },
        select: { name: true },
      })
      .catch(() => {
        throw new BadRequestException('table is not found');
      });

    const fieldRaws = await this.prismaService.field.findMany({
      where: { tableId, deletedTime: null, hasError: null },
      select: {
        id: true,
        type: true,
      },
    });

    if (projection) {
      const inplaceFieldIds = Object.keys(sourceColumnMap);
      const noUpdateFields = difference(inplaceFieldIds, projection);
      if (noUpdateFields.length !== 0) {
        const tips = noUpdateFields.join(',');
        throw new ForbiddenException(`There is no permission to update there field ${tips}`);
      }
    }

    if (!tableRaw || !fieldRaws) {
      return;
    }

    const importer = importerFactory(fileType, {
      url: attachmentUrl,
      type: fileType,
      maxRowCount,
    });

    this.importRecords(
      baseId,
      { id: tableId, name: tableRaw.name },
      userId,
      importer,
      { skipFirstNLines: excludeFirstRow ? 1 : 0, sheetKey: sourceWorkSheetKey, notification },
      {
        sourceColumnMap,
        fields: fieldRaws as { id: string; type: FieldType }[],
      }
    );
  }

  private importRecords(
    baseId: string,
    table: { id: string; name: string },
    userId: string,
    importer: CsvImporter | ExcelImporter,
    options: { skipFirstNLines: number; sheetKey: string; notification: boolean },
    recordsCal: {
      columnInfo?: IImportColumn[];
      fields: { id: string; type: FieldType }[];
      sourceColumnMap?: Record<string, number | null>;
    }
  ) {
    const { sheetKey, notification } = options;
    const { columnInfo, fields, sourceColumnMap } = recordsCal;
    const localPresence = this.createImportPresence(table.id);
    this.setImportStatus(localPresence, true);

    // mark this import all jobs
    const jobIdPrefix = `${ImportTableCsvQueueProcessor.JOB_ID_PREFIX}:${getRandomString(10)}`;

    let recordCursor = 1;
    importer.parse(
      {
        key: options.sheetKey,
        skipFirstNLines: options.skipFirstNLines,
      },
      async (chunk: Record<string, unknown[][]>, lastChunk?: boolean) => {
        const currentRecords = chunk[sheetKey];
        const currentRange = [recordCursor, recordCursor + currentRecords.length - 1] as [
          number,
          number,
        ];
        recordCursor += currentRecords.length;
        const jobId = `${jobIdPrefix}_${getRandomString(6)}`;
        await this.importTableCsvQueueProcessor.queue.add(
          `${TABLE_IMPORT_CSV_QUEUE}_job`,
          {
            userId,
            chunk,
            columnInfo,
            fields,
            sourceColumnMap,
            sheetKey,
            table,
            baseId,
            range: currentRange,
            notification,
            lastChunk,
          },
          {
            jobId,
            removeOnComplete: true,
            removeOnFail: true,
          }
        );
      },
      // finished add job to queue
      () => {
        this.logger.log(`All chunk tasks have been added to queue for ${table.id}:${table.name}`);
      },
      // error, now for queue way to import, mostly causing by over plan row count
      (message: string) => {
        this.logger.error(`Import ${table.id}:${table.name} failed causing: ${message}`);
        this.setImportStatus(localPresence, false);
        notification &&
          this.notificationService.sendImportResultNotify({
            baseId,
            tableId: table.id,
            toUserId: userId,
            message: `❌ ${table.name} import failed: ${message}`,
          });
        this.updateRowCount(table.id);
      }
    );
  }

  private setImportStatus(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    presence: LocalPresence<any>,
    loading: boolean
  ) {
    presence.submit(
      {
        loading,
      },
      (error) => {
        error && this.logger.error(error);
      }
    );
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

  private createImportPresence(tableId: string) {
    const channel = getTableImportChannel(tableId);
    const presence = this.shareDbService.connect().getPresence(channel);
    return presence.create(channel);
  }
}
