/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable sonarjs/no-duplicate-string */
/* eslint-disable sonarjs/cognitive-complexity */
import type { INestApplication } from '@nestjs/common';
import type { IAttachmentItem, IConditionalRollupFieldOptions, IFilter } from '@teable/core';
import { FieldKeyType, FieldType, SortFunc, ViewType } from '@teable/core';
import type { INotifyVo, ITableFullVo } from '@teable/openapi';
import {
  createField,
  getFields,
  installViewPlugin,
  exportBase,
  importBase,
  getTableList,
  createBase,
  createDashboard,
  installPlugin,
  createPluginPanel,
  installPluginPanel,
  getDashboardList,
  getDashboard,
  listPluginPanels,
  getPluginPanel,
  getPluginPanelPlugin,
  getViewList,
} from '@teable/openapi';
import { pick } from 'lodash';
import type { ClsStore } from 'nestjs-cls';
import { ClsService } from 'nestjs-cls';
import { EventEmitterService } from '../src/event-emitter/event-emitter.service';
import { Events } from '../src/event-emitter/events';
import { AttachmentsService } from '../src/features/attachments/attachments.service';
import { replaceStringByMap } from '../src/features/base/utils';
import { x_20 } from './data-helpers/20x';
import { x_20_link, x_20_link_from_lookups } from './data-helpers/20x-link';
import { createAwaitWithEventWithResult } from './utils/event-promise';

import {
  createTable,
  permanentDeleteTable,
  initApp,
  getViews,
  getTable,
  permanentDeleteBase,
  getRecords,
  getRecord,
} from './utils/init-app';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForComputedRecord(
  tableId: string,
  recordId: string,
  fieldIds: string[],
  timeoutMs = 8000
) {
  const start = Date.now();
  let latestRecord = await getRecord(tableId, recordId);
  while (Date.now() - start < timeoutMs) {
    const hasAllValues = fieldIds.every((fieldId) => latestRecord.fields?.[fieldId] !== undefined);
    if (hasAllValues) {
      return latestRecord;
    }
    await sleep(200);
    latestRecord = await getRecord(tableId, recordId);
  }
  return latestRecord;
}

async function waitForRecordWithFieldValue(
  tableId: string,
  fieldId: string,
  expectedValue: unknown,
  timeoutMs = 8000
) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const records = await getRecords(tableId, {
      fieldKeyType: FieldKeyType.Id,
    });
    const matched = records.records.find((record) => record.fields?.[fieldId] === expectedValue);
    if (matched) {
      return matched;
    }
    await sleep(200);
  }
  return undefined;
}

function getAttachmentService(app: INestApplication) {
  return app.get<AttachmentsService>(AttachmentsService);
}

describe('OpenAPI BaseController for base import (e2e)', () => {
  let app: INestApplication;
  let appUrl: string;
  let sourceBaseId: string;
  const spaceId = globalThis.testConfig.spaceId;
  const userId = globalThis.testConfig.userId;
  let eventEmitterService: EventEmitterService;
  let awaitWithEvent: <T>(fn: () => Promise<T>) => Promise<{ previewUrl: string }>;

  beforeAll(async () => {
    const appCtx = await initApp();
    app = appCtx.app;
    appUrl = appCtx.appUrl;
  });

  afterAll(async () => {
    await app.close();
  });

  describe('export table and import the table', () => {
    let table: ITableFullVo;
    let subTable: ITableFullVo;

    // let duplicateTableData: IDuplicateTableVo;
    beforeAll(async () => {
      const sourceBase = (
        await createBase({
          name: 'source_base',
          spaceId: spaceId,
          icon: '😄',
        })
      ).data;
      sourceBaseId = sourceBase.id;
      table = await createTable(sourceBase.id, {
        name: 'record_query_x_20',
        fields: x_20.fields,
        records: x_20.records,
      });

      const x20Link = x_20_link(table);
      subTable = await createTable(sourceBaseId, {
        name: 'lookup_filter_x_20',
        fields: x20Link.fields,
        records: x20Link.records,
      });
      eventEmitterService = app.get(EventEmitterService);

      const x20LinkFromLookups = x_20_link_from_lookups(table, subTable.fields[2].id);
      for (const field of x20LinkFromLookups.fields) {
        await createField(subTable.id, field);
      }

      awaitWithEvent = createAwaitWithEventWithResult<{ previewUrl: string }>(
        eventEmitterService,
        Events.BASE_EXPORT_COMPLETE
      );

      // dashboard init
      const dashboard = (await createDashboard(sourceBaseId, { name: 'dashboard' })).data;
      const dashboard2 = (await createDashboard(sourceBaseId, { name: 'dashboard2' })).data;

      await installPlugin(sourceBaseId, dashboard.id, {
        name: 'plugin1',
        pluginId: 'plgchart',
      });

      await installPlugin(sourceBaseId, dashboard.id, {
        name: 'plugin2',
        pluginId: 'plgchart',
      });

      await installPlugin(sourceBaseId, dashboard2.id, {
        name: 'plugin2_1',
        pluginId: 'plgchart',
      });

      // pluginViews init
      await installViewPlugin(table.id, { name: 'sheetView1', pluginId: 'plgsheetform' });
      await installViewPlugin(table.id, { name: 'sheetView2', pluginId: 'plgsheetform' });

      // pluginPanel init
      const panel = (await createPluginPanel(table.id, { name: 'panel1' })).data;
      const panel2 = (await createPluginPanel(table.id, { name: 'panel2' })).data;

      await installPluginPanel(table.id, panel.id, {
        name: 'plugin1',
        pluginId: 'plgchart',
      });

      await installPluginPanel(table.id, panel.id, {
        name: 'plugin2',
        pluginId: 'plgchart',
      });

      await installPluginPanel(table.id, panel2.id, {
        name: 'plugin2_1',
        pluginId: 'plgchart',
      });

      table.fields = (await getFields(table.id)).data;
      table.views = await getViews(table.id);
      subTable.fields = (await getFields(subTable.id)).data;
      subTable.views = await getViews(subTable.id);
    });
    afterAll(async () => {
      await permanentDeleteTable(sourceBaseId, table.id);
      await permanentDeleteTable(sourceBaseId, subTable.id);
    });
    it('should export table and import the table', async () => {
      const { previewUrl: url } = await awaitWithEvent(async () => {
        await exportBase(sourceBaseId);
      });
      const previewUrl = appUrl + url;

      const clsService = app.get(ClsService);

      const attachmentService = getAttachmentService(app);

      const notify = await clsService.runWith<Promise<IAttachmentItem>>(
        {
          // eslint-disable-next-line
          user: {
            id: userId,
            name: 'Test User',
            email: 'test@example.com',
            isAdmin: null,
          },
        } as unknown as ClsStore,
        async () => {
          return await attachmentService.uploadFromUrl(previewUrl);
        }
      );

      const { base, tableIdMap, viewIdMap, fieldIdMap } = (
        await importBase({
          notify: {
            ...(notify as unknown as INotifyVo),
          },
          spaceId: spaceId,
        })
      ).data;

      expect(base.spaceId).toBe(spaceId);

      const tableList = (await getTableList(base.id)).data;

      expect(tableList.length).toBe(2);

      const table1 = await getTable(base.id, tableList[0].id, {
        includeContent: true,
      });
      const table2 = await getTable(base.id, tableList[1].id, {
        includeContent: true,
      });

      const table1Fields = table1.fields!;
      const table2Fields = table2.fields!;

      const table1Views = table1.views!;
      const table2Views = table2.views!;

      // fields
      expect(table1Fields.length).toBe(table.fields.length);
      expect(table2Fields.length).toBe(subTable.fields.length);
      const testFieldProperties = [
        'cellValueType',
        'dbFieldName',
        'dbFieldType',
        'description',
        'isLookup',
        'isPrimary',
        'name',
        'unique',
        'notNull',
        'type',
      ];

      const duplicatedTable1Fields = table1Fields.map((field) => pick(field, testFieldProperties));
      const duplicatedTable2Fields = table2Fields.map((field) => pick(field, testFieldProperties));

      const sourceTable1Fields = table.fields.map((field) => pick(field, testFieldProperties));
      const sourceTable2Fields = subTable.fields.map((field) => pick(field, testFieldProperties));

      expect(duplicatedTable1Fields).toEqual(sourceTable1Fields);
      expect(duplicatedTable2Fields).toEqual(sourceTable2Fields);

      const testViewProperties = [
        'id',
        'columnMeta',
        'filter',
        'sort',
        'group',
        'options',
        'pluginInstall',
        'order',
      ];

      const duplicatedTable1Views = table1Views.map((view) => pick(view, testViewProperties));
      const duplicatedTable2Views = table2Views.map((view) => pick(view, testViewProperties));

      const sourceTable1Views = table.views
        .map((view) => pick(view, testViewProperties))
        .map((v) => {
          const res = replaceStringByMap(v, {
            tableIdMap,
            viewIdMap,
            fieldIdMap,
          });
          return res ? JSON.parse(res) : v;
        });
      const sourceTable2Views = subTable.views
        .map((view) => pick(view, testViewProperties))
        .map((v) => {
          const res = replaceStringByMap(v, {
            tableIdMap,
            viewIdMap,
            fieldIdMap,
          });
          return res ? JSON.parse(res) : v;
        });

      // views
      expect(table1Views.length).toBe(table.views.length);
      expect(table2Views.length).toBe(subTable.views.length);

      expect(duplicatedTable1Views).toEqual(sourceTable1Views);
      expect(duplicatedTable2Views).toEqual(sourceTable2Views);

      // plugins
      // dashboard
      const sourceDashboardList = (await getDashboardList(sourceBaseId)).data;
      const dashboardList = (await getDashboardList(base.id)).data;
      expect(dashboardList.length).toBe(sourceDashboardList.length);
      expect(sourceDashboardList.map((d) => d.name)).toEqual(dashboardList.map((d) => d.name));

      const sourceDashboard1Info = (await getDashboard(sourceBaseId, sourceDashboardList[0].id))
        .data;
      const dashboard1Info = (await getDashboard(base.id, dashboardList[0].id)).data;

      const sourceDashboard2Info = (await getDashboard(sourceBaseId, sourceDashboardList[1].id))
        .data;
      const dashboard2Info = (await getDashboard(base.id, dashboardList[1].id)).data;

      const layoutProperties = ['h', 'w', 'x', 'y'];

      expect(sourceDashboard1Info.layout?.map((l) => pick(l, layoutProperties))).toEqual(
        dashboard1Info.layout?.map((l) => pick(l, layoutProperties))
      );

      expect(sourceDashboard2Info.layout?.map((l) => pick(l, layoutProperties))).toEqual(
        dashboard2Info.layout?.map((l) => pick(l, layoutProperties))
      );

      // panel
      const panelList = (await listPluginPanels(table.id)).data;

      const panel1Info = (
        await getPluginPanel(table.id, panelList.find(({ name }) => name === 'panel1')!.id)
      ).data;

      const installedPlugins = (
        await getPluginPanelPlugin(
          table.id,
          panelList.find(({ name }) => name === 'panel1')!.id,
          panel1Info.layout![0].pluginInstallId
        )
      ).data;

      expect(installedPlugins.name).toBe('plugin1');
      // pluginViews
      const views = (await getViewList(table.id)).data;

      const pluginViews = views.filter(({ type }) => type === ViewType.Plugin);
      expect(pluginViews.length).toBe(2);

      expect(pluginViews.find(({ name }) => name === 'sheetView1')).toBeDefined();
      expect(pluginViews.find(({ name }) => name === 'sheetView2')).toBeDefined();

      for (const tableId of Object.values(tableIdMap)) {
        await permanentDeleteTable(base.id, tableId);
      }
    });
  });

  describe('conditional rollup import', () => {
    let conditionalBaseId: string;
    let importedBaseId: string | undefined;
    let foreignTable: ITableFullVo;
    let hostTable: ITableFullVo;
    let awaitConditionalExport: <T>(fn: () => Promise<T>) => Promise<{ previewUrl: string }>;

    beforeAll(async () => {
      const base = (
        await createBase({
          name: 'conditional_rollup_source',
          spaceId,
          icon: '🧮',
        })
      ).data;
      conditionalBaseId = base.id;

      foreignTable = await createTable(conditionalBaseId, {
        name: 'CR_Foreign',
        fields: [
          { name: 'Title', type: FieldType.SingleLineText },
          { name: 'Status', type: FieldType.SingleLineText },
        ],
        records: [
          { fields: { Title: 'Alpha', Status: 'Active' } },
          { fields: { Title: 'Beta', Status: 'Inactive' } },
        ],
      });

      hostTable = await createTable(conditionalBaseId, {
        name: 'CR_Host',
        fields: [{ name: 'StatusFilter', type: FieldType.SingleLineText }],
        records: [{ fields: { StatusFilter: 'Active' } }, { fields: { StatusFilter: 'Inactive' } }],
      });

      const titleFieldId = foreignTable.fields.find((field) => field.name === 'Title')!.id;
      const statusFieldId = foreignTable.fields.find((field) => field.name === 'Status')!.id;
      const statusFilterFieldId = hostTable.fields.find(
        (field) => field.name === 'StatusFilter'
      )!.id;

      const statusMatchFilter: IFilter = {
        conjunction: 'and',
        filterSet: [
          {
            fieldId: statusFieldId,
            operator: 'is',
            value: { type: 'field', fieldId: statusFilterFieldId },
          },
        ],
      };

      await createField(hostTable.id, {
        name: 'Status Rollup',
        type: FieldType.ConditionalRollup,
        options: {
          foreignTableId: foreignTable.id,
          lookupFieldId: titleFieldId,
          expression: 'array_join({values})',
          filter: statusMatchFilter,
        } as IConditionalRollupFieldOptions,
      });

      await createField(hostTable.id, {
        name: 'Status Lookup',
        type: FieldType.SingleLineText,
        isLookup: true,
        isConditionalLookup: true,
        lookupOptions: {
          foreignTableId: foreignTable.id,
          lookupFieldId: titleFieldId,
          filter: statusMatchFilter,
          sort: { fieldId: titleFieldId, order: SortFunc.Asc },
          limit: 1,
        },
      });

      awaitConditionalExport = createAwaitWithEventWithResult<{ previewUrl: string }>(
        app.get(EventEmitterService),
        Events.BASE_EXPORT_COMPLETE
      );
    });

    afterAll(async () => {
      if (importedBaseId) {
        await permanentDeleteBase(importedBaseId);
      }
      if (conditionalBaseId) {
        await permanentDeleteBase(conditionalBaseId);
      }
    });

    it('imports base with conditional rollup without circular dependency', async () => {
      const { previewUrl } = await awaitConditionalExport(async () => {
        await exportBase(conditionalBaseId);
      });

      const attachmentService = getAttachmentService(app);
      const clsService = app.get(ClsService);

      const notify = await clsService.runWith<Promise<IAttachmentItem>>(
        {
          user: {
            id: userId,
            name: 'Test User',
            email: 'test@example.com',
            isAdmin: null,
          },
        } as unknown as ClsStore,
        async () => {
          return await attachmentService.uploadFromUrl(appUrl + previewUrl);
        }
      );

      const { base: importedBase } = (
        await importBase({
          notify: notify as unknown as INotifyVo,
          spaceId,
        })
      ).data;

      importedBaseId = importedBase.id;

      const tableList = (await getTableList(importedBase.id)).data;
      expect(tableList.map(({ name }) => name).sort()).toEqual(
        [hostTable.name, foreignTable.name].sort()
      );

      const importedHostMeta = tableList.find((tableMeta) => tableMeta.name === hostTable.name)!;
      const importedHost = await getTable(importedBase.id, importedHostMeta.id, {
        includeContent: true,
      });

      const importedFields = importedHost.fields ?? [];
      const importedRollupField = importedFields.find((field) => field.name === 'Status Rollup')!;
      expect(importedRollupField.type).toBe(FieldType.ConditionalRollup);
      expect(importedRollupField.hasError).toBeFalsy();

      const importedLookupField = importedFields.find((field) => field.name === 'Status Lookup')!;
      expect(importedLookupField.isLookup).toBeTruthy();
      expect(importedLookupField.isConditionalLookup).toBeTruthy();
      expect(importedLookupField.hasError).toBeFalsy();
      const lookupOptions =
        typeof importedLookupField.lookupOptions === 'string'
          ? (JSON.parse(importedLookupField.lookupOptions) as {
              sort?: { fieldId: string; order?: SortFunc };
            })
          : (importedLookupField.lookupOptions as
              | { sort?: { fieldId: string; order?: SortFunc } }
              | undefined);
      expect(lookupOptions?.sort?.order).toBe(SortFunc.Asc);

      const importedStatusFilter = importedFields.find((field) => field.name === 'StatusFilter')!;

      const activeRecordMeta = await waitForRecordWithFieldValue(
        importedHostMeta.id,
        importedStatusFilter.id,
        'Active'
      );
      const inactiveRecordMeta = await waitForRecordWithFieldValue(
        importedHostMeta.id,
        importedStatusFilter.id,
        'Inactive'
      );

      expect(activeRecordMeta).toBeDefined();
      expect(inactiveRecordMeta).toBeDefined();

      const activeRecord = await waitForComputedRecord(importedHostMeta.id, activeRecordMeta!.id, [
        importedRollupField.id,
        importedLookupField.id,
      ]);
      const inactiveRecord = await waitForComputedRecord(
        importedHostMeta.id,
        inactiveRecordMeta!.id,
        [importedRollupField.id, importedLookupField.id]
      );

      expect(activeRecord.fields?.[importedRollupField.id]).toBe('Alpha');
      expect(inactiveRecord.fields?.[importedRollupField.id]).toBe('Beta');
      expect(activeRecord.fields?.[importedLookupField.id]).toEqual(['Alpha']);
      expect(inactiveRecord.fields?.[importedLookupField.id]).toEqual(['Beta']);
    });
  });
});
