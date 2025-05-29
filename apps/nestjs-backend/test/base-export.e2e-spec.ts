/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable sonarjs/no-duplicate-string */
/* eslint-disable sonarjs/cognitive-complexity */
import type { INestApplication } from '@nestjs/common';
import type { IAttachmentItem } from '@teable/core';
import { FieldType, Relationship } from '@teable/core';
import type { IBaseJson, ITableFullVo } from '@teable/openapi';
import {
  createField,
  installViewPlugin,
  exportBase,
  createBase,
  createDashboard,
  installPlugin,
  createPluginPanel,
  installPluginPanel,
  permanentDeleteTable,
} from '@teable/openapi';

import type { ClsStore } from 'nestjs-cls';
import { ClsService } from 'nestjs-cls';
import * as unzipper from 'unzipper';

import { EventEmitterService } from '../src/event-emitter/event-emitter.service';
import { Events } from '../src/event-emitter/events';
import { AttachmentsService } from '../src/features/attachments/attachments.service';
import { x_20 } from './data-helpers/20x';
import { x_20_link, x_20_link_from_lookups } from './data-helpers/20x-link';
import { createAwaitWithEventWithResult } from './utils/event-promise';
import { createTable, initApp } from './utils/init-app';

function getAttachmentService(app: INestApplication) {
  return app.get<AttachmentsService>(AttachmentsService);
}

describe('OpenAPI BaseController for base import and export (e2e)', () => {
  let app: INestApplication;
  let sourceBaseId: string;
  const spaceId = globalThis.testConfig.spaceId;
  const userId = globalThis.testConfig.userId;
  let table: ITableFullVo;
  let subTable: ITableFullVo;
  let eventEmitterService: EventEmitterService;
  let awaitWithEvent: <T>(fn: () => Promise<T>) => Promise<{ previewUrl: string }>;

  beforeEach(async () => {
    const appCtx = await initApp();
    app = appCtx.app;

    const crossBase = (
      await createBase({
        name: 'cross_base',
        spaceId: spaceId,
        icon: '😄',
      })
    ).data;

    const crossBaseTable = await createTable(crossBase.id, {
      name: 'cross_base_table',
    });

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

    // cross base link field
    const crossBaseLinkField = (
      await createField(table.id, {
        name: 'cross_base_link',
        type: FieldType.Link,
        options: {
          baseId: crossBase.id,
          relationship: Relationship.ManyMany,
          foreignTableId: crossBaseTable.id,
        },
      })
    ).data;

    // create cross base lookup field
    await createField(table.id, {
      name: 'cross_base_lookup',
      type: FieldType.SingleLineText,
      isLookup: true,
      lookupOptions: {
        foreignTableId: crossBaseTable.id,
        linkFieldId: crossBaseLinkField.id,
        lookupFieldId: crossBaseTable.fields[0].id,
      },
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

    await createField(table.id, {
      name: 'date_formula',
      type: FieldType.Formula,
      options: {
        expression: '"TODAY()"',
        timeZone: 'Asia/Shanghai',
      },
    });

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
  });

  afterEach(async () => {
    await permanentDeleteTable(sourceBaseId, table.id);
    await permanentDeleteTable(sourceBaseId, subTable.id);
    await app.close();
  });

  it('should duplicate base with cross base relative field like lookup、rollup field', async () => {
    const base = (await createBase({ spaceId, name: 'test base' })).data;
    const base2 = (await createBase({ spaceId, name: 'test base 2' })).data;
    const base2Table = await createTable(base2.id, { name: 'table1' });
    const table1 = await createTable(base.id, { name: 'table1' });

    const lookupedCrossBaseTExtField = base2Table.fields.find(
      ({ type }) => type === FieldType.SingleLineText
    )!;

    const lookupedCrossBaseNumberField = base2Table.fields.find(
      ({ type }) => type === FieldType.Number
    )!;

    // cross base link field
    const crossBaseLinkField = (
      await createField(table1.id, {
        name: 'cross-base-link-field',
        type: FieldType.Link,
        options: {
          baseId: base2.id,
          relationship: Relationship.ManyMany,
          foreignTableId: base2Table.id,
        },
      })
    ).data;

    // cross base lookup field
    await createField(table1.id, {
      name: 'cross-base-lookup-field',
      isLookup: true,
      type: FieldType.SingleLineText,
      lookupOptions: {
        foreignTableId: base2Table.id,
        linkFieldId: crossBaseLinkField.id,
        lookupFieldId: lookupedCrossBaseTExtField.id,
      },
    });

    // cross base rollup field
    await createField(table1.id, {
      name: 'cross-base-rollup-field',
      type: FieldType.Rollup,
      lookupOptions: {
        foreignTableId: base2Table.id,
        linkFieldId: crossBaseLinkField.id,
        lookupFieldId: lookupedCrossBaseNumberField.id,
      },
      options: {
        expression: 'countall({values})',
        formatting: {
          precision: 0,
          type: 'decimal',
        },
        timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone as string,
      },
    });

    const { previewUrl } = await awaitWithEvent(async () => {
      await exportBase(base2.id);
    });

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

    const zip = await fetch(notify.presignedUrl!);
    // Use http.request to create a proper ClientRequest instance
    const directory = await unzipper.Open.buffer(Buffer.from(await zip.arrayBuffer()));

    const structureFile = directory.files.find(({ path }) => path === 'structure.json');

    const structure = await new Promise<IBaseJson>((resolve, reject) => {
      structureFile
        ?.buffer()
        .then((buffer) => {
          const structure = JSON.parse(buffer.toString());
          resolve(structure);
        })
        .catch(reject);
    });

    const duplicatedTable1Fields = structure.tables[0].fields;

    const duplicatedCrossBaseLookupField = duplicatedTable1Fields.find(
      ({ name }) => name === 'cross-base-lookup-field'
    );

    const duplicatedCrossBaseRollupField = duplicatedTable1Fields.find(
      ({ name }) => name === 'cross-base-rollup-field'
    );

    expect(duplicatedCrossBaseLookupField?.options).toBeUndefined();
    expect(duplicatedCrossBaseRollupField?.options).toBeUndefined();
    expect(duplicatedCrossBaseLookupField?.isLookup).toBeFalsy();
    expect(duplicatedCrossBaseRollupField?.isLookup).toBeFalsy();
  });
});
