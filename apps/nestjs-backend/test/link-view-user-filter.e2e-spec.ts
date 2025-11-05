import type { INestApplication } from '@nestjs/common';
import type { IFieldRo, IFieldVo, IFilterRo } from '@teable/core';
import { FieldKeyType, FieldType, is, Me, Relationship } from '@teable/core';
import type { ITableFullVo } from '@teable/openapi';
import {
  createField,
  createTable,
  getRecords,
  initApp,
  permanentDeleteTable,
  updateRecordByApi,
  updateViewFilter,
} from './utils/init-app';

describe('Link field filtered by view with Me (e2e)', () => {
  let app: INestApplication;
  const baseId = globalThis.testConfig.baseId;
  const userId = globalThis.testConfig.userId;
  const userName = globalThis.testConfig.userName;
  const userEmail = globalThis.testConfig.email;

  beforeAll(async () => {
    const appCtx = await initApp();
    app = appCtx.app;
  });

  afterAll(async () => {
    await app.close();
  });

  describe('link with view filter referencing Me', () => {
    let primaryTable: ITableFullVo;
    let foreignTable: ITableFullVo;
    let linkField: IFieldVo;

    beforeEach(async () => {
      const primaryFields: IFieldRo[] = [
        {
          name: 'Name',
          type: FieldType.SingleLineText,
        },
      ];

      primaryTable = await createTable(baseId, {
        name: 'link_me_primary',
        fields: primaryFields,
        records: [
          {
            fields: {
              Name: 'Row 1',
            },
          },
        ],
      });

      const foreignFields: IFieldRo[] = [
        {
          name: 'Title',
          type: FieldType.SingleLineText,
        },
        {
          name: 'Assignee',
          type: FieldType.User,
        },
      ];

      foreignTable = await createTable(
        baseId,
        {
          name: 'link_me_foreign',
          fields: foreignFields,
          records: [
            {
              fields: {
                Title: 'Owned by me',
                Assignee: {
                  id: userId,
                  title: userName,
                  email: userEmail,
                },
              },
            },
            {
              fields: {
                Title: 'Unassigned record',
              },
            },
          ],
        },
        201
      );

      const filterByMe: IFilterRo = {
        filter: {
          conjunction: 'and',
          filterSet: [
            {
              fieldId: foreignTable.fields[1].id,
              operator: is.value,
              value: Me,
            },
          ],
        },
      };

      await updateViewFilter(foreignTable.id, foreignTable.defaultViewId!, filterByMe);

      linkField = await createField(primaryTable.id, {
        name: 'Filtered Tasks',
        type: FieldType.Link,
        options: {
          relationship: Relationship.ManyMany,
          foreignTableId: foreignTable.id,
          filterByViewId: foreignTable.defaultViewId,
        },
      });
    });

    afterEach(async () => {
      await permanentDeleteTable(baseId, primaryTable.id);
      await permanentDeleteTable(baseId, foreignTable.id);
    });

    it('should link records respecting view filter with Me without SQL errors', async () => {
      await expect(
        updateRecordByApi(primaryTable.id, primaryTable.records[0].id, linkField.id, [
          { id: foreignTable.records[0].id },
        ])
      ).resolves.toBeDefined();

      const listResponse = await getRecords(primaryTable.id, {
        fieldKeyType: FieldKeyType.Id,
      });
      const currentRecord = listResponse.records.find(
        (record) => record.id === primaryTable.records[0].id
      );
      const linked = currentRecord?.fields[linkField.id] as Array<{ id: string }> | undefined;
      expect(linked).toBeDefined();
      expect(linked).toHaveLength(1);
      expect(linked?.[0].id).toBe(foreignTable.records[0].id);
    });
  });
});
