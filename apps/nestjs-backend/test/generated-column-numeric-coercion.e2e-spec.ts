/* eslint-disable @typescript-eslint/naming-convention */
import type { INestApplication } from '@nestjs/common';
import type { IFieldRo, IFieldVo } from '@teable/core';
import { FieldType } from '@teable/core';
import type { ITableFullVo } from '@teable/openapi';
import {
  createField,
  createTable,
  getRecord,
  initApp,
  permanentDeleteTable,
  updateRecordByApi,
} from './utils/init-app';

describe('Generated column numeric coercion (e2e)', () => {
  let app: INestApplication;
  const baseId = globalThis.testConfig.baseId;

  beforeAll(async () => {
    const appCtx = await initApp();
    app = appCtx.app;
  });

  afterAll(async () => {
    await app.close();
  });

  describe('text fields in arithmetic formulas', () => {
    let table: ITableFullVo;
    let durationField: IFieldVo;
    let consumedField: IFieldVo;
    let remainingField: IFieldVo;
    let progressField: IFieldVo;

    beforeEach(async () => {
      const seedFields: IFieldRo[] = [
        {
          name: 'Planned Duration',
          type: FieldType.SingleLineText,
        },
        {
          name: 'Consumed Days',
          type: FieldType.SingleLineText,
        },
      ];

      table = await createTable(baseId, {
        name: 'generated_numeric_coercion',
        fields: seedFields,
        records: [
          {
            fields: {
              'Planned Duration': '10天',
              'Consumed Days': '3',
            },
          },
        ],
      });

      const fieldMap = new Map(table.fields.map((field) => [field.name, field]));
      durationField = fieldMap.get('Planned Duration')!;
      consumedField = fieldMap.get('Consumed Days')!;

      remainingField = await createField(table.id, {
        name: 'Remaining Days',
        type: FieldType.Formula,
        options: {
          expression: `{${durationField.id}} - {${consumedField.id}}`,
        },
      });

      progressField = await createField(table.id, {
        name: 'Progress',
        type: FieldType.Formula,
        options: {
          expression: `{${consumedField.id}} / {${durationField.id}}`,
        },
      });
    });

    afterEach(async () => {
      if (table) {
        await permanentDeleteTable(baseId, table.id);
      }
    });

    it('coerces numeric strings when updating generated columns', async () => {
      const recordId = table.records[0].id;

      const createdRecord = await getRecord(table.id, recordId);
      expect(createdRecord.fields[remainingField.id]).toBe(7);
      expect(createdRecord.fields[progressField.id]).toBeCloseTo(3 / 10, 2);

      await expect(
        updateRecordByApi(table.id, recordId, consumedField.id, '4天')
      ).resolves.toBeDefined();

      const updatedRecord = await getRecord(table.id, recordId);
      expect(updatedRecord.fields[remainingField.id]).toBe(6);
      expect(updatedRecord.fields[progressField.id]).toBeCloseTo(4 / 10, 2);

      await expect(
        updateRecordByApi(table.id, recordId, durationField.id, '12周')
      ).resolves.toBeDefined();

      const finalRecord = await getRecord(table.id, recordId);
      expect(finalRecord.fields[remainingField.id]).toBe(8);
      expect(finalRecord.fields[progressField.id]).toBeCloseTo(4 / 12, 2);
    });
  });
});
