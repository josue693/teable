/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable @typescript-eslint/no-explicit-any */
import type { INestApplication } from '@nestjs/common';
import type { IFieldRo, IFieldVo } from '@teable/core';
import { FieldKeyType, FieldType, Relationship } from '@teable/core';
import type { ITableFullVo } from '@teable/openapi';
import {
  createField,
  createTable,
  getFields,
  getRecords,
  initApp,
  permanentDeleteTable,
  updateRecordByApi,
} from './utils/init-app';

/**
 * Regression test: verifies FieldCteVisitor no longer overflows the stack when link/lookup/formula
 * dependencies form a cycle (calculation formula references lookups, the linked table looks the formula back up).
 */
describe('Link/Formula circular dependency regression (e2e)', () => {
  let app: INestApplication;
  const baseId = globalThis.testConfig.baseId;

  beforeAll(async () => {
    const appCtx = await initApp();
    app = appCtx.app;
  });

  afterAll(async () => {
    await app.close();
  });

  it('handles circular link/lookups without overflowing the stack', async () => {
    let calculationTable: ITableFullVo | undefined;
    let salesTable: ITableFullVo | undefined;

    try {
      salesTable = await createTable(baseId, {
        name: 'Sales',
        fields: [
          {
            name: 'Name',
            type: FieldType.SingleLineText,
          },
          {
            name: 'Count',
            type: FieldType.Number,
            options: {
              formatting: {
                type: 'decimal',
                precision: 0,
              },
            },
          },
          {
            name: 'Status',
            type: FieldType.SingleLineText,
          },
        ],
        records: [
          {
            fields: {
              Name: 'Order A',
              Count: 3,
              Status: 'light',
            },
          },
        ],
      });

      calculationTable = await createTable(baseId, {
        name: 'Calculation',
        fields: [
          {
            name: 'Project',
            type: FieldType.SingleLineText,
          },
        ],
        records: [
          {
            fields: {
              Project: 'X-001',
            },
          },
        ],
      });

      const calculationToSalesLink = await createField(calculationTable.id, {
        name: 'Sales Link',
        type: FieldType.Link,
        options: {
          relationship: Relationship.ManyOne,
          foreignTableId: salesTable.id,
        },
      });

      const salesFieldsAfterLink = await getFields(salesTable.id);
      const salesToCalculationLink = salesFieldsAfterLink.find(
        (field) =>
          field.type === FieldType.Link &&
          (field.options as { foreignTableId?: string })?.foreignTableId === calculationTable!.id
      ) as IFieldVo | undefined;

      expect(salesToCalculationLink).toBeDefined();

      const salesNameFieldId = salesTable.fields.find((f) => f.name === 'Name')!.id;
      const salesCountFieldId = salesTable.fields.find((f) => f.name === 'Count')!.id;

      // Create lookups on the calculation table that pull data from Sales.
      const countLookup = await createField(calculationTable.id, {
        name: 'Sales Count',
        type: FieldType.Number,
        isLookup: true,
        lookupOptions: {
          foreignTableId: salesTable.id,
          linkFieldId: calculationToSalesLink.id,
          lookupFieldId: salesCountFieldId,
        },
      } as unknown as IFieldRo);

      const nameLookup = await createField(calculationTable.id, {
        name: 'Sales Name',
        type: FieldType.SingleLineText,
        isLookup: true,
        lookupOptions: {
          foreignTableId: salesTable.id,
          linkFieldId: calculationToSalesLink.id,
          lookupFieldId: salesNameFieldId,
        },
      } as unknown as IFieldRo);

      const formulaField = await createField(calculationTable.id, {
        name: 'Calculation Formula',
        type: FieldType.Formula,
        options: {
          expression: `2+2 & {${countLookup.id}}&{${nameLookup.id}} & 4 & 'xxxxxxx'`,
        },
      } as unknown as IFieldRo);

      // Sales table looks up the calculation formula, closing the dependency cycle.
      const calculationLookupOnSales = await createField(salesTable.id, {
        name: 'Calculation Lookup',
        type: FieldType.Formula,
        isLookup: true,
        lookupOptions: {
          foreignTableId: calculationTable.id,
          linkFieldId: salesToCalculationLink!.id,
          lookupFieldId: formulaField.id,
        },
      } as unknown as IFieldRo);

      // Link the calculation record to the sales record.
      await updateRecordByApi(
        calculationTable.id,
        calculationTable.records[0].id,
        calculationToSalesLink.id,
        { id: salesTable.records[0].id }
      );

      // First query should succeed and the formula output should include expected content.
      const calculationRecords = await getRecords(calculationTable.id, {
        fieldKeyType: FieldKeyType.Id,
      });
      expect(calculationRecords.records).toHaveLength(1);
      const calcValue = calculationRecords.records[0].fields[formulaField.id];
      expect(typeof calcValue).toBe('string');
      expect(calcValue as string).toContain('xxxxxxx');
      expect(calcValue as string).toContain('Order A');
      expect(calcValue as string).toContain('3');

      // Updating the sales count forces the entire chain to recompute.
      await updateRecordByApi(salesTable.id, salesTable.records[0].id, salesCountFieldId, 7);

      const calcRecordsAfterUpdate = await getRecords(calculationTable.id, {
        fieldKeyType: FieldKeyType.Id,
      });
      const updatedValue = calcRecordsAfterUpdate.records[0].fields[formulaField.id];
      expect(typeof updatedValue).toBe('string');
      expect(updatedValue as string).toContain('7');

      // Ensure the lookup on the sales table resolves correctly as well.
      const salesRecords = await getRecords(salesTable.id, { fieldKeyType: FieldKeyType.Id });
      expect(salesRecords.records).toHaveLength(1);
      const lookupValue = salesRecords.records[0].fields[calculationLookupOnSales.id];
      expect(lookupValue).toBeTruthy();
    } finally {
      if (calculationTable) {
        await permanentDeleteTable(baseId, calculationTable.id);
      }
      if (salesTable) {
        await permanentDeleteTable(baseId, salesTable.id);
      }
    }
  });

  it('avoids forward CTE joins when lookups bounce across symmetric links', async () => {
    let hostTable: ITableFullVo | undefined;
    let foreignTable: ITableFullVo | undefined;

    try {
      hostTable = await createTable(baseId, {
        name: 'Forward CTE Host',
        fields: [
          {
            name: 'Host Name',
            type: FieldType.SingleLineText,
          },
        ],
        records: [
          {
            fields: {
              'Host Name': 'Alpha',
            },
          },
        ],
      });

      foreignTable = await createTable(baseId, {
        name: 'Forward CTE Foreign',
        fields: [
          {
            name: 'Foreign Title',
            type: FieldType.SingleLineText,
          },
        ],
        records: [
          {
            fields: {
              'Foreign Title': 'Beacon',
            },
          },
        ],
      });

      const hostToForeignLink = await createField(hostTable.id, {
        name: 'Foreign Link',
        type: FieldType.Link,
        options: {
          relationship: Relationship.ManyOne,
          foreignTableId: foreignTable.id,
        },
      });

      let foreignToHostLink: IFieldVo | undefined;
      for (let attempt = 0; attempt < 5 && !foreignToHostLink; attempt++) {
        // Wait for symmetric link creation to finish.
        // eslint-disable-next-line no-await-in-loop
        await new Promise((resolve) => setTimeout(resolve, 100));
        const foreignFields = await getFields(foreignTable.id);
        foreignToHostLink = foreignFields.find(
          (field) =>
            field.type === FieldType.Link &&
            (field.options as { symmetricFieldId?: string })?.symmetricFieldId ===
              hostToForeignLink.id
        );
      }

      expect(foreignToHostLink).toBeDefined();
      // eslint-disable-next-line no-console
      console.log(
        'link ids',
        hostToForeignLink.id,
        (hostToForeignLink.options as { symmetricFieldId?: string }).symmetricFieldId,
        foreignToHostLink?.id,
        (foreignToHostLink?.options as { symmetricFieldId?: string })?.symmetricFieldId
      );
      // Debug field ids for diagnosing CTE ordering issues
      // eslint-disable-next-line no-console
      console.log(
        'hostToForeignLink',
        hostToForeignLink.id,
        'foreignToHostLink',
        foreignToHostLink?.id
      );

      const foreignTitleFieldId = foreignTable.fields.find((f) => f.name === 'Foreign Title')!.id;

      const hostLookupOfForeign = await createField(hostTable.id, {
        name: 'Foreign Title Lookup',
        type: FieldType.SingleLineText,
        isLookup: true,
        lookupOptions: {
          foreignTableId: foreignTable.id,
          linkFieldId: hostToForeignLink.id,
          lookupFieldId: foreignTitleFieldId,
        },
      } as unknown as IFieldRo);

      const foreignLookupOfHostLookup = await createField(foreignTable.id, {
        name: 'Host Lookup Mirror',
        type: FieldType.SingleLineText,
        isLookup: true,
        lookupOptions: {
          foreignTableId: hostTable.id,
          linkFieldId: foreignToHostLink!.id,
          lookupFieldId: hostLookupOfForeign.id,
        },
      } as unknown as IFieldRo);

      const hostRoundTripLookup = await createField(hostTable.id, {
        name: 'Round Trip Title',
        type: FieldType.SingleLineText,
        isLookup: true,
        lookupOptions: {
          foreignTableId: foreignTable.id,
          linkFieldId: hostToForeignLink.id,
          lookupFieldId: foreignLookupOfHostLookup.id,
        },
      } as unknown as IFieldRo);

      await updateRecordByApi(hostTable.id, hostTable.records[0].id, hostToForeignLink.id, {
        id: foreignTable.records[0].id,
      });
      // eslint-disable-next-line no-console
      console.log(
        'hostLookupOfForeign',
        hostLookupOfForeign.id,
        'foreignLookupOfHostLookup',
        foreignLookupOfHostLookup.id,
        'hostRoundTripLookup',
        hostRoundTripLookup.id
      );

      const hostRecords = await getRecords(hostTable.id, { fieldKeyType: FieldKeyType.Id });
      // eslint-disable-next-line no-console
      console.log('host record fields', hostRecords.records[0].fields);
      expect(hostRecords.records).toHaveLength(1);
      const roundTripValue = hostRecords.records[0].fields[hostRoundTripLookup.id];
      expect(typeof roundTripValue).toBe('string');
      expect(roundTripValue).toBe('Beacon');
    } finally {
      if (hostTable) {
        await permanentDeleteTable(baseId, hostTable.id);
      }
      if (foreignTable) {
        await permanentDeleteTable(baseId, foreignTable.id);
      }
    }
  });
});
