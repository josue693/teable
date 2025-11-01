import { DbFieldType } from '@teable/core';
import type { Knex } from 'knex';
import { describe, expect, it } from 'vitest';
import { PgRecordQueryDialect } from './pg-record-query-dialect';

describe('PgRecordQueryDialect#flattenLookupCteValue', () => {
  const dialect = new PgRecordQueryDialect({} as unknown as Knex);

  it('returns null for single-value lookups', () => {
    const result = dialect.flattenLookupCteValue(
      'cte_lookup',
      'fld_single',
      false,
      DbFieldType.Text
    );
    expect(result).toBeNull();
  });

  it('keeps jsonb payloads when field is stored as json', () => {
    const sql = dialect.flattenLookupCteValue('cte_lookup', 'fld_json', true, DbFieldType.Json);
    expect(sql).toContain('"cte_lookup"."lookup_fld_json"::jsonb');
    expect(sql).not.toContain('to_jsonb("cte_lookup"."lookup_fld_json")');
  });

  it('wraps scalar payloads with to_jsonb for non-json fields', () => {
    const sql = dialect.flattenLookupCteValue('cte_lookup', 'fld_scalar', true, DbFieldType.Text);
    expect(sql).toContain('to_jsonb("cte_lookup"."lookup_fld_scalar")');
  });
});
