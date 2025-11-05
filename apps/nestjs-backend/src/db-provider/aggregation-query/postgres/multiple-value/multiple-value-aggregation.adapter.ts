import { AggregationFunctionPostgres } from '../aggregation-function.postgres';

export class MultipleValueAggregationAdapter extends AggregationFunctionPostgres {
  private toNumericSafe(columnExpression: string): string {
    const textExpr = `(${columnExpression})::text`;
    const sanitized = `REGEXP_REPLACE(${textExpr}, '[^0-9.+-]', '', 'g')`;
    return `NULLIF(${sanitized}, '')::double precision`;
  }

  unique(): string {
    return this.knex
      .raw(
        `SELECT COUNT(DISTINCT "value") AS "value" FROM ?? as "${this.tableAlias}", jsonb_array_elements_text(${this.tableColumnRef}::jsonb)`,
        [this.dbTableName]
      )
      .toQuery();
  }

  max(): string {
    return this.knex
      .raw(
        `SELECT MAX(${this.toNumericSafe('"value"')}) AS "value" FROM ?? as "${this.tableAlias}", jsonb_array_elements_text(${this.tableColumnRef}::jsonb)`,
        [this.dbTableName]
      )
      .toQuery();
  }

  min(): string {
    return this.knex
      .raw(
        `SELECT MIN(${this.toNumericSafe('"value"')}) AS "value" FROM ?? as "${this.tableAlias}", jsonb_array_elements_text(${this.tableColumnRef}::jsonb)`,
        [this.dbTableName]
      )
      .toQuery();
  }

  sum(): string {
    return this.knex
      .raw(
        `SELECT SUM(${this.toNumericSafe('"value"')}) AS "value" FROM ?? as "${this.tableAlias}", jsonb_array_elements_text(${this.tableColumnRef}::jsonb)`,
        [this.dbTableName]
      )
      .toQuery();
  }

  average(): string {
    return this.knex
      .raw(
        `SELECT AVG(${this.toNumericSafe('"value"')}) AS "value" FROM ?? as "${this.tableAlias}", jsonb_array_elements_text(${this.tableColumnRef}::jsonb)`,
        [this.dbTableName]
      )
      .toQuery();
  }

  percentUnique(): string {
    return this.knex
      .raw(
        `SELECT (COUNT(DISTINCT "value") * 1.0 / GREATEST(COUNT(*), 1)) * 100 AS "value" FROM ?? as "${this.tableAlias}", jsonb_array_elements_text(${this.tableColumnRef}::jsonb)`,
        [this.dbTableName]
      )
      .toQuery();
  }

  dateRangeOfDays(): string {
    return this.knex
      .raw(
        `SELECT extract(DAY FROM (MAX("value"::TIMESTAMPTZ) - MIN("value"::TIMESTAMPTZ)))::INTEGER AS "value" FROM ?? as "${this.tableAlias}", jsonb_array_elements_text(${this.tableColumnRef}::jsonb)`,
        [this.dbTableName]
      )
      .toQuery();
  }

  dateRangeOfMonths(): string {
    return this.knex
      .raw(
        `SELECT CONCAT(MAX("value"::TIMESTAMPTZ), ',', MIN("value"::TIMESTAMPTZ)) AS "value" FROM ?? as "${this.tableAlias}", jsonb_array_elements_text(${this.tableColumnRef}::jsonb)`,
        [this.dbTableName]
      )
      .toQuery();
  }
}
