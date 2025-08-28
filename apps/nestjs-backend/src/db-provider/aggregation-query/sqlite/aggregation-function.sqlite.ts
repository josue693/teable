import { NotImplementedException } from '@nestjs/common';
import { FieldType } from '@teable/core';
import { AbstractAggregationFunction } from '../aggregation-function.abstract';

export class AggregationFunctionSqlite extends AbstractAggregationFunction {
  unique(): string {
    const { type, isMultipleCellValue } = this.field;
    if (
      ![FieldType.User, FieldType.CreatedBy, FieldType.LastModifiedBy].includes(type) ||
      isMultipleCellValue
    ) {
      return super.unique();
    }

    return this.knex.raw(`COUNT(DISTINCT json_extract(${this.tableColumnRef}, '$.id'))`).toQuery();
  }

  percentUnique(): string {
    const { type, isMultipleCellValue } = this.field;
    if (
      ![FieldType.User, FieldType.CreatedBy, FieldType.LastModifiedBy].includes(type) ||
      isMultipleCellValue
    ) {
      return this.knex
        .raw(`(COUNT(DISTINCT ${this.tableColumnRef}) * 1.0 / MAX(COUNT(*), 1)) * 100`)
        .toQuery();
    }

    return this.knex
      .raw(
        `(COUNT(DISTINCT json_extract(${this.tableColumnRef}, '$.id')) * 1.0 / MAX(COUNT(*), 1)) * 100`
      )
      .toQuery();
  }
  dateRangeOfDays(): string {
    throw new NotImplementedException();
  }

  dateRangeOfMonths(): string {
    throw new NotImplementedException();
  }

  totalAttachmentSize(): string {
    // Sum sizes per row, then sum across the current scope (respects GROUP BY)
    return this.knex
      .raw(
        `SUM(COALESCE((SELECT SUM(json_extract(j.value, '$.size'))
          FROM json_each(COALESCE(${this.tableColumnRef}, '[]')) AS j), 0))`
      )
      .toQuery();
  }

  percentEmpty(): string {
    return this.knex
      .raw(`((COUNT(*) - COUNT(${this.tableColumnRef})) * 1.0 / MAX(COUNT(*), 1)) * 100`)
      .toQuery();
  }

  percentFilled(): string {
    return this.knex
      .raw(`(COUNT(${this.tableColumnRef}) * 1.0 / MAX(COUNT(*), 1)) * 100`)
      .toQuery();
  }

  percentChecked(): string {
    return this.percentFilled();
  }

  percentUnChecked(): string {
    return this.percentEmpty();
  }
}
