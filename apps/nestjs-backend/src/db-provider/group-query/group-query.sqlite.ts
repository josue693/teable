import type { INumberFieldOptions, IDateFieldOptions } from '@teable/core';
import type { Knex } from 'knex';
import type { IFieldInstance } from '../../features/field/model/factory';
import { isUserOrLink } from '../../utils/is-user-or-link';
import { getOffset } from '../search-query/get-offset';
import { AbstractGroupQuery } from './group-query.abstract';
import type { IGroupQueryExtra } from './group-query.interface';

export class GroupQuerySqlite extends AbstractGroupQuery {
  constructor(
    protected readonly knex: Knex,
    protected readonly originQueryBuilder: Knex.QueryBuilder,
    protected readonly fieldMap?: { [fieldId: string]: IFieldInstance },
    protected readonly groupFieldIds?: string[],
    protected readonly extra?: IGroupQueryExtra
  ) {
    super(knex, originQueryBuilder, fieldMap, groupFieldIds, extra);
  }

  private get isDistinct() {
    const { isDistinct } = this.extra ?? {};
    return isDistinct;
  }

  string(field: IFieldInstance): Knex.QueryBuilder {
    if (!field) return this.originQueryBuilder;

    const { dbFieldName } = field;
    const column = this.knex.ref(dbFieldName);

    if (this.isDistinct) {
      return this.originQueryBuilder.countDistinct(dbFieldName);
    }
    return this.originQueryBuilder.select(column).groupBy(dbFieldName);
  }

  number(field: IFieldInstance): Knex.QueryBuilder {
    const { dbFieldName, options } = field;
    const { precision } = (options as INumberFieldOptions).formatting;
    const column = this.knex.raw('ROUND(??, ?) as ??', [dbFieldName, precision, dbFieldName]);
    const groupByColumn = this.knex.raw('ROUND(??, ?)', [dbFieldName, precision]);

    if (this.isDistinct) {
      return this.originQueryBuilder.countDistinct(groupByColumn);
    }
    return this.originQueryBuilder.select(column).groupBy(groupByColumn);
  }

  date(field: IFieldInstance): Knex.QueryBuilder {
    const { dbFieldName, options } = field;
    const { date, time, timeZone } = (options as IDateFieldOptions).formatting;
    const offsetStr = `${getOffset(timeZone)} hour`;
    const isYear = date === 'YYYY';
    const isMonth = date === 'YYYY-MM' || date === 'MM';
    const hasTime = time && time !== 'None';

    const local = (expr: string) => this.knex.raw(`DATETIME(${expr}, ?)`, [offsetStr]);
    const yearStr = this.knex.raw(`STRFTIME('%Y', ${local('??').toString()})`, [dbFieldName]);
    const monthStr = this.knex.raw(`STRFTIME('%m', ${local('??').toString()})`, [dbFieldName]);
    const dayStr = this.knex.raw(`STRFTIME('%d', ${local('??').toString()})`, [dbFieldName]);
    const hourStr = this.knex.raw(`STRFTIME('%H', ${local('??').toString()})`, [dbFieldName]);
    const minuteStr = this.knex.raw(`STRFTIME('%M', ${local('??').toString()})`, [dbFieldName]);

    const localBucket = isYear
      ? this.knex.raw(`(${yearStr}) || '-01-01 00:00:00'`)
      : isMonth
        ? this.knex.raw(`(${yearStr}) || '-' || (${monthStr}) || '-01 00:00:00'`)
        : hasTime
          ? this.knex.raw(
              `(${yearStr}) || '-' || (${monthStr}) || '-' || (${dayStr}) || ' ' || (${hourStr}) || ':' || (${minuteStr}) || ':00'`
            )
          : this.knex.raw(
              `(${yearStr}) || '-' || (${monthStr}) || '-' || (${dayStr}) || ' 00:00:00'`
            );

    const utcBucket = this.knex.raw(
      `REPLACE(datetime(${localBucket.toString()}, ? * -1), ' ', 'T') || 'Z'`,
      [offsetStr]
    );

    const isoBucket = utcBucket;

    const column = this.knex.raw(`(${isoBucket}) as ??`, [dbFieldName]);
    const groupByColumn = this.knex.raw(`(${isoBucket})`);

    if (this.isDistinct) {
      return this.originQueryBuilder.countDistinct(groupByColumn);
    }
    return this.originQueryBuilder.select(column).groupBy(groupByColumn);
  }

  json(field: IFieldInstance): Knex.QueryBuilder {
    const { type, dbFieldName, isMultipleCellValue } = field;

    if (this.isDistinct) {
      if (isUserOrLink(type)) {
        if (!isMultipleCellValue) {
          const groupByColumn = this.knex.raw(
            `json_extract(??, '$.id') || json_extract(??, '$.title')`,
            [dbFieldName, dbFieldName]
          );
          return this.originQueryBuilder.countDistinct(groupByColumn);
        }
        const groupByColumn = this.knex.raw(`json_extract(??, '$[0].id', '$[0].title')`, [
          dbFieldName,
        ]);
        return this.originQueryBuilder.countDistinct(groupByColumn);
      }
      return this.originQueryBuilder.countDistinct(dbFieldName);
    }

    if (isUserOrLink(type)) {
      if (!isMultipleCellValue) {
        const groupByColumn = this.knex.raw(
          `json_extract(??, '$.id') || json_extract(??, '$.title')`,
          [dbFieldName, dbFieldName]
        );
        return this.originQueryBuilder.select(dbFieldName).groupBy(groupByColumn);
      }

      const groupByColumn = this.knex.raw(`json_extract(??, '$[0].id', '$[0].title')`, [
        dbFieldName,
      ]);
      return this.originQueryBuilder.select(dbFieldName).groupBy(groupByColumn);
    }

    const column = this.knex.raw(`CAST(?? as text) as ??`, [dbFieldName, dbFieldName]);
    return this.originQueryBuilder.select(column).groupBy(dbFieldName);
  }

  multipleDate(field: IFieldInstance): Knex.QueryBuilder {
    const { dbFieldName, options } = field;
    const { date, time, timeZone } = (options as IDateFieldOptions).formatting;
    const offsetStr = `${getOffset(timeZone)} hour`;
    const isYear = date === 'YYYY';
    const isMonth = date === 'YYYY-MM' || date === 'MM';
    const hasTime = time && time !== 'None';

    const local = (expr: string) => this.knex.raw(`DATETIME(${expr}, ?)`, [offsetStr]);

    const isoAgg = isYear
      ? this.knex.raw(
          `(
        SELECT json_group_array(REPLACE(datetime(STRFTIME('%Y', ${local('value').toString()}) || '-01-01 00:00:00', ? * -1), ' ', 'T') || 'Z')
        FROM json_each(??)
      ) as ??`,
          [offsetStr, dbFieldName, dbFieldName]
        )
      : isMonth
        ? this.knex.raw(
            `(
        SELECT json_group_array(REPLACE(datetime(STRFTIME('%Y', ${local('value').toString()}) || '-' || STRFTIME('%m', ${local('value').toString()}) || '-01 00:00:00', ? * -1), ' ', 'T') || 'Z')
        FROM json_each(??)
      ) as ??`,
            [offsetStr, dbFieldName, dbFieldName]
          )
        : hasTime
          ? this.knex.raw(
              `(
        SELECT json_group_array(REPLACE(datetime(STRFTIME('%Y', ${local('value').toString()}) || '-' || STRFTIME('%m', ${local('value').toString()}) || '-' || STRFTIME('%d', ${local('value').toString()}) || ' ' || STRFTIME('%H', ${local('value').toString()}) || ':' || STRFTIME('%M', ${local('value').toString()}) || ':00', ? * -1), ' ', 'T') || 'Z')
        FROM json_each(??)
      ) as ??`,
              [offsetStr, dbFieldName, dbFieldName]
            )
          : this.knex.raw(
              `(
        SELECT json_group_array(REPLACE(datetime(STRFTIME('%Y', ${local('value').toString()}) || '-' || STRFTIME('%m', ${local('value').toString()}) || '-' || STRFTIME('%d', ${local('value').toString()}) || ' 00:00:00', ? * -1), ' ', 'T') || 'Z')
        FROM json_each(??)
      ) as ??`,
              [offsetStr, dbFieldName, dbFieldName]
            );

    const column = isoAgg;
    const groupByColumn = this.knex.raw(column.toSQL().sql.replace(/\s+as\s+\?\?$/i, ''), []);

    if (this.isDistinct) {
      return this.originQueryBuilder.countDistinct(groupByColumn);
    }
    return this.originQueryBuilder.select(column).groupBy(groupByColumn);
  }

  multipleNumber(field: IFieldInstance): Knex.QueryBuilder {
    const { dbFieldName, options } = field;
    const { precision } = (options as INumberFieldOptions).formatting;
    const column = this.knex.raw(
      `
      (
        SELECT json_group_array(ROUND(value, ?))
        FROM json_each(??)
      ) as ??
      `,
      [precision, dbFieldName, dbFieldName]
    );
    const groupByColumn = this.knex.raw(
      `
      (
        SELECT json_group_array(ROUND(value, ?))
        FROM json_each(??)
      )
      `,
      [precision, dbFieldName]
    );

    if (this.isDistinct) {
      return this.originQueryBuilder.countDistinct(groupByColumn);
    }
    return this.originQueryBuilder.select(column).groupBy(groupByColumn);
  }
}
