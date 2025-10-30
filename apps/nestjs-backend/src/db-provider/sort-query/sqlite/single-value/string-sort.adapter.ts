import type { ISelectFieldOptions } from '@teable/core';
import { FieldType } from '@teable/core';
import type { Knex } from 'knex';
import { SortFunctionSqlite } from '../sort-query.function';

export class StringSortAdapter extends SortFunctionSqlite {
  asc(builderClient: Knex.QueryBuilder): Knex.QueryBuilder {
    if (!this.columnName) {
      return builderClient;
    }
    const { type, options } = this.field;

    if (type !== FieldType.SingleSelect) {
      return super.asc(builderClient);
    }

    const { choices } = options as ISelectFieldOptions;

    const optionSets = choices.map(({ name }) => name);
    builderClient.orderByRaw(
      `${this.generateOrderByCase(optionSets, this.columnName)} ASC NULLS FIRST`
    );
    return builderClient;
  }

  desc(builderClient: Knex.QueryBuilder): Knex.QueryBuilder {
    if (!this.columnName) {
      return builderClient;
    }
    const { type, options } = this.field;

    if (type !== FieldType.SingleSelect) {
      return super.desc(builderClient);
    }

    const { choices } = options as ISelectFieldOptions;

    const optionSets = choices.map(({ name }) => name);
    builderClient.orderByRaw(
      `${this.generateOrderByCase(optionSets, this.columnName)} DESC NULLS LAST`
    );
    return builderClient;
  }

  getAscSQL() {
    const { type, options } = this.field;

    if (type !== FieldType.SingleSelect) {
      return super.getAscSQL();
    }
    if (!this.columnName) {
      return undefined;
    }

    const { choices } = options as ISelectFieldOptions;

    const optionSets = choices.map(({ name }) => name);
    return this.knex
      .raw(`${this.generateOrderByCase(optionSets, this.columnName)} ASC NULLS FIRST`)
      .toQuery();
  }

  getDescSQL() {
    const { type, options } = this.field;

    if (type !== FieldType.SingleSelect) {
      return super.getDescSQL();
    }
    if (!this.columnName) {
      return undefined;
    }

    const { choices } = options as ISelectFieldOptions;

    const optionSets = choices.map(({ name }) => name);
    return this.knex
      .raw(`${this.generateOrderByCase(optionSets, this.columnName)} DESC NULLS LAST`)
      .toQuery();
  }
}
