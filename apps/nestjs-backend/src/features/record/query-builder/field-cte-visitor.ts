/* eslint-disable sonarjs/no-identical-functions */
/* eslint-disable sonarjs/cognitive-complexity */
/* eslint-disable sonarjs/no-duplicated-branches */
/* eslint-disable sonarjs/no-duplicate-string */
/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable @typescript-eslint/no-empty-function */
import { Logger } from '@nestjs/common';
import {
  DriverClient,
  FieldType,
  Relationship,
  type IFilter,
  type IFilterItem,
  type IFieldVisitor,
  type AttachmentFieldCore,
  type AutoNumberFieldCore,
  type CheckboxFieldCore,
  type CreatedByFieldCore,
  type CreatedTimeFieldCore,
  type DateFieldCore,
  type FormulaFieldCore,
  type LastModifiedByFieldCore,
  type LastModifiedTimeFieldCore,
  type LinkFieldCore,
  type LongTextFieldCore,
  type MultipleSelectFieldCore,
  type NumberFieldCore,
  type RatingFieldCore,
  type RollupFieldCore,
  type ConditionalRollupFieldCore,
  type IConditionalLookupOptions,
  type SingleLineTextFieldCore,
  type SingleSelectFieldCore,
  type UserFieldCore,
  type ButtonFieldCore,
  type Tables,
  type TableDomain,
  type ILinkFieldOptions,
  type FieldCore,
  type IRollupFieldOptions,
  DbFieldType,
  SortFunc,
  isFieldReferenceValue,
  isLinkLookupOptions,
  normalizeConditionalLimit,
  contains as FilterOperatorContains,
  doesNotContain as FilterOperatorDoesNotContain,
  hasAllOf as FilterOperatorHasAllOf,
  hasAnyOf as FilterOperatorHasAnyOf,
  hasNoneOf as FilterOperatorHasNoneOf,
  is as FilterOperatorIs,
  isAfter as FilterOperatorIsAfter,
  isAnyOf as FilterOperatorIsAnyOf,
  isBefore as FilterOperatorIsBefore,
  isExactly as FilterOperatorIsExactly,
  isGreater as FilterOperatorIsGreater,
  isGreaterEqual as FilterOperatorIsGreaterEqual,
  isLess as FilterOperatorIsLess,
  isLessEqual as FilterOperatorIsLessEqual,
  isNoneOf as FilterOperatorIsNoneOf,
  isNotEmpty as FilterOperatorIsNotEmpty,
  isNotExactly as FilterOperatorIsNotExactly,
  isEmpty as FilterOperatorIsEmpty,
  isOnOrAfter as FilterOperatorIsOnOrAfter,
  isOnOrBefore as FilterOperatorIsOnOrBefore,
} from '@teable/core';
import type { Knex } from 'knex';
import { match } from 'ts-pattern';
import type { IDbProvider } from '../../../db-provider/db.provider.interface';
import { ID_FIELD_NAME } from '../../field/constant';
import { FieldFormattingVisitor } from './field-formatting-visitor';
import { FieldSelectVisitor } from './field-select-visitor';
import type { IFieldSelectName } from './field-select.type';
import type {
  IMutableQueryBuilderState,
  IReadonlyQueryBuilderState,
} from './record-query-builder.interface';
import { RecordQueryBuilderManager, ScopedSelectionState } from './record-query-builder.manager';
import {
  getLinkUsesJunctionTable,
  getTableAliasFromTable,
  getOrderedFieldsByProjection,
  isDateLikeField,
  makeScopedLinkCteKey,
} from './record-query-builder.util';
import type { IRecordQueryDialectProvider } from './record-query-dialect.interface';

type ICteResult = void;

const JUNCTION_ALIAS = 'j';

const SUPPORTED_EQUALITY_RESIDUAL_OPERATORS = new Set<string>([
  FilterOperatorIs.value,
  FilterOperatorContains.value,
  FilterOperatorDoesNotContain.value,
  FilterOperatorIsGreater.value,
  FilterOperatorIsGreaterEqual.value,
  FilterOperatorIsLess.value,
  FilterOperatorIsLessEqual.value,
  FilterOperatorIsEmpty.value,
  FilterOperatorIsNotEmpty.value,
  FilterOperatorIsAnyOf.value,
  FilterOperatorIsNoneOf.value,
  FilterOperatorHasAnyOf.value,
  FilterOperatorHasAllOf.value,
  FilterOperatorHasNoneOf.value,
  FilterOperatorIsExactly.value,
  FilterOperatorIsNotExactly.value,
  FilterOperatorIsBefore.value,
  FilterOperatorIsAfter.value,
  FilterOperatorIsOnOrBefore.value,
  FilterOperatorIsOnOrAfter.value,
]);

const JSON_AGG_FUNCTIONS = new Set(['array_compact', 'array_unique']);

interface ILinkCteNode {
  table: TableDomain;
  linkField: LinkFieldCore;
  dependencies: Map<string, LinkFieldCore>;
}

class LinkCteScheduler {
  private readonly roots: Array<{ table: TableDomain; linkField: LinkFieldCore }> = [];
  private readonly registeredRootKeys = new Set<string>();
  private readonly visiting = new Set<string>();
  private readonly visited = new Set<string>();
  private readonly pathStack: Array<{
    key: string;
    node: { table: TableDomain; linkField: LinkFieldCore };
  }> = [];
  private readonly ordered: ILinkCteNode[] = [];
  private planned = false;

  constructor(
    private readonly tables: Tables,
    private readonly state: IReadonlyQueryBuilderState,
    private readonly entryTable: TableDomain,
    private readonly filteredMainFieldSet?: ReadonlySet<string>,
    private readonly filteredFieldSetsByTable?: ReadonlyMap<string, ReadonlySet<string>>
  ) {}

  registerProjectionFields(fields: FieldCore[]): void {
    for (const field of fields) {
      this.registerFieldDependencies(field);
    }
  }

  addRoot(table: TableDomain, linkField?: LinkFieldCore | null): void {
    if (!linkField || linkField.hasError) {
      return;
    }
    const key = this.getLinkNodeKey(table, linkField);
    if (this.registeredRootKeys.has(key)) {
      return;
    }
    this.registeredRootKeys.add(key);
    this.roots.push({ table, linkField });
  }

  plan(): ILinkCteNode[] {
    if (this.planned) {
      return this.ordered;
    }
    for (const root of this.roots) {
      this.visit(root.table, root.linkField);
    }
    this.planned = true;
    return this.ordered;
  }

  private visit(table: TableDomain, linkField: LinkFieldCore): void {
    if (this.state.hasFieldCte(makeScopedLinkCteKey(table, linkField.id))) {
      return;
    }
    const key = this.getLinkNodeKey(table, linkField);
    if (this.visited.has(key)) {
      return;
    }
    if (this.visiting.has(key)) {
      const cycleStartIndex = this.pathStack.findIndex((entry) => entry.key === key);
      const cyclePath = this.pathStack
        .slice(cycleStartIndex)
        .map((entry) => entry.node.linkField.name);
      cyclePath.push(linkField.name);
      throw new Error(`Detected circular link dependency: ${cyclePath.join(' -> ')}`);
    }

    this.visiting.add(key);
    this.pathStack.push({ key, node: { table, linkField } });

    const localDependencies = this.collectLocalDependencies(table, linkField);
    for (const dep of localDependencies.values()) {
      this.visit(table, dep);
    }

    const dependencies = this.collectLinkDependencies(table, linkField);
    const foreignTable = this.tables.getLinkForeignTable(linkField);
    if (foreignTable) {
      for (const dep of dependencies.values()) {
        this.visit(foreignTable, dep);
      }
    }

    this.pathStack.pop();
    this.visiting.delete(key);
    this.visited.add(key);
    this.ordered.push({ table, linkField, dependencies });
  }

  private registerFieldDependencies(field: FieldCore): void {
    const linkFields = field.getLinkFields(this.entryTable) ?? [];
    for (const lf of linkFields) {
      this.addRoot(this.entryTable, lf);
    }
    if (field.type === FieldType.Link) {
      const linkField = field as LinkFieldCore;
      this.addRoot(this.entryTable, linkField);
      const symmetricId = (linkField.options as ILinkFieldOptions)?.symmetricFieldId;
      const foreignTableId = linkField.options?.foreignTableId;
      if (symmetricId && foreignTableId) {
        const foreignTable = this.tables.getTable(foreignTableId);
        const symmetricField = foreignTable?.getField(symmetricId) as LinkFieldCore | undefined;
        if (foreignTable && symmetricField) {
          this.addRoot(foreignTable, symmetricField);
        }
      }
    }
    if (field.isLookup) {
      const lookupLinkId = getLinkFieldId(field.lookupOptions);
      if (lookupLinkId) {
        const lookupLink = this.entryTable.getField(lookupLinkId) as LinkFieldCore | undefined;
        if (lookupLink) {
          this.addRoot(this.entryTable, lookupLink);
        }
      }
    }
  }

  private getLinkNodeKey(table: TableDomain, linkField: LinkFieldCore): string {
    return `${table.id}:${linkField.id}`;
  }

  private collectLinkDependencies(
    scopeTable: TableDomain,
    linkField: LinkFieldCore
  ): Map<string, LinkFieldCore> {
    const foreignTable = this.tables.getLinkForeignTable(linkField);
    if (!foreignTable) {
      return new Map();
    }

    let lookupFields = linkField.getLookupFields(scopeTable);
    let rollupFields = linkField.getRollupFields(scopeTable);

    const scopedFilter =
      this.filteredFieldSetsByTable?.get(scopeTable.id) ??
      (scopeTable.id === this.entryTable.id ? this.filteredMainFieldSet : undefined);
    if (scopedFilter && scopedFilter.size === 0) {
      return new Map();
    }
    if (scopedFilter?.size) {
      lookupFields = lookupFields.filter((f) => scopedFilter.has(f.id));
      rollupFields = rollupFields.filter((f) => scopedFilter.has(f.id));
    }

    const nestedLinks = new Map<string, LinkFieldCore>();
    const ensureLinkDependency = (candidate?: LinkFieldCore | null) => {
      if (!candidate?.id || candidate.id === linkField.id) {
        return;
      }
      const symmetricId = (candidate.options as ILinkFieldOptions)?.symmetricFieldId;
      if (symmetricId && symmetricId === linkField.id && scopeTable.id !== this.entryTable.id) {
        return;
      }
      if (!nestedLinks.has(candidate.id)) {
        nestedLinks.set(candidate.id, candidate);
      }
    };

    const collectFromField = (field: FieldCore | undefined, visited: Set<string> = new Set()) => {
      if (!field || visited.has(field.id)) {
        return;
      }
      visited.add(field.id);

      if (field.type === FieldType.Link) {
        ensureLinkDependency(field as LinkFieldCore);
        const symmetric = (field.options as ILinkFieldOptions | undefined)?.symmetricFieldId;
        if (symmetric) {
          const symmetricField = this.entryTable.getField(symmetric) as LinkFieldCore | undefined;
          if (symmetricField) {
            ensureLinkDependency(symmetricField);
          }
        }
      }

      const viaLookupId = getLinkFieldId(field.lookupOptions);
      if (viaLookupId) {
        const nestedLinkField = foreignTable.getField(viaLookupId) as LinkFieldCore | undefined;
        ensureLinkDependency(nestedLinkField);
      }

      const directLinks = field.getLinkFields(foreignTable);
      for (const lf of directLinks) {
        ensureLinkDependency(lf);
      }

      const maybeGetReferenceFields = (
        field as unknown as {
          getReferenceFields?: (table: TableDomain) => FieldCore[];
        }
      ).getReferenceFields;
      if (typeof maybeGetReferenceFields === 'function') {
        const referencedFields = maybeGetReferenceFields.call(field, foreignTable) ?? [];
        for (const refField of referencedFields) {
          collectFromField(refField, visited);
        }
      }
    };

    const addDependenciesFromLookup = (lookupField: FieldCore) => {
      const target = lookupField.getForeignLookupField(foreignTable);
      if (target) {
        collectFromField(target);
      } else {
        const nestedId = lookupField.lookupOptions?.lookupFieldId;
        const nestedField = nestedId ? foreignTable.getField(nestedId) : undefined;
        if (nestedField?.type === FieldType.Link) {
          ensureLinkDependency(nestedField as LinkFieldCore);
        }
      }
    };

    const addDependenciesFromRollup = (rollupField: FieldCore) => {
      const target = rollupField.getForeignLookupField(foreignTable);
      if (target) {
        collectFromField(target);
      } else {
        const nestedId = rollupField.lookupOptions?.lookupFieldId;
        const nestedField = nestedId ? foreignTable.getField(nestedId) : undefined;
        if (nestedField?.type === FieldType.Link) {
          ensureLinkDependency(nestedField as LinkFieldCore);
        }
      }
    };

    for (const lookupField of lookupFields) {
      addDependenciesFromLookup(lookupField);
    }
    for (const rollupField of rollupFields) {
      addDependenciesFromRollup(rollupField);
    }

    collectFromField(linkField.getForeignLookupField(foreignTable));
    return nestedLinks;
  }

  private collectLocalDependencies(
    scopeTable: TableDomain,
    linkField: LinkFieldCore
  ): Map<string, LinkFieldCore> {
    const locals = new Map<string, LinkFieldCore>();
    if (!linkField.isLookup) {
      return locals;
    }
    const sourceLinkId = getLinkFieldId(linkField.lookupOptions);
    if (!sourceLinkId || sourceLinkId === linkField.id) {
      return locals;
    }
    const sourceLink = scopeTable.getField(sourceLinkId) as LinkFieldCore | undefined;
    if (sourceLink && !sourceLink.hasError) {
      locals.set(sourceLink.id, sourceLink);
    }
    return locals;
  }
}

class ScopedSelectionStateWithCteFilter extends ScopedSelectionState {
  private filtered?: ReadonlyMap<string, string>;

  constructor(
    base: IReadonlyQueryBuilderState,
    private readonly blocked: ReadonlySet<string>
  ) {
    super(base);
  }

  override getFieldCteMap(): ReadonlyMap<string, string> {
    if (!this.blocked?.size) {
      return super.getFieldCteMap();
    }
    if (!this.filtered) {
      const baseMap = super.getFieldCteMap();
      if (!baseMap.size) {
        this.filtered = baseMap;
      } else {
        const filtered = new Map<string, string>();
        for (const [fieldId, cteName] of baseMap) {
          if (!this.blocked.has(fieldId)) {
            filtered.set(fieldId, cteName);
          }
        }
        this.filtered = filtered;
      }
    }
    return this.filtered;
  }
}

function parseRollupFunctionName(expression: string): string {
  const match = expression.match(/^(\w+)\(\{values\}\)$/);
  if (!match) {
    throw new Error(`Invalid rollup expression: ${expression}`);
  }
  return match[1].toLowerCase();
}

function unwrapJsonAggregateForScalar(
  driver: DriverClient,
  expression: string,
  field: FieldCore,
  isJsonAggregate: boolean
): string {
  if (
    !isJsonAggregate ||
    field.isMultipleCellValue ||
    field.dbFieldType === DbFieldType.Json ||
    driver !== DriverClient.Pg
  ) {
    return expression;
  }
  return `(${expression}) ->> 0`;
}

class FieldCteSelectionVisitor implements IFieldVisitor<IFieldSelectName> {
  constructor(
    private readonly qb: Knex.QueryBuilder,
    private readonly dbProvider: IDbProvider,
    private readonly dialect: IRecordQueryDialectProvider,
    private readonly table: TableDomain,
    private readonly foreignTable: TableDomain,
    private readonly state: IReadonlyQueryBuilderState,
    private readonly joinedCtes?: Set<string>, // Track which CTEs are already JOINed in current scope
    private readonly isSingleValueRelationshipContext: boolean = false, // In ManyOne/OneOne CTEs, avoid aggregates
    private readonly foreignAliasOverride?: string,
    private readonly currentLinkFieldId?: string,
    private readonly resolvedLinkKeys?: ReadonlySet<string>
  ) {}
  private get fieldCteMap() {
    return this.state.getFieldCteMap();
  }

  private hasLinkCte(table: TableDomain, linkFieldId: string): boolean {
    const key = makeScopedLinkCteKey(table, linkFieldId);
    if (!this.fieldCteMap.has(key)) {
      return false;
    }
    if (this.resolvedLinkKeys && !this.resolvedLinkKeys.has(key)) {
      return false;
    }
    return true;
  }

  private getLinkCteName(table: TableDomain, linkFieldId: string): string | undefined {
    return this.fieldCteMap.get(makeScopedLinkCteKey(table, linkFieldId));
  }

  private buildBlockedKeySet(table: TableDomain, linkFieldId: string): Set<string> {
    const blocked = new Set<string>([makeScopedLinkCteKey(table, linkFieldId)]);
    if (this.currentLinkFieldId) {
      blocked.add(makeScopedLinkCteKey(this.table, this.currentLinkFieldId));
    }
    return blocked;
  }

  private getForeignAlias(): string {
    return this.foreignAliasOverride || getTableAliasFromTable(this.foreignTable);
  }

  private isSymmetricToCurrentLink(table: TableDomain, linkFieldId: string): boolean {
    if (!this.currentLinkFieldId) {
      return false;
    }
    const linkField = table.getField(linkFieldId) as LinkFieldCore | undefined;
    if (!linkField) {
      return false;
    }
    const symmetricId = (linkField.options as ILinkFieldOptions | undefined)?.symmetricFieldId;
    return symmetricId === this.currentLinkFieldId;
  }
  private getJsonAggregationFunction(fieldReference: string): string {
    return this.dialect.jsonAggregateNonNull(fieldReference);
  }

  private normalizeJsonAggregateExpression(expression: string): string {
    const trimmed = expression.trim();
    if (!trimmed) {
      return expression;
    }
    const upper = trimmed.toUpperCase();
    if (upper === 'NULL') {
      return 'NULL::jsonb';
    }
    if (upper === 'NULL::JSONB') {
      return trimmed;
    }
    if (upper.startsWith('NULL::')) {
      return `(${expression})::jsonb`;
    }
    return expression;
  }
  /**
   * Build a subquery (SELECT 1 WHERE ...) for foreign table filter using provider's filterQuery.
   * The subquery references the current foreign alias in-scope and carries proper bindings.
   */
  private buildForeignFilterSubquery(filter: IFilter): string {
    const foreignAlias = this.getForeignAlias();
    // Build selectionMap mapping foreign field ids to alias-qualified columns
    const selectionMap = new Map<string, string>();
    for (const f of this.foreignTable.fields.ordered) {
      selectionMap.set(f.id, `"${foreignAlias}"."${f.dbFieldName}"`);
    }
    // Build field map for filter compiler
    const fieldMap = this.foreignTable.fieldList.reduce(
      (map, f) => {
        map[f.id] = f as FieldCore;
        return map;
      },
      {} as Record<string, FieldCore>
    );
    // Build subquery with WHERE conditions
    const sub = this.qb.client.queryBuilder().select(this.qb.client.raw('1'));
    this.dbProvider
      .filterQuery(sub, fieldMap, filter, undefined, { selectionMap } as unknown as {
        selectionMap: Map<string, string>;
      })
      .appendQueryBuilder();
    return `(${sub.toQuery()})`;
  }
  /**
   * Generate rollup aggregation expression based on rollup function
   */
  // eslint-disable-next-line sonarjs/cognitive-complexity
  private generateRollupAggregation(
    expression: string,
    fieldExpression: string,
    targetField: FieldCore,
    orderByField?: string,
    rowPresenceExpr?: string
  ): string {
    const functionName = parseRollupFunctionName(expression);
    return this.dialect.rollupAggregate(functionName, fieldExpression, {
      targetField,
      orderByField,
      rowPresenceExpr,
    });
  }

  /**
   * Generate rollup expression for single-value relationships (ManyOne/OneOne)
   * Avoids using aggregate functions so GROUP BY is not required.
   */
  private generateSingleValueRollupAggregation(
    rollupField: FieldCore,
    targetField: FieldCore,
    expression: string,
    fieldExpression: string
  ): string {
    const functionName = parseRollupFunctionName(expression);
    return this.dialect.singleValueRollupAggregate(functionName, fieldExpression, {
      rollupField,
      targetField,
    });
  }
  private buildSingleValueRollup(
    field: FieldCore,
    targetField: FieldCore,
    expression: string
  ): string {
    const rollupOptions = field.options as IRollupFieldOptions;
    const rollupFilter = (field as FieldCore).getFilter?.();
    if (rollupFilter) {
      const sub = this.buildForeignFilterSubquery(rollupFilter);
      const filteredExpr =
        this.dbProvider.driver === DriverClient.Pg
          ? `CASE WHEN EXISTS ${sub} THEN ${expression} ELSE NULL END`
          : expression;
      return this.generateSingleValueRollupAggregation(
        field,
        targetField,
        rollupOptions.expression,
        filteredExpr
      );
    }
    return this.generateSingleValueRollupAggregation(
      field,
      targetField,
      rollupOptions.expression,
      expression
    );
  }
  private buildAggregateRollup(
    rollupField: FieldCore,
    targetField: FieldCore,
    expression: string
  ): string {
    const linkField = rollupField.getLinkField(this.table);
    const options = linkField?.options as ILinkFieldOptions | undefined;
    const rollupOptions = rollupField.options as IRollupFieldOptions;

    let orderByField: string | undefined;
    if (this.dbProvider.driver === DriverClient.Pg && linkField && options) {
      const usesJunctionTable = getLinkUsesJunctionTable(linkField);
      const hasOrderColumn = linkField.getHasOrderColumn();
      if (usesJunctionTable) {
        orderByField = hasOrderColumn
          ? `${JUNCTION_ALIAS}."${linkField.getOrderColumnName()}" IS NULL DESC, ${JUNCTION_ALIAS}."${linkField.getOrderColumnName()}" ASC, ${JUNCTION_ALIAS}."__id" ASC`
          : `${JUNCTION_ALIAS}."__id" ASC`;
      } else if (options.relationship === Relationship.OneMany) {
        const foreignAlias = this.getForeignAlias();
        orderByField = hasOrderColumn
          ? `"${foreignAlias}"."${linkField.getOrderColumnName()}" IS NULL DESC, "${foreignAlias}"."${linkField.getOrderColumnName()}" ASC, "${foreignAlias}"."__id" ASC`
          : `"${foreignAlias}"."__id" ASC`;
      }
    }

    const rowPresenceField = `"${this.getForeignAlias()}"."__id"`;

    const rollupFunctionName = parseRollupFunctionName(rollupOptions.expression);
    const aggregatesToJson = JSON_AGG_FUNCTIONS.has(rollupFunctionName);
    const buildAggregate = (expr: string) => {
      const aggregate = this.generateRollupAggregation(
        rollupOptions.expression,
        expr,
        targetField,
        orderByField,
        rowPresenceField
      );
      return unwrapJsonAggregateForScalar(
        this.dbProvider.driver,
        aggregate,
        rollupField,
        aggregatesToJson
      );
    };

    const rollupFilter = (rollupField as FieldCore).getFilter?.();
    if (rollupFilter && this.dbProvider.driver === DriverClient.Pg) {
      const sub = this.buildForeignFilterSubquery(rollupFilter);
      const filteredExpr = `CASE WHEN EXISTS ${sub} THEN ${expression} ELSE NULL END`;
      return buildAggregate(filteredExpr);
    }

    return buildAggregate(expression);
  }
  private visitLookupField(field: FieldCore): IFieldSelectName {
    if (!field.isLookup) {
      throw new Error('Not a lookup field');
    }

    // If this lookup field is marked as error, don't attempt to resolve.
    // Emit a typed NULL so the expression matches the physical column.
    if (field.hasError) {
      return this.dialect.typedNullFor(field.dbFieldType);
    }

    if (field.isConditionalLookup) {
      const cteName = this.fieldCteMap.get(field.id);
      if (!cteName) {
        return this.dialect.typedNullFor(field.dbFieldType);
      }
      return `"${cteName}"."conditional_lookup_${field.id}"`;
    }

    const qb = this.qb.client.queryBuilder();
    const createSelectVisitor = (blocked?: ReadonlySet<string>) => {
      const scopedState =
        blocked && blocked.size
          ? new ScopedSelectionStateWithCteFilter(this.state, blocked)
          : new ScopedSelectionState(this.state);
      return new FieldSelectVisitor(
        qb,
        this.dbProvider,
        this.foreignTable,
        scopedState,
        this.dialect,
        undefined,
        true,
        true
      );
    };
    const selectVisitor = createSelectVisitor();

    const evaluateFieldExpression = (target: FieldCore, blocked?: ReadonlySet<string>): string => {
      const visitorInstance =
        blocked && blocked.size ? createSelectVisitor(blocked) : selectVisitor;
      const result = target.accept(visitorInstance);
      return typeof result === 'string' ? result : result.toSQL().sql;
    };

    const foreignAlias = this.getForeignAlias();
    const targetLookupField = field.getForeignLookupField(this.foreignTable);

    if (!targetLookupField) {
      // Try to fetch via the CTE of the foreign link if present
      const nestedLinkFieldId = getLinkFieldId(field.lookupOptions);
      // Guard against self-referencing the CTE being defined (would require WITH RECURSIVE)
      if (
        nestedLinkFieldId &&
        !this.isSymmetricToCurrentLink(this.table, nestedLinkFieldId) &&
        this.hasLinkCte(this.table, nestedLinkFieldId) &&
        nestedLinkFieldId !== this.currentLinkFieldId
      ) {
        const nestedCteName = this.getLinkCteName(this.table, nestedLinkFieldId)!;
        // Check if this CTE is JOINed in current scope
        if (this.joinedCtes?.has(nestedLinkFieldId)) {
          const linkExpr = `"${nestedCteName}"."link_value"`;
          return this.isSingleValueRelationshipContext
            ? linkExpr
            : field.isMultipleCellValue
              ? this.getJsonAggregationFunction(linkExpr)
              : linkExpr;
        } else {
          // Fallback to subquery if CTE not JOINed in current scope
          const linkExpr = `((SELECT link_value FROM "${nestedCteName}" WHERE "${nestedCteName}"."main_record_id" = "${foreignAlias}"."${ID_FIELD_NAME}"))`;
          return this.isSingleValueRelationshipContext
            ? linkExpr
            : field.isMultipleCellValue
              ? this.getJsonAggregationFunction(linkExpr)
              : linkExpr;
        }
      }
      // If still not found or field has error, return NULL instead of throwing
      return this.dialect.typedNullFor(field.dbFieldType);
    }

    // If the target is a Link field, read its link_value from the JOINed CTE or subquery
    if (targetLookupField.type === FieldType.Link) {
      const nestedLinkFieldId = (targetLookupField as LinkFieldCore).id;
      if (
        !this.isSymmetricToCurrentLink(this.foreignTable, nestedLinkFieldId) &&
        this.hasLinkCte(this.foreignTable, nestedLinkFieldId) &&
        nestedLinkFieldId !== this.currentLinkFieldId
      ) {
        const nestedCteName = this.getLinkCteName(this.foreignTable, nestedLinkFieldId)!;
        // Check if this CTE is JOINed in current scope
        if (this.joinedCtes?.has(nestedLinkFieldId)) {
          const linkExpr = `"${nestedCteName}"."link_value"`;
          return this.isSingleValueRelationshipContext
            ? linkExpr
            : field.isMultipleCellValue
              ? this.getJsonAggregationFunction(linkExpr)
              : linkExpr;
        } else {
          const linkExpr = `((SELECT link_value FROM "${nestedCteName}" WHERE "${nestedCteName}"."main_record_id" = "${foreignAlias}"."${ID_FIELD_NAME}"))`;
          return this.isSingleValueRelationshipContext
            ? linkExpr
            : field.isMultipleCellValue
              ? this.getJsonAggregationFunction(linkExpr)
              : linkExpr;
        }
      }
      const blocked = this.buildBlockedKeySet(this.foreignTable, nestedLinkFieldId);
      const fallbackExpr = evaluateFieldExpression(targetLookupField, blocked);
      return this.isSingleValueRelationshipContext
        ? fallbackExpr
        : field.isMultipleCellValue
          ? this.getJsonAggregationFunction(fallbackExpr)
          : fallbackExpr;
    }

    // If the target is a Rollup field, read its precomputed rollup value from the link CTE
    if (targetLookupField.type === FieldType.Rollup) {
      const rollupField = targetLookupField as RollupFieldCore;
      const rollupLinkField = rollupField.getLinkField(this.foreignTable);
      if (rollupLinkField) {
        const nestedLinkFieldId = rollupLinkField.id;
        if (this.hasLinkCte(this.foreignTable, nestedLinkFieldId)) {
          const nestedCteName = this.getLinkCteName(this.foreignTable, nestedLinkFieldId)!;
          let expr: string;
          if (this.joinedCtes?.has(nestedLinkFieldId)) {
            expr = `"${nestedCteName}"."rollup_${rollupField.id}"`;
          } else {
            expr = `((SELECT "rollup_${rollupField.id}" FROM "${nestedCteName}" WHERE "${nestedCteName}"."main_record_id" = "${foreignAlias}"."${ID_FIELD_NAME}"))`;
          }
          return this.isSingleValueRelationshipContext
            ? expr
            : field.isMultipleCellValue
              ? this.getJsonAggregationFunction(expr)
              : expr;
        }
        const blocked = this.buildBlockedKeySet(this.foreignTable, nestedLinkFieldId);
        const expr = evaluateFieldExpression(targetLookupField, blocked);
        return this.isSingleValueRelationshipContext
          ? expr
          : field.isMultipleCellValue
            ? this.getJsonAggregationFunction(expr)
            : expr;
      }
      return evaluateFieldExpression(targetLookupField);
    }

    // If the target is itself a lookup, reference its precomputed value from the JOINed CTE or subquery
    let expression: string;
    if (targetLookupField.isLookup) {
      const nestedLinkFieldId = getLinkFieldId(targetLookupField.lookupOptions);
      if (
        nestedLinkFieldId &&
        !this.isSymmetricToCurrentLink(this.foreignTable, nestedLinkFieldId) &&
        this.hasLinkCte(this.foreignTable, nestedLinkFieldId) &&
        nestedLinkFieldId !== this.currentLinkFieldId
      ) {
        const nestedCteName = this.getLinkCteName(this.foreignTable, nestedLinkFieldId)!;
        if (this.joinedCtes?.has(nestedLinkFieldId)) {
          expression = `"${nestedCteName}"."lookup_${targetLookupField.id}"`;
        } else {
          expression = `((SELECT "lookup_${targetLookupField.id}" FROM "${nestedCteName}" WHERE "${nestedCteName}"."main_record_id" = "${foreignAlias}"."${ID_FIELD_NAME}"))`;
        }
      } else if (nestedLinkFieldId) {
        const blocked = this.buildBlockedKeySet(this.foreignTable, nestedLinkFieldId);
        expression = evaluateFieldExpression(targetLookupField, blocked);
      } else {
        expression = evaluateFieldExpression(targetLookupField);
      }
    } else {
      const targetFieldResult = targetLookupField.accept(selectVisitor);
      const defaultForeignAlias = getTableAliasFromTable(this.foreignTable);
      const baseExpression =
        typeof targetFieldResult === 'string' ? targetFieldResult : targetFieldResult.toSQL().sql;
      const normalizedBaseExpression =
        defaultForeignAlias !== foreignAlias
          ? baseExpression.replaceAll(`"${defaultForeignAlias}"`, `"${foreignAlias}"`)
          : baseExpression;
      expression = normalizedBaseExpression;

      // For Postgres multi-value lookups targeting datetime-like fields, normalize the
      // element expression to an ISO8601 UTC string so downstream JSON comparisons using
      // lexicographical ranges (jsonpath @ >= "..." && @ <= "...") behave correctly.
      // Do NOT alter single-value lookups to preserve native type comparisons in filters.
      if (
        this.dbProvider.driver === DriverClient.Pg &&
        field.isMultipleCellValue &&
        isDateLikeField(targetLookupField)
      ) {
        // Format: 2020-01-10T16:00:00.000Z, wrap as jsonb so downstream aggregation remains valid JSON.
        const isoUtcExpr = `to_char(${normalizedBaseExpression} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
        expression = `to_jsonb(${isoUtcExpr})`;
      }
    }
    // Build deterministic order-by for multi-value lookups using the link field configuration
    const linkForOrderingId = getLinkFieldId(field.lookupOptions);
    let orderByClause: string | undefined;
    if (linkForOrderingId) {
      try {
        const linkForOrdering = this.table.getField(linkForOrderingId) as LinkFieldCore;
        const usesJunctionTable = getLinkUsesJunctionTable(linkForOrdering);
        const hasOrderColumn = linkForOrdering.getHasOrderColumn();
        if (this.dbProvider.driver === DriverClient.Pg) {
          if (usesJunctionTable) {
            orderByClause = hasOrderColumn
              ? `${JUNCTION_ALIAS}."${linkForOrdering.getOrderColumnName()}" IS NULL DESC, ${JUNCTION_ALIAS}."${linkForOrdering.getOrderColumnName()}" ASC, ${JUNCTION_ALIAS}."__id" ASC`
              : `${JUNCTION_ALIAS}."__id" ASC`;
          } else {
            orderByClause = hasOrderColumn
              ? `"${foreignAlias}"."${linkForOrdering.getOrderColumnName()}" IS NULL DESC, "${foreignAlias}"."${linkForOrdering.getOrderColumnName()}" ASC, "${foreignAlias}"."__id" ASC`
              : `"${foreignAlias}"."__id" ASC`;
          }
        }
      } catch (_) {
        // ignore ordering if link field not found in current table context
      }
    }

    // Field-specific filter applied here
    const filter = field.getFilter?.();
    if (!filter) {
      if (!field.isMultipleCellValue || this.isSingleValueRelationshipContext) {
        return expression;
      }
      if (this.dbProvider.driver === DriverClient.Pg && orderByClause) {
        const sanitizedExpression = this.normalizeJsonAggregateExpression(expression);
        return `json_agg(${sanitizedExpression} ORDER BY ${orderByClause}) FILTER (WHERE ${sanitizedExpression} IS NOT NULL)`;
      }
      // For SQLite, ensure deterministic ordering by aggregating from an ordered correlated subquery
      if (this.dbProvider.driver === DriverClient.Sqlite) {
        try {
          const linkForOrderingId = getLinkFieldId(field.lookupOptions);
          const fieldCteMap = this.state.getFieldCteMap();
          const mainAlias = getTableAliasFromTable(this.table);
          const foreignDb = this.foreignTable.dbTableName;
          // Prefer order from link CTE's JSON array (preserves insertion order)
          if (
            linkForOrderingId &&
            fieldCteMap.has(makeScopedLinkCteKey(this.table, linkForOrderingId)) &&
            this.joinedCtes?.has(linkForOrderingId) &&
            linkForOrderingId !== this.currentLinkFieldId
          ) {
            const cteName = fieldCteMap.get(makeScopedLinkCteKey(this.table, linkForOrderingId))!;
            const exprForInner = expression.replaceAll(`"${this.getForeignAlias()}"`, '"f"');
            return `(
              SELECT CASE WHEN COUNT(*) > 0
                THEN json_group_array(CASE WHEN ${exprForInner} IS NOT NULL THEN ${exprForInner} END)
                ELSE NULL END
              FROM json_each(
                CASE
                  WHEN json_valid((SELECT "link_value" FROM "${cteName}" WHERE "${cteName}"."main_record_id" = "${mainAlias}"."__id"))
                   AND json_type((SELECT "link_value" FROM "${cteName}" WHERE "${cteName}"."main_record_id" = "${mainAlias}"."__id")) = 'array'
                  THEN (SELECT "link_value" FROM "${cteName}" WHERE "${cteName}"."main_record_id" = "${mainAlias}"."__id")
                  ELSE json('[]')
                END
              ) AS je
              JOIN "${foreignDb}" AS f ON f."__id" = json_extract(je.value, '$.id')
              ORDER BY je.key ASC
            )`;
          }
          // Fallback to FK/junction ordering using the current link field
          const baseLink = field as LinkFieldCore;
          const opts = baseLink.options as ILinkFieldOptions;
          const usesJunctionTable = getLinkUsesJunctionTable(baseLink);
          const hasOrderColumn = baseLink.getHasOrderColumn();
          const fkHost = opts.fkHostTableName!;
          const selfKey = opts.selfKeyName;
          const foreignKey = opts.foreignKeyName;
          const exprForInner = expression.replaceAll(`"${this.getForeignAlias()}"`, '"f"');
          if (usesJunctionTable) {
            const ordCol = hasOrderColumn ? `j."${baseLink.getOrderColumnName()}"` : undefined;
            const order = ordCol
              ? `(CASE WHEN ${ordCol} IS NULL THEN 0 ELSE 1 END) ASC, ${ordCol} ASC, j."__id" ASC`
              : `j."__id" ASC`;
            return `(
              SELECT CASE WHEN COUNT(*) > 0
                THEN json_group_array(CASE WHEN ${exprForInner} IS NOT NULL THEN ${exprForInner} END)
                ELSE NULL END
              FROM "${fkHost}" AS j
              JOIN "${foreignDb}" AS f ON j."${foreignKey}" = f."__id"
              WHERE j."${selfKey}" = "${mainAlias}"."__id"
              ORDER BY ${order}
            )`;
          }
          const ordCol = hasOrderColumn ? `f."${opts.selfKeyName}_order"` : undefined;
          const order = ordCol
            ? `(CASE WHEN ${ordCol} IS NULL THEN 0 ELSE 1 END) ASC, ${ordCol} ASC, f."__id" ASC`
            : `f."__id" ASC`;
          return `(
            SELECT CASE WHEN COUNT(*) > 0
              THEN json_group_array(CASE WHEN ${exprForInner} IS NOT NULL THEN ${exprForInner} END)
              ELSE NULL END
            FROM "${foreignDb}" AS f
            WHERE f."${selfKey}" = "${mainAlias}"."__id"
            ORDER BY ${order}
          )`;
        } catch (_) {
          // fallback to non-deterministic aggregation
        }
      }
      return this.getJsonAggregationFunction(expression);
    }
    const sub = this.buildForeignFilterSubquery(filter);

    if (!field.isMultipleCellValue || this.isSingleValueRelationshipContext) {
      // Single value: conditionally null out for both PG and SQLite
      if (this.dbProvider.driver === DriverClient.Pg) {
        return `CASE WHEN EXISTS ${sub} THEN ${expression} ELSE NULL END`;
      }
      return `CASE WHEN EXISTS ${sub} THEN ${expression} ELSE NULL END`;
    }

    if (this.dbProvider.driver === DriverClient.Pg) {
      const sanitizedExpression = this.normalizeJsonAggregateExpression(expression);
      if (orderByClause) {
        return `json_agg(${sanitizedExpression} ORDER BY ${orderByClause}) FILTER (WHERE (EXISTS ${sub}) AND ${sanitizedExpression} IS NOT NULL)`;
      }
      return `json_agg(${sanitizedExpression}) FILTER (WHERE (EXISTS ${sub}) AND ${sanitizedExpression} IS NOT NULL)`;
    }

    // SQLite: use a correlated, ordered subquery to produce deterministic ordering
    try {
      const linkForOrderingId = getLinkFieldId(field.lookupOptions);
      const fieldCteMap = this.state.getFieldCteMap();
      const mainAlias = getTableAliasFromTable(this.table);
      const foreignDb = this.foreignTable.dbTableName;
      // Prefer order from link CTE JSON array
      if (
        linkForOrderingId &&
        fieldCteMap.has(makeScopedLinkCteKey(this.table, linkForOrderingId)) &&
        this.joinedCtes?.has(linkForOrderingId) &&
        linkForOrderingId !== this.currentLinkFieldId
      ) {
        const cteName = fieldCteMap.get(makeScopedLinkCteKey(this.table, linkForOrderingId))!;
        const exprForInner = expression.replaceAll(`"${this.getForeignAlias()}"`, '"f"');
        const subForInner = sub.replaceAll(`"${this.getForeignAlias()}"`, '"f"');
        return `(
          SELECT CASE WHEN SUM(CASE WHEN (EXISTS ${subForInner}) THEN 1 ELSE 0 END) > 0
            THEN json_group_array(CASE WHEN (EXISTS ${subForInner}) AND ${exprForInner} IS NOT NULL THEN ${exprForInner} END)
            ELSE NULL END
          FROM json_each(
            CASE
              WHEN json_valid((SELECT "link_value" FROM "${cteName}" WHERE "${cteName}"."main_record_id" = "${mainAlias}"."__id"))
               AND json_type((SELECT "link_value" FROM "${cteName}" WHERE "${cteName}"."main_record_id" = "${mainAlias}"."__id")) = 'array'
              THEN (SELECT "link_value" FROM "${cteName}" WHERE "${cteName}"."main_record_id" = "${mainAlias}"."__id")
              ELSE json('[]')
            END
          ) AS je
          JOIN "${foreignDb}" AS f ON f."__id" = json_extract(je.value, '$.id')
          ORDER BY je.key ASC
        )`;
      }
      if (linkForOrderingId) {
        const linkForOrdering = this.table.getField(linkForOrderingId) as LinkFieldCore;
        const opts = linkForOrdering.options as ILinkFieldOptions;
        const usesJunctionTable = getLinkUsesJunctionTable(linkForOrdering);
        const hasOrderColumn = linkForOrdering.getHasOrderColumn();
        const fkHost = opts.fkHostTableName!;
        const selfKey = opts.selfKeyName;
        const foreignKey = opts.foreignKeyName;
        // Adapt expression and filter subquery to inner alias "f"
        const exprForInner = expression.replaceAll(`"${this.getForeignAlias()}"`, '"f"');
        const subForInner = sub.replaceAll(`"${this.getForeignAlias()}"`, '"f"');
        if (usesJunctionTable) {
          const ordCol = hasOrderColumn ? `j."${linkForOrdering.getOrderColumnName()}"` : undefined;
          const order = ordCol
            ? `(CASE WHEN ${ordCol} IS NULL THEN 0 ELSE 1 END) ASC, ${ordCol} ASC, j."__id" ASC`
            : `j."__id" ASC`;
          return `(
            SELECT CASE WHEN SUM(CASE WHEN (EXISTS ${subForInner}) THEN 1 ELSE 0 END) > 0
              THEN json_group_array(CASE WHEN (EXISTS ${subForInner}) AND ${exprForInner} IS NOT NULL THEN ${exprForInner} END)
              ELSE NULL END
            FROM "${fkHost}" AS j
            JOIN "${foreignDb}" AS f ON j."${foreignKey}" = f."__id"
            WHERE j."${selfKey}" = "${mainAlias}"."__id"
            ORDER BY ${order}
          )`;
        } else {
          const ordCol = hasOrderColumn ? `f."${selfKey}_order"` : undefined;
          const order = ordCol
            ? `(CASE WHEN ${ordCol} IS NULL THEN 0 ELSE 1 END) ASC, ${ordCol} ASC, f."__id" ASC`
            : `f."__id" ASC`;
          return `(
            SELECT CASE WHEN SUM(CASE WHEN (EXISTS ${subForInner}) THEN 1 ELSE 0 END) > 0
              THEN json_group_array(CASE WHEN (EXISTS ${subForInner}) AND ${exprForInner} IS NOT NULL THEN ${exprForInner} END)
              ELSE NULL END
            FROM "${foreignDb}" AS f
            WHERE f."${selfKey}" = "${mainAlias}"."__id"
            ORDER BY ${order}
          )`;
        }
      }
      // Default ordering using the current link field
      const baseLink = field as LinkFieldCore;
      const opts = baseLink.options as ILinkFieldOptions;
      const usesJunctionTable = getLinkUsesJunctionTable(baseLink);
      const hasOrderColumn = baseLink.getHasOrderColumn();
      const fkHost = opts.fkHostTableName!;
      const selfKey = opts.selfKeyName;
      const foreignKey = opts.foreignKeyName;
      const exprForInner = expression.replaceAll(`"${this.getForeignAlias()}"`, '"f"');
      const subForInner = sub.replaceAll(`"${this.getForeignAlias()}"`, '"f"');
      if (usesJunctionTable) {
        const ordCol = hasOrderColumn ? `j."${baseLink.getOrderColumnName()}"` : undefined;
        const order = ordCol
          ? `(CASE WHEN ${ordCol} IS NULL THEN 0 ELSE 1 END) ASC, ${ordCol} ASC, j."__id" ASC`
          : `j."__id" ASC`;
        return `(
          SELECT CASE WHEN SUM(CASE WHEN (EXISTS ${subForInner}) THEN 1 ELSE 0 END) > 0
            THEN json_group_array(CASE WHEN (EXISTS ${subForInner}) AND ${exprForInner} IS NOT NULL THEN ${exprForInner} END)
            ELSE NULL END
          FROM "${fkHost}" AS j
          JOIN "${foreignDb}" AS f ON j."${foreignKey}" = f."__id"
          WHERE j."${selfKey}" = "${mainAlias}"."__id"
          ORDER BY ${order}
        )`;
      }
      {
        const ordCol = hasOrderColumn ? `f."${selfKey}_order"` : undefined;
        const order = ordCol
          ? `(CASE WHEN ${ordCol} IS NULL THEN 0 ELSE 1 END) ASC, ${ordCol} ASC, f."__id" ASC`
          : `f."__id" ASC`;
        return `(
          SELECT CASE WHEN SUM(CASE WHEN (EXISTS ${subForInner}) THEN 1 ELSE 0 END) > 0
            THEN json_group_array(CASE WHEN (EXISTS ${subForInner}) AND ${exprForInner} IS NOT NULL THEN ${exprForInner} END)
            ELSE NULL END
          FROM "${foreignDb}" AS f
          WHERE f."${selfKey}" = "${mainAlias}"."__id"
          ORDER BY ${order}
        )`;
      }
    } catch (_) {
      // fall back
    }
    // Fallback: emulate FILTER and null removal using CASE inside the aggregate
    return `json_group_array(CASE WHEN (EXISTS ${sub}) AND ${expression} IS NOT NULL THEN ${expression} END)`;
  }
  visitNumberField(field: NumberFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitSingleLineTextField(field: SingleLineTextFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitLongTextField(field: LongTextFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitAttachmentField(field: AttachmentFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitCheckboxField(field: CheckboxFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitDateField(field: DateFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitRatingField(field: RatingFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitAutoNumberField(field: AutoNumberFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitLinkField(field: LinkFieldCore): IFieldSelectName {
    // If this Link field is itself a lookup (lookup-of-link), treat it as a generic lookup
    // so we resolve via nested CTEs instead of using physical link options.
    if (field.isLookup) {
      return this.visitLookupField(field);
    }
    const foreignTable = this.foreignTable;
    const driver = this.dbProvider.driver;
    const junctionAlias = JUNCTION_ALIAS;

    const targetLookupField = foreignTable.mustGetField(field.options.lookupFieldId);
    const usesJunctionTable = getLinkUsesJunctionTable(field);
    const foreignTableAlias = this.getForeignAlias();
    const isMultiValue = field.getIsMultiValue();
    const hasOrderColumn = field.getHasOrderColumn();

    // Use table alias for cleaner SQL
    const recordIdRef = `"${foreignTableAlias}"."${ID_FIELD_NAME}"`;

    const qb = this.qb.client.queryBuilder();
    const selectVisitor = new FieldSelectVisitor(
      qb,
      this.dbProvider,
      foreignTable,
      new ScopedSelectionState(this.state),
      this.dialect,
      foreignTableAlias,
      true,
      true
    );
    const targetFieldResult = targetLookupField.accept(selectVisitor);
    let rawSelectionExpression =
      typeof targetFieldResult === 'string' ? targetFieldResult : targetFieldResult.toSQL().sql;

    // Apply field formatting to build the display expression
    const formattingVisitor = new FieldFormattingVisitor(rawSelectionExpression, this.dialect);
    let formattedSelectionExpression = targetLookupField.accept(formattingVisitor);
    // Self-join: ensure expressions use the foreign alias override
    const defaultForeignAlias = getTableAliasFromTable(foreignTable);
    if (defaultForeignAlias !== foreignTableAlias) {
      formattedSelectionExpression = formattedSelectionExpression.replaceAll(
        `"${defaultForeignAlias}"`,
        `"${foreignTableAlias}"`
      );
      rawSelectionExpression = rawSelectionExpression.replaceAll(
        `"${defaultForeignAlias}"`,
        `"${foreignTableAlias}"`
      );
    }

    // Determine if this relationship should return multiple values (array) or single value (object)
    // Apply field-level filter for Link (only affects this column)
    const linkFieldFilter = (field as FieldCore).getFilter?.();
    const linkFilterSub = linkFieldFilter
      ? this.buildForeignFilterSubquery(linkFieldFilter)
      : undefined;
    return match(driver)
      .with(DriverClient.Pg, () => {
        // Build JSON object with id and title, then strip null values to remove title key when null
        const conditionalJsonObject = this.dialect.buildLinkJsonObject(
          recordIdRef,
          formattedSelectionExpression,
          rawSelectionExpression
        );

        if (isMultiValue) {
          // Filter out null records and return empty array if no valid records exist
          // Build an ORDER BY clause with NULLS FIRST semantics and stable tie-breaks using __id

          const orderByClause = match({ usesJunctionTable, hasOrderColumn })
            .with({ usesJunctionTable: true, hasOrderColumn: true }, () => {
              // ManyMany with order column: NULLS FIRST, then order column ASC, then junction __id ASC
              const linkField = field as LinkFieldCore;
              const ord = `${junctionAlias}."${linkField.getOrderColumnName()}"`;
              return `${ord} IS NULL DESC, ${ord} ASC, ${junctionAlias}."__id" ASC`;
            })
            .with({ usesJunctionTable: true, hasOrderColumn: false }, () => {
              // ManyMany without order column: order by junction __id
              return `${junctionAlias}."__id" ASC`;
            })
            .with({ usesJunctionTable: false, hasOrderColumn: true }, () => {
              // OneMany/ManyOne/OneOne with order column: NULLS FIRST, then order ASC, then foreign __id ASC
              const linkField = field as LinkFieldCore;
              const ord = `"${foreignTableAlias}"."${linkField.getOrderColumnName()}"`;
              return `${ord} IS NULL DESC, ${ord} ASC, "${foreignTableAlias}"."__id" ASC`;
            })
            .with({ usesJunctionTable: false, hasOrderColumn: false }, () => `${recordIdRef} ASC`) // Fallback to record ID if no order column is available
            .exhaustive();

          const baseFilter = `${recordIdRef} IS NOT NULL`;
          const appliedFilter = linkFilterSub
            ? `(EXISTS ${linkFilterSub}) AND ${baseFilter}`
            : baseFilter;
          const sanitizedExpression = this.normalizeJsonAggregateExpression(conditionalJsonObject);
          return `json_agg(${sanitizedExpression} ORDER BY ${orderByClause}) FILTER (WHERE ${appliedFilter})`;
        } else {
          // For single value relationships (ManyOne, OneOne) always return a single object or null
          const cond = linkFilterSub
            ? `${recordIdRef} IS NOT NULL AND EXISTS ${linkFilterSub}`
            : `${recordIdRef} IS NOT NULL`;
          return `CASE WHEN ${cond} THEN ${conditionalJsonObject} ELSE NULL END`;
        }
      })
      .with(DriverClient.Sqlite, () => {
        // Create conditional JSON object that only includes title if it's not null
        const conditionalJsonObject = this.dialect.buildLinkJsonObject(
          recordIdRef,
          formattedSelectionExpression,
          rawSelectionExpression
        );

        if (isMultiValue) {
          // For SQLite, build a correlated, ordered subquery to ensure deterministic ordering
          const usesJunctionTable = getLinkUsesJunctionTable(field);
          const hasOrderColumn = field.getHasOrderColumn();

          const opts = field.options as ILinkFieldOptions;
          return (
            this.dialect.buildDeterministicLookupAggregate({
              tableDbName: this.table.dbTableName,
              mainAlias: getTableAliasFromTable(this.table),
              foreignDbName: this.foreignTable.dbTableName,
              foreignAlias: foreignTableAlias,
              linkFieldOrderColumn: hasOrderColumn
                ? `${JUNCTION_ALIAS}."${field.getOrderColumnName()}"`
                : undefined,
              linkFieldHasOrderColumn: hasOrderColumn,
              usesJunctionTable,
              selfKeyName: opts.selfKeyName,
              foreignKeyName: opts.foreignKeyName,
              recordIdRef,
              formattedSelectionExpression,
              rawSelectionExpression,
              linkFilterSubquerySql: linkFilterSub,
              // Pass the actual junction table name here; the dialect will alias it as "j".
              junctionAlias: opts.fkHostTableName!,
            }) || this.getJsonAggregationFunction(conditionalJsonObject)
          );
        } else {
          const cond = linkFilterSub
            ? `${recordIdRef} IS NOT NULL AND EXISTS ${linkFilterSub}`
            : `${recordIdRef} IS NOT NULL`;
          return `CASE WHEN ${cond} THEN ${conditionalJsonObject} ELSE NULL END`;
        }
      })
      .otherwise(() => {
        throw new Error(`Unsupported database driver: ${driver}`);
      });
  }
  visitRollupField(field: RollupFieldCore): IFieldSelectName {
    if (field.isLookup) {
      return this.visitLookupField(field);
    }

    // If rollup field is marked as error, don't attempt to resolve; just return NULL
    if (field.hasError) {
      return this.dialect.typedNullFor(field.dbFieldType);
    }

    const qb = this.qb.client.queryBuilder();
    const scopedState = new ScopedSelectionState(this.state);
    const foreignAlias = this.getForeignAlias();
    const selectVisitor = new FieldSelectVisitor(
      qb,
      this.dbProvider,
      this.foreignTable,
      scopedState,
      this.dialect,
      foreignAlias,
      true,
      false
    );

    const evaluateFieldExpression = (target: FieldCore, blocked?: ReadonlySet<string>): string => {
      const visitor =
        blocked && blocked.size
          ? new FieldSelectVisitor(
              qb,
              this.dbProvider,
              this.foreignTable,
              new ScopedSelectionStateWithCteFilter(this.state, blocked),
              this.dialect,
              foreignAlias,
              true,
              false
            )
          : selectVisitor;
      const result = target.accept(visitor);
      return typeof result === 'string' ? result : result.toSQL().sql;
    };

    const targetLookupField = field.getForeignLookupField(this.foreignTable);
    if (!targetLookupField) {
      return this.dialect.typedNullFor(field.dbFieldType);
    }
    // If the target of rollup depends on a foreign link CTE, reference the JOINed CTE columns or use subquery
    if (targetLookupField.type === FieldType.Formula) {
      const formulaField = targetLookupField as FormulaFieldCore;
      const referenced = formulaField.getReferenceFields(this.foreignTable);
      for (const ref of referenced) {
        // Pre-generate nested CTEs for foreign-table link dependencies if any lookup/rollup targets are themselves lookup fields.
        ref.accept(selectVisitor);
      }
    }

    // If the target of rollup depends on a foreign link CTE, reference the JOINed CTE columns or use subquery
    let expression: string;
    const nestedLinkFieldId = getLinkFieldId(targetLookupField.lookupOptions);
    if (nestedLinkFieldId) {
      if (this.hasLinkCte(this.foreignTable, nestedLinkFieldId)) {
        const nestedCteName = this.getLinkCteName(this.foreignTable, nestedLinkFieldId)!;
        const columnName = targetLookupField.isLookup
          ? `lookup_${targetLookupField.id}`
          : targetLookupField.type === FieldType.Rollup
            ? `rollup_${targetLookupField.id}`
            : undefined;
        if (columnName) {
          // Check if this CTE is JOINed in current scope
          if (this.joinedCtes?.has(nestedLinkFieldId)) {
            expression = `"${nestedCteName}"."${columnName}"`;
          } else {
            expression = `((SELECT "${columnName}" FROM "${nestedCteName}" WHERE "${nestedCteName}"."main_record_id" = "${foreignAlias}"."${ID_FIELD_NAME}"))`;
          }
        } else {
          const blocked = this.buildBlockedKeySet(this.foreignTable, nestedLinkFieldId);
          expression = evaluateFieldExpression(targetLookupField, blocked);
        }
      } else {
        expression = evaluateFieldExpression(targetLookupField);
      }
    } else {
      const targetFieldResult = targetLookupField.accept(selectVisitor);
      expression =
        typeof targetFieldResult === 'string' ? targetFieldResult : targetFieldResult.toSQL().sql;
    }

    if (
      targetLookupField.isConditionalLookup ||
      (targetLookupField.type === FieldType.ConditionalRollup && !targetLookupField.isLookup)
    ) {
      const nestedCteName = this.fieldCteMap.get(targetLookupField.id);
      if (nestedCteName) {
        const columnName =
          targetLookupField.type === FieldType.ConditionalRollup && !targetLookupField.isLookup
            ? `conditional_rollup_${targetLookupField.id}`
            : `conditional_lookup_${targetLookupField.id}`;
        if (this.joinedCtes?.has(targetLookupField.id)) {
          expression = `"${nestedCteName}"."${columnName}"`;
        } else {
          expression = `((SELECT "${columnName}" FROM "${nestedCteName}" WHERE "${nestedCteName}"."main_record_id" = "${foreignAlias}"."${ID_FIELD_NAME}"))`;
        }
      }
    }
    const linkField = field.getLinkField(this.table);
    const options = linkField?.options as ILinkFieldOptions;
    const isSingleValueRelationship =
      options.relationship === Relationship.ManyOne || options.relationship === Relationship.OneOne;

    if (isSingleValueRelationship) {
      return this.buildSingleValueRollup(field, targetLookupField, expression);
    }
    return this.buildAggregateRollup(field, targetLookupField, expression);
  }

  visitConditionalRollupField(field: ConditionalRollupFieldCore): IFieldSelectName {
    const cteName = this.fieldCteMap.get(field.id);
    if (!cteName) {
      return this.dialect.typedNullFor(field.dbFieldType);
    }

    return `"${cteName}"."conditional_rollup_${field.id}"`;
  }
  visitSingleSelectField(field: SingleSelectFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitMultipleSelectField(field: MultipleSelectFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitFormulaField(field: FormulaFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitCreatedTimeField(field: CreatedTimeFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitLastModifiedTimeField(field: LastModifiedTimeFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitUserField(field: UserFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitCreatedByField(field: CreatedByFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitLastModifiedByField(field: LastModifiedByFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
  visitButtonField(field: ButtonFieldCore): IFieldSelectName {
    return this.visitLookupField(field);
  }
}

export class FieldCteVisitor implements IFieldVisitor<ICteResult> {
  private logger = new Logger(FieldCteVisitor.name);

  static generateCTENameForField(table: TableDomain, field: LinkFieldCore) {
    return `CTE_${getTableAliasFromTable(table)}_${field.id}`;
  }

  private readonly _table: TableDomain;
  private readonly state: IMutableQueryBuilderState;
  private readonly conditionalRollupGenerationStack = new Set<string>();
  private readonly conditionalLookupGenerationStack = new Set<string>();
  private filteredIdSet?: Set<string>;
  private readonly projection?: string[];
  private readonly resolvedLinkKeys = new Set<string>();
  private readonly filteredFieldSetsByTable?: ReadonlyMap<string, ReadonlySet<string>>;

  constructor(
    public readonly qb: Knex.QueryBuilder,
    private readonly dbProvider: IDbProvider,
    private readonly tables: Tables,
    state: IMutableQueryBuilderState | undefined,
    private readonly dialect: IRecordQueryDialectProvider,
    projection?: string[],
    filteredFieldSetsByTable?: ReadonlyMap<string, ReadonlySet<string>>
  ) {
    this.state = state ?? new RecordQueryBuilderManager('table');
    this._table = tables.mustGetEntryTable();
    this.projection = projection;
    this.filteredFieldSetsByTable = filteredFieldSetsByTable;
  }

  get table() {
    return this._table;
  }

  get fieldCteMap(): ReadonlyMap<string, string> {
    return this.state.getFieldCteMap();
  }

  private makeLinkCteKey(scopeTable: TableDomain, linkFieldId: string): string {
    return makeScopedLinkCteKey(scopeTable, linkFieldId);
  }

  private hasLinkFieldCte(scopeTable: TableDomain, linkFieldId: string): boolean {
    return this.state.hasFieldCte(this.makeLinkCteKey(scopeTable, linkFieldId));
  }

  private getLinkFieldCte(scopeTable: TableDomain, linkFieldId: string): string | undefined {
    return this.state.getCteName(this.makeLinkCteKey(scopeTable, linkFieldId));
  }

  private setLinkFieldCte(
    scopeTable: TableDomain,
    linkField: LinkFieldCore,
    cteName: string
  ): void {
    const key = this.makeLinkCteKey(scopeTable, linkField.id);
    this.state.setFieldCte(key, cteName);
    this.resolvedLinkKeys.add(key);
  }

  private createLinkCteScheduler(): LinkCteScheduler {
    return new LinkCteScheduler(
      this.tables,
      this.state,
      this.table,
      this.filteredIdSet,
      this.filteredFieldSetsByTable
    );
  }

  private buildLinkCtesFromScheduler(scheduler: LinkCteScheduler): void {
    const plan = scheduler.plan();
    for (const node of plan) {
      this.buildLinkCteScope(node.table, node.linkField, node.dependencies);
    }
  }

  private getCteNameForField(fieldId: string): string | undefined {
    return this.state.getCteName(fieldId);
  }

  private getBaseIdSubquery(): Knex.QueryBuilder | undefined {
    const baseCteName = this.state.getBaseCteName();
    if (!baseCteName) {
      return undefined;
    }
    return this.qb.client.queryBuilder().select(ID_FIELD_NAME).from(baseCteName);
  }

  private applyMainTableRestriction(builder: Knex.QueryBuilder, alias: string): void {
    const subquery = this.getBaseIdSubquery();
    if (!subquery) {
      return;
    }
    builder.whereIn(`${alias}.${ID_FIELD_NAME}`, subquery);
  }

  private fromTableWithRestriction(
    builder: Knex.QueryBuilder,
    table: TableDomain,
    alias: string
  ): void {
    const source =
      table.id === this.table.id
        ? this.state.getOriginalMainTableSource() ?? table.dbTableName
        : table.dbTableName;
    builder.from(`${source} as ${alias}`);
    if (table.id === this.table.id) {
      this.applyMainTableRestriction(builder, alias);
    }
  }

  private buildLinkCteScope(
    scopeTable: TableDomain,
    linkField: LinkFieldCore,
    nestedLinks: Map<string, LinkFieldCore> = new Map()
  ): void {
    if (this.hasLinkFieldCte(scopeTable, linkField.id)) {
      if (scopeTable.id === this.table.id) {
        const existing = this.getLinkFieldCte(scopeTable, linkField.id);
        if (existing) {
          this.ensureLinkCteJoined(existing);
        }
      }
      return;
    }

    const foreignTable = this.tables.getLinkForeignTable(linkField);
    if (!foreignTable) {
      return;
    }

    const cteName = FieldCteVisitor.generateCTENameForField(scopeTable, linkField);
    const usesJunctionTable = getLinkUsesJunctionTable(linkField);
    const options = linkField.options as ILinkFieldOptions;
    const mainAlias = getTableAliasFromTable(scopeTable);
    const foreignAlias = getTableAliasFromTable(foreignTable);
    const foreignAliasUsed = foreignAlias === mainAlias ? `${foreignAlias}_f` : foreignAlias;
    const { fkHostTableName, selfKeyName, foreignKeyName, relationship } = options;
    const isMainScope = scopeTable.id === this.table.id;

    let lookupFields = linkField.getLookupFields(scopeTable);
    let rollupFields = linkField.getRollupFields(scopeTable);
    if (isMainScope && this.filteredIdSet) {
      lookupFields = lookupFields.filter((f) => this.filteredIdSet?.has(f.id));
      rollupFields = rollupFields.filter((f) => this.filteredIdSet?.has(f.id));
    }

    const ensureConditionalComputedCteForField = (targetField?: FieldCore) => {
      if (!targetField) {
        return;
      }
      if (targetField.type === FieldType.ConditionalRollup && !targetField.isLookup) {
        this.generateConditionalRollupFieldCteForScope(
          foreignTable,
          targetField as ConditionalRollupFieldCore
        );
      }
      if (targetField.isConditionalLookup) {
        const opts = targetField.getConditionalLookupOptions?.();
        if (opts) {
          this.generateConditionalLookupFieldCteForScope(foreignTable, targetField, opts);
        }
      }
    };

    const addConditionalTargets = (fields: FieldCore[]) => {
      for (const field of fields) {
        const target = field.getForeignLookupField(foreignTable);
        if (target) {
          ensureConditionalComputedCteForField(target);
        }
      }
    };

    addConditionalTargets(lookupFields);
    addConditionalTargets(rollupFields);
    ensureConditionalComputedCteForField(linkField.getForeignLookupField(foreignTable));

    this.qb.with(cteName, (cqb) => {
      const joinedCtesInScope = new Set(nestedLinks.keys());

      const visitor = new FieldCteSelectionVisitor(
        cqb,
        this.dbProvider,
        this.dialect,
        scopeTable,
        foreignTable,
        this.state,
        joinedCtesInScope,
        usesJunctionTable || relationship === Relationship.OneMany ? false : true,
        foreignAliasUsed,
        linkField.id,
        this.resolvedLinkKeys
      );
      const linkValue = linkField.accept(visitor);

      cqb.select(`${mainAlias}.${ID_FIELD_NAME} as main_record_id`);
      const linkValueExpr =
        this.dbProvider.driver === DriverClient.Pg ? `${linkValue}::jsonb` : `${linkValue}`;
      cqb.select(cqb.client.raw(`${linkValueExpr} as link_value`));

      for (const lookupField of lookupFields) {
        const lookupVisitor = new FieldCteSelectionVisitor(
          cqb,
          this.dbProvider,
          this.dialect,
          scopeTable,
          foreignTable,
          this.state,
          joinedCtesInScope,
          usesJunctionTable || relationship === Relationship.OneMany ? false : true,
          foreignAliasUsed,
          linkField.id,
          this.resolvedLinkKeys
        );
        const lookupValue = lookupField.accept(lookupVisitor);
        cqb.select(cqb.client.raw(`${lookupValue} as "lookup_${lookupField.id}"`));
      }

      for (const rollupField of rollupFields) {
        const rollupVisitor = new FieldCteSelectionVisitor(
          cqb,
          this.dbProvider,
          this.dialect,
          scopeTable,
          foreignTable,
          this.state,
          joinedCtesInScope,
          usesJunctionTable || relationship === Relationship.OneMany ? false : true,
          foreignAliasUsed,
          linkField.id,
          this.resolvedLinkKeys
        );
        const rollupValue = rollupField.accept(rollupVisitor);
        const value = typeof rollupValue === 'string' ? rollupValue : rollupValue.toQuery();
        const castedRollupValue = this.castExpressionForDbType(value, rollupField);
        cqb.select(cqb.client.raw(`${castedRollupValue} as "rollup_${rollupField.id}"`));
      }

      const joinNestedLinkCtes = () => {
        for (const nestedLinkFieldId of nestedLinks.keys()) {
          const nestedCteName = this.getLinkFieldCte(foreignTable, nestedLinkFieldId);
          if (!nestedCteName) {
            continue;
          }
          cqb.leftJoin(
            nestedCteName,
            `${nestedCteName}.main_record_id`,
            `${foreignAliasUsed}.__id`
          );
        }
      };

      if (usesJunctionTable) {
        this.fromTableWithRestriction(cqb, scopeTable, mainAlias);
        cqb
          .leftJoin(
            `${fkHostTableName} as ${JUNCTION_ALIAS}`,
            `${mainAlias}.__id`,
            `${JUNCTION_ALIAS}.${selfKeyName}`
          )
          .leftJoin(
            `${foreignTable.dbTableName} as ${foreignAliasUsed}`,
            `${JUNCTION_ALIAS}.${foreignKeyName}`,
            `${foreignAliasUsed}.__id`
          );
        joinNestedLinkCtes();
        cqb.groupBy(`${mainAlias}.__id`);

        if (this.dbProvider.driver === DriverClient.Sqlite) {
          if (linkField.getHasOrderColumn()) {
            const ordCol = `${JUNCTION_ALIAS}.${linkField.getOrderColumnName()}`;
            cqb.orderByRaw(`(CASE WHEN ${ordCol} IS NULL THEN 0 ELSE 1 END) ASC`);
            cqb.orderBy(ordCol, 'asc');
          }
          cqb.orderBy(`${JUNCTION_ALIAS}.__id`, 'asc');
        }
      } else if (relationship === Relationship.OneMany) {
        this.fromTableWithRestriction(cqb, scopeTable, mainAlias);
        cqb.leftJoin(
          `${foreignTable.dbTableName} as ${foreignAliasUsed}`,
          `${mainAlias}.__id`,
          `${foreignAliasUsed}.${selfKeyName}`
        );
        joinNestedLinkCtes();
        cqb.groupBy(`${mainAlias}.__id`);

        if (this.dbProvider.driver === DriverClient.Sqlite) {
          if (linkField.getHasOrderColumn()) {
            cqb.orderByRaw(
              `(CASE WHEN ${foreignAliasUsed}.${selfKeyName}_order IS NULL THEN 0 ELSE 1 END) ASC`
            );
            cqb.orderBy(`${foreignAliasUsed}.${selfKeyName}_order`, 'asc');
          }
          cqb.orderBy(`${foreignAliasUsed}.__id`, 'asc');
        }
      } else if (relationship === Relationship.ManyOne || relationship === Relationship.OneOne) {
        const isForeignKeyInMainTable = fkHostTableName === scopeTable.dbTableName;
        this.fromTableWithRestriction(cqb, scopeTable, mainAlias);
        if (isForeignKeyInMainTable) {
          cqb.leftJoin(
            `${foreignTable.dbTableName} as ${foreignAliasUsed}`,
            `${mainAlias}.${foreignKeyName}`,
            `${foreignAliasUsed}.__id`
          );
        } else {
          cqb.leftJoin(
            `${foreignTable.dbTableName} as ${foreignAliasUsed}`,
            `${foreignAliasUsed}.${selfKeyName}`,
            `${mainAlias}.__id`
          );
        }
        joinNestedLinkCtes();
      }
    });

    this.setLinkFieldCte(scopeTable, linkField, cteName);
    if (isMainScope) {
      this.ensureLinkCteJoined(cteName);
    }
  }

  /**
   * Apply an explicit cast to align the SQL expression type with the target field's DB column type.
   * This prevents Postgres from rejecting UPDATE ... FROM assignments due to type mismatches
   * (e.g., assigning a text expression to a double precision column).
   */
  private castExpressionForDbType(expression: string, field: FieldCore): string {
    if (this.dbProvider.driver !== DriverClient.Pg) return expression;
    const castSuffix = (() => {
      switch (field.dbFieldType) {
        case DbFieldType.Json:
          return '::jsonb';
        case DbFieldType.Integer:
          return '::integer';
        case DbFieldType.Real:
          return '::double precision';
        case DbFieldType.DateTime:
          return '::timestamptz';
        case DbFieldType.Boolean:
          return '::boolean';
        case DbFieldType.Blob:
          return '::bytea';
        case DbFieldType.Text:
        default:
          return '::text';
      }
    })();
    return `(${expression})${castSuffix}`;
  }

  private shouldUseFormattedExpressionForAggregation(fn: string): boolean {
    switch (fn) {
      case 'array_join':
      case 'concatenate':
        return true;
      default:
        return false;
    }
  }

  private rollupFunctionSupportsOrdering(expression: string): boolean {
    const fn = parseRollupFunctionName(expression);
    switch (fn) {
      case 'array_join':
      case 'array_compact':
      case 'concatenate':
        return true;
      default:
        return false;
    }
  }

  private buildConditionalRollupAggregation(
    rollupExpression: string,
    fieldExpression: string,
    targetField: FieldCore,
    foreignAlias: string,
    orderByClause?: string
  ): string {
    const fn = parseRollupFunctionName(rollupExpression);
    return this.dialect.rollupAggregate(fn, fieldExpression, {
      targetField,
      rowPresenceExpr: `"${foreignAlias}"."${ID_FIELD_NAME}"`,
      orderByField: orderByClause,
      flattenNestedArray: fn === 'array_compact' && !!targetField.isConditionalLookup,
    });
  }

  private extractConditionalEqualityJoinPlan(
    filter: IFilter | null | undefined,
    table: TableDomain,
    foreignTable: TableDomain,
    mainAlias: string,
    foreignAlias: string
  ): {
    joinKeys: Array<{ alias: string; hostExpr: string; foreignExpr: string }>;
    residualFilter: IFilter | null;
  } | null {
    if (!filter?.filterSet?.length) return null;

    const joinKeys: Array<{ alias: string; hostExpr: string; foreignExpr: string }> = [];

    type FilterNode = Exclude<IFilter, null>;

    const buildResidual = (
      current: IFilter | null | undefined
    ): { ok: boolean; residual: IFilter } => {
      if (!current?.filterSet?.length) return { ok: false, residual: null };
      const conjunction = current.conjunction ?? 'and';
      if (conjunction !== 'and') return { ok: false, residual: null };

      const residualEntries: Array<FilterNode | IFilterItem> = [];

      for (const entry of current.filterSet ?? []) {
        if (!entry) continue;
        if ('fieldId' in entry) {
          const item = entry as IFilterItem;

          if (item.operator === FilterOperatorIs.value && isFieldReferenceValue(item.value)) {
            const hostRef = item.value;
            if (hostRef.tableId && hostRef.tableId !== table.id) {
              return { ok: false, residual: null };
            }
            const foreignField = foreignTable.getField(item.fieldId);
            const hostField = table.getField(hostRef.fieldId);
            if (!foreignField || !hostField) {
              return { ok: false, residual: null };
            }
            if (isDateLikeField(foreignField) || isDateLikeField(hostField)) {
              return { ok: false, residual: null };
            }
            const caseInsensitive =
              foreignField.dbFieldType === DbFieldType.Text &&
              hostField.dbFieldType === DbFieldType.Text;
            const alias = `__cr_key_${joinKeys.length}`;
            const foreignExpr = caseInsensitive
              ? `LOWER("${foreignAlias}"."${foreignField.dbFieldName}")`
              : `"${foreignAlias}"."${foreignField.dbFieldName}"`;
            const hostExpr = caseInsensitive
              ? `LOWER("${mainAlias}"."${hostField.dbFieldName}")`
              : `"${mainAlias}"."${hostField.dbFieldName}"`;
            joinKeys.push({ alias, hostExpr, foreignExpr });
            continue;
          }

          if (isFieldReferenceValue(item.value)) {
            return { ok: false, residual: null };
          }

          if (!SUPPORTED_EQUALITY_RESIDUAL_OPERATORS.has(item.operator)) {
            return { ok: false, residual: null };
          }

          residualEntries.push(entry);
          continue;
        }

        if ('filterSet' in entry) {
          const nested = buildResidual(entry as IFilter);
          if (!nested.ok) {
            return { ok: false, residual: null };
          }
          const nestedResidual = nested.residual;
          if (nestedResidual && 'filterSet' in nestedResidual && nestedResidual.filterSet?.length) {
            residualEntries.push(nestedResidual as FilterNode);
          }
          continue;
        }

        return { ok: false, residual: null };
      }

      if (!residualEntries.length) {
        return { ok: true, residual: null };
      }

      return {
        ok: true,
        residual: {
          conjunction,
          filterSet: residualEntries,
        } as FilterNode,
      };
    };

    const { ok, residual } = buildResidual(filter);
    if (!ok || !joinKeys.length) return null;
    return { joinKeys, residualFilter: residual };
  }

  private getConditionalEqualityFallback(aggregationFn: string, field: FieldCore): string | null {
    switch (aggregationFn) {
      case 'countall':
      case 'count':
      case 'sum':
      case 'average':
        return '0::double precision';
      case 'max':
      case 'min': {
        const dbType = field.dbFieldType ?? DbFieldType.Text;
        return this.dialect.typedNullFor(dbType);
      }
      default:
        return null;
    }
  }

  private resolveConditionalComputedTargetExpression(
    targetField: FieldCore,
    foreignTable: TableDomain,
    foreignAlias: string,
    selectVisitor: FieldSelectVisitor
  ): string {
    if (targetField.type === FieldType.ConditionalRollup && !targetField.isLookup) {
      const conditionalTarget = targetField as ConditionalRollupFieldCore;
      this.generateConditionalRollupFieldCteForScope(foreignTable, conditionalTarget);
      const nestedCteName = this.getCteNameForField(conditionalTarget.id);
      if (nestedCteName) {
        return `((SELECT "conditional_rollup_${conditionalTarget.id}" FROM "${nestedCteName}" WHERE "${nestedCteName}"."main_record_id" = "${foreignAlias}"."${ID_FIELD_NAME}"))`;
      }
      const fallback = conditionalTarget.accept(selectVisitor);
      return typeof fallback === 'string' ? fallback : fallback.toSQL().sql;
    }

    if (targetField.isConditionalLookup) {
      const options = targetField.getConditionalLookupOptions?.();
      if (options) {
        this.generateConditionalLookupFieldCteForScope(foreignTable, targetField, options);
      }
      const nestedCteName = this.getCteNameForField(targetField.id);
      if (nestedCteName) {
        const column =
          targetField.type === FieldType.ConditionalRollup
            ? `conditional_rollup_${targetField.id}`
            : `conditional_lookup_${targetField.id}`;
        return `((SELECT "${column}" FROM "${nestedCteName}" WHERE "${nestedCteName}"."main_record_id" = "${foreignAlias}"."${ID_FIELD_NAME}"))`;
      }
    }

    const targetSelect = targetField.accept(selectVisitor);
    return typeof targetSelect === 'string' ? targetSelect : targetSelect.toSQL().sql;
  }

  private generateConditionalRollupFieldCte(field: ConditionalRollupFieldCore): void {
    this.generateConditionalRollupFieldCteForScope(this.table, field);
  }

  private generateConditionalRollupFieldCteForScope(
    table: TableDomain,
    field: ConditionalRollupFieldCore
  ): void {
    if (field.hasError) return;
    if (this.state.getFieldCteMap().has(field.id)) return;
    if (this.conditionalRollupGenerationStack.has(field.id)) return;

    this.conditionalRollupGenerationStack.add(field.id);
    try {
      const {
        foreignTableId,
        lookupFieldId,
        expression = 'countall({values})',
        filter,
        sort,
        limit,
      } = field.options;
      if (!foreignTableId || !lookupFieldId) {
        return;
      }

      const foreignTable = this.tables.getTable(foreignTableId);
      if (!foreignTable) {
        return;
      }

      const targetField = foreignTable.getField(lookupFieldId);
      if (!targetField) {
        return;
      }

      const joinToMain = table === this.table;

      const cteName = `CTE_REF_${field.id}`;
      const mainAlias = getTableAliasFromTable(table);
      const foreignAlias = getTableAliasFromTable(foreignTable);
      const foreignAliasUsed = foreignAlias === mainAlias ? `${foreignAlias}_ref` : foreignAlias;

      const qb = this.qb.client.queryBuilder();
      const selectVisitor = new FieldSelectVisitor(
        qb,
        this.dbProvider,
        foreignTable,
        new ScopedSelectionState(this.state),
        this.dialect,
        foreignAliasUsed,
        true,
        false
      );

      const rawExpression = this.resolveConditionalComputedTargetExpression(
        targetField,
        foreignTable,
        foreignAliasUsed,
        selectVisitor
      );
      const formattingVisitor = new FieldFormattingVisitor(rawExpression, this.dialect);
      const formattedExpression = targetField.accept(formattingVisitor);

      const aggregationFn = parseRollupFunctionName(expression);
      const aggregationInputExpression = this.shouldUseFormattedExpressionForAggregation(
        aggregationFn
      )
        ? formattedExpression
        : rawExpression;

      const supportsOrdering = this.rollupFunctionSupportsOrdering(expression);

      let orderByClause: string | undefined;
      if (supportsOrdering && sort?.fieldId) {
        const sortField = foreignTable.getField(sort.fieldId);
        if (sortField) {
          let sortExpression = this.resolveConditionalComputedTargetExpression(
            sortField,
            foreignTable,
            foreignAliasUsed,
            selectVisitor
          );

          const defaultForeignAlias = getTableAliasFromTable(foreignTable);
          if (defaultForeignAlias !== foreignAliasUsed) {
            sortExpression = sortExpression.replaceAll(
              `"${defaultForeignAlias}"`,
              `"${foreignAliasUsed}"`
            );
          }

          const direction = sort.order === SortFunc.Desc ? 'DESC' : 'ASC';
          orderByClause = `${sortExpression} ${direction}`;
        }
      }

      const aggregateExpression = this.buildConditionalRollupAggregation(
        expression,
        aggregationInputExpression,
        targetField,
        foreignAliasUsed,
        supportsOrdering ? orderByClause : undefined
      );
      const aggregatesToJson = JSON_AGG_FUNCTIONS.has(aggregationFn);
      const normalizedAggregateExpression = unwrapJsonAggregateForScalar(
        this.dbProvider.driver,
        aggregateExpression,
        field,
        aggregatesToJson
      );
      const castedAggregateExpression = this.castExpressionForDbType(
        normalizedAggregateExpression,
        field
      );

      const equalityEnabledFns = new Set(['countall', 'count', 'sum', 'average', 'max', 'min']);
      const canUseEqualityPlan =
        equalityEnabledFns.has(aggregationFn) &&
        !supportsOrdering &&
        !orderByClause &&
        !sort?.fieldId;
      const equalityPlan = canUseEqualityPlan
        ? this.extractConditionalEqualityJoinPlan(
            filter,
            table,
            foreignTable,
            mainAlias,
            foreignAliasUsed
          )
        : null;

      if (equalityPlan?.joinKeys.length) {
        const countsAlias = `__cr_counts_${field.id}`;
        const countsQuery = this.qb.client
          .queryBuilder()
          .from(`${foreignTable.dbTableName} as ${foreignAliasUsed}`);
        for (const cond of equalityPlan.joinKeys) {
          countsQuery.select(this.qb.client.raw(`${cond.foreignExpr} as "${cond.alias}"`));
          countsQuery.groupByRaw(cond.foreignExpr);
        }
        countsQuery.select(this.qb.client.raw(`${castedAggregateExpression} as "reference_value"`));

        if (equalityPlan.residualFilter) {
          const fieldMap = foreignTable.fieldList.reduce(
            (map, f) => {
              map[f.id] = f as FieldCore;
              return map;
            },
            {} as Record<string, FieldCore>
          );

          const selectionMap = new Map<string, IFieldSelectName>();
          for (const f of foreignTable.fields.ordered) {
            selectionMap.set(f.id, `"${foreignAliasUsed}"."${f.dbFieldName}"`);
          }

          const fieldReferenceSelectionMap = new Map<string, string>();
          const fieldReferenceFieldMap = new Map<string, FieldCore>();
          for (const mainField of table.fields.ordered) {
            fieldReferenceSelectionMap.set(
              mainField.id,
              `"${mainAlias}"."${mainField.dbFieldName}"`
            );
            fieldReferenceFieldMap.set(mainField.id, mainField as FieldCore);
          }

          this.dbProvider
            .filterQuery(countsQuery, fieldMap, equalityPlan.residualFilter, undefined, {
              selectionMap,
              fieldReferenceSelectionMap,
              fieldReferenceFieldMap,
            })
            .appendQueryBuilder();
        }

        const equalityFallback = this.getConditionalEqualityFallback(aggregationFn, field);
        this.qb.with(cteName, (cqb) => {
          cqb.select(`${mainAlias}.${ID_FIELD_NAME} as main_record_id`);
          const refValueSql =
            equalityFallback != null
              ? `COALESCE(${countsAlias}."reference_value", ${equalityFallback})`
              : `${countsAlias}."reference_value"`;
          cqb.select(cqb.client.raw(`${refValueSql} as "conditional_rollup_${field.id}"`));
          this.fromTableWithRestriction(cqb, table, mainAlias);
          cqb.leftJoin(
            this.qb.client.raw(`(${countsQuery.toQuery()}) as ${countsAlias}`),
            (join) => {
              for (const cond of equalityPlan.joinKeys) {
                join.on(
                  this.qb.client.raw(cond.hostExpr),
                  '=',
                  this.qb.client.raw(`${countsAlias}."${cond.alias}"`)
                );
              }
            }
          );
        });

        if (joinToMain && !this.state.isCteJoined(cteName)) {
          this.qb.leftJoin(cteName, `${mainAlias}.${ID_FIELD_NAME}`, `${cteName}.main_record_id`);
          this.state.markCteJoined(cteName);
        }

        this.state.setFieldCte(field.id, cteName);
        return;
      }

      const aggregateSourceQuery = this.qb.client
        .queryBuilder()
        .select('*')
        .from(`${foreignTable.dbTableName} as ${foreignAliasUsed}`);

      if (filter) {
        const fieldMap = foreignTable.fieldList.reduce(
          (map, f) => {
            map[f.id] = f as FieldCore;
            return map;
          },
          {} as Record<string, FieldCore>
        );

        const selectionMap = new Map<string, IFieldSelectName>();
        for (const f of foreignTable.fields.ordered) {
          selectionMap.set(f.id, `"${foreignAliasUsed}"."${f.dbFieldName}"`);
        }

        const fieldReferenceSelectionMap = new Map<string, string>();
        const fieldReferenceFieldMap = new Map<string, FieldCore>();
        for (const mainField of table.fields.ordered) {
          fieldReferenceSelectionMap.set(mainField.id, `"${mainAlias}"."${mainField.dbFieldName}"`);
          fieldReferenceFieldMap.set(mainField.id, mainField as FieldCore);
        }

        this.dbProvider
          .filterQuery(aggregateSourceQuery, fieldMap, filter, undefined, {
            selectionMap,
            fieldReferenceSelectionMap,
            fieldReferenceFieldMap,
          })
          .appendQueryBuilder();
      }

      if (supportsOrdering && orderByClause) {
        aggregateSourceQuery.orderByRaw(orderByClause);
      }

      if (supportsOrdering) {
        const resolvedLimit = normalizeConditionalLimit(limit);
        aggregateSourceQuery.limit(resolvedLimit);
      }

      const aggregateQuery = this.qb.client
        .queryBuilder()
        .from(aggregateSourceQuery.as(foreignAliasUsed));

      aggregateQuery.select(this.qb.client.raw(`${castedAggregateExpression} as reference_value`));
      const aggregateSql = aggregateQuery.toQuery();

      this.qb.with(cteName, (cqb) => {
        cqb
          .select(`${mainAlias}.${ID_FIELD_NAME} as main_record_id`)
          .select(cqb.client.raw(`(${aggregateSql}) as "conditional_rollup_${field.id}"`))
          .modify((builder) => this.fromTableWithRestriction(builder, table, mainAlias));
      });

      if (joinToMain && !this.state.isCteJoined(cteName)) {
        this.qb.leftJoin(cteName, `${mainAlias}.${ID_FIELD_NAME}`, `${cteName}.main_record_id`);
        this.state.markCteJoined(cteName);
      }

      this.state.setFieldCte(field.id, cteName);
    } finally {
      this.conditionalRollupGenerationStack.delete(field.id);
    }
  }

  private generateConditionalLookupFieldCte(field: FieldCore, options: IConditionalLookupOptions) {
    this.generateConditionalLookupFieldCteForScope(this.table, field, options);
  }

  private generateConditionalLookupFieldCteForScope(
    table: TableDomain,
    field: FieldCore,
    options: IConditionalLookupOptions
  ): void {
    if (field.hasError) return;
    if (this.state.getFieldCteMap().has(field.id)) return;
    if (this.conditionalLookupGenerationStack.has(field.id)) return;

    this.conditionalLookupGenerationStack.add(field.id);
    try {
      const { foreignTableId, lookupFieldId, filter, sort, limit } = options;
      if (!foreignTableId || !lookupFieldId) {
        return;
      }

      const foreignTable = this.tables.getTable(foreignTableId);
      if (!foreignTable) {
        return;
      }

      const targetField = foreignTable.getField(lookupFieldId);
      if (!targetField) {
        return;
      }

      const joinToMain = table === this.table;

      const cteName = `CTE_CONDITIONAL_LOOKUP_${field.id}`;
      const mainAlias = getTableAliasFromTable(table);
      const foreignAlias = getTableAliasFromTable(foreignTable);
      const foreignAliasUsed = foreignAlias === mainAlias ? `${foreignAlias}_ref` : foreignAlias;

      const qb = this.qb.client.queryBuilder();
      const selectVisitor = new FieldSelectVisitor(
        qb,
        this.dbProvider,
        foreignTable,
        new ScopedSelectionState(this.state),
        this.dialect,
        foreignAliasUsed,
        true,
        false
      );

      const rawExpression = this.resolveConditionalComputedTargetExpression(
        targetField,
        foreignTable,
        foreignAliasUsed,
        selectVisitor
      );

      let orderByClause: string | undefined;
      if (sort?.fieldId) {
        const sortField = foreignTable.getField(sort.fieldId);
        if (sortField) {
          let sortExpression = this.resolveConditionalComputedTargetExpression(
            sortField,
            foreignTable,
            foreignAliasUsed,
            selectVisitor
          );

          const defaultForeignAlias = getTableAliasFromTable(foreignTable);
          if (defaultForeignAlias !== foreignAliasUsed) {
            sortExpression = sortExpression.replaceAll(
              `"${defaultForeignAlias}"`,
              `"${foreignAliasUsed}"`
            );
          }

          const direction = sort.order === SortFunc.Desc ? 'DESC' : 'ASC';
          orderByClause = `${sortExpression} ${direction}`;
        }
      }

      const aggregateExpressionInfo =
        field.type === FieldType.ConditionalRollup
          ? {
              expression: this.dialect.jsonAggregateNonNull(rawExpression, orderByClause),
              isJsonAggregate: true,
            }
          : (() => {
              const expression = this.buildConditionalRollupAggregation(
                'array_compact({values})',
                rawExpression,
                targetField,
                foreignAliasUsed,
                orderByClause
              );
              return {
                expression,
                isJsonAggregate: JSON_AGG_FUNCTIONS.has('array_compact'),
              };
            })();
      const normalizedAggregateExpression = unwrapJsonAggregateForScalar(
        this.dbProvider.driver,
        aggregateExpressionInfo.expression,
        field,
        aggregateExpressionInfo.isJsonAggregate
      );
      const castedAggregateExpression = this.castExpressionForDbType(
        normalizedAggregateExpression,
        field
      );

      const applyConditionalFilter = (targetQb: Knex.QueryBuilder) => {
        if (!filter) return;

        const fieldMap = foreignTable.fieldList.reduce(
          (map, f) => {
            map[f.id] = f as FieldCore;
            return map;
          },
          {} as Record<string, FieldCore>
        );

        const selectionMap = new Map<string, IFieldSelectName>();
        for (const f of foreignTable.fields.ordered) {
          selectionMap.set(f.id, `"${foreignAliasUsed}"."${f.dbFieldName}"`);
        }

        const fieldReferenceSelectionMap = new Map<string, string>();
        const fieldReferenceFieldMap = new Map<string, FieldCore>();
        for (const mainField of table.fields.ordered) {
          fieldReferenceSelectionMap.set(mainField.id, `"${mainAlias}"."${mainField.dbFieldName}"`);
          fieldReferenceFieldMap.set(mainField.id, mainField as FieldCore);
        }

        this.dbProvider
          .filterQuery(targetQb, fieldMap, filter, undefined, {
            selectionMap,
            fieldReferenceSelectionMap,
            fieldReferenceFieldMap,
          })
          .appendQueryBuilder();
      };

      const aggregateSourceQuery = this.qb.client
        .queryBuilder()
        .select('*')
        .from(`${foreignTable.dbTableName} as ${foreignAliasUsed}`);

      applyConditionalFilter(aggregateSourceQuery);

      if (orderByClause) {
        aggregateSourceQuery.orderByRaw(orderByClause);
      }

      const resolvedLimit = normalizeConditionalLimit(limit);
      aggregateSourceQuery.limit(resolvedLimit);

      const aggregateQuery = this.qb.client
        .queryBuilder()
        .from(aggregateSourceQuery.as(foreignAliasUsed));

      aggregateQuery.select(this.qb.client.raw(`${castedAggregateExpression} as reference_value`));

      const aggregateSql = aggregateQuery.toQuery();
      const lookupAlias = `conditional_lookup_${field.id}`;
      const rollupAlias = `conditional_rollup_${field.id}`;

      this.qb.with(cteName, (cqb) => {
        cqb.select(`${mainAlias}.${ID_FIELD_NAME} as main_record_id`);
        cqb.select(cqb.client.raw(`(${aggregateSql}) as "${lookupAlias}"`));
        if (field.type === FieldType.ConditionalRollup) {
          cqb.select(cqb.client.raw(`(${aggregateSql}) as "${rollupAlias}"`));
        }
        this.fromTableWithRestriction(cqb, table, mainAlias);
      });

      if (joinToMain && !this.state.isCteJoined(cteName)) {
        this.qb.leftJoin(cteName, `${mainAlias}.${ID_FIELD_NAME}`, `${cteName}.main_record_id`);
        this.state.markCteJoined(cteName);
      }

      this.state.setFieldCte(field.id, cteName);
    } finally {
      this.conditionalLookupGenerationStack.delete(field.id);
    }
  }

  public build() {
    const list = getOrderedFieldsByProjection(this.table, this.projection) as FieldCore[];
    this.filteredIdSet = new Set(list.map((f) => f.id));

    const linkScheduler = this.createLinkCteScheduler();
    linkScheduler.registerProjectionFields(list);

    this.buildLinkCtesFromScheduler(linkScheduler);

    for (const field of list) {
      if (field.isConditionalLookup) {
        const options = field.getConditionalLookupOptions?.();
        if (options) {
          this.generateConditionalLookupFieldCte(field, options);
        }
      }
    }

    for (const field of list) {
      field.accept(this);
    }
  }

  private generateLinkFieldCte(linkField: LinkFieldCore): void {
    const existingCteName = this.getLinkFieldCte(this.table, linkField.id);
    if (existingCteName) {
      this.ensureLinkCteJoined(existingCteName);
      return;
    }

    const scheduler = this.createLinkCteScheduler();
    scheduler.addRoot(this.table, linkField);
    this.buildLinkCtesFromScheduler(scheduler);

    const finalCteName = this.getLinkFieldCte(this.table, linkField.id);
    if (finalCteName) {
      this.ensureLinkCteJoined(finalCteName);
    }
  }

  visitNumberField(_field: NumberFieldCore): void {}
  visitSingleLineTextField(_field: SingleLineTextFieldCore): void {}
  visitLongTextField(_field: LongTextFieldCore): void {}
  visitAttachmentField(_field: AttachmentFieldCore): void {}
  visitCheckboxField(_field: CheckboxFieldCore): void {}
  visitDateField(_field: DateFieldCore): void {}
  visitRatingField(_field: RatingFieldCore): void {}
  visitAutoNumberField(_field: AutoNumberFieldCore): void {}
  visitLinkField(field: LinkFieldCore): void {
    if (field.hasError) return;
    const existingCteName = this.getLinkFieldCte(this.table, field.id);
    if (existingCteName) {
      this.ensureLinkCteJoined(existingCteName);
      return;
    }
    this.generateLinkFieldCte(field);
  }
  visitRollupField(_field: RollupFieldCore): void {}
  visitConditionalRollupField(field: ConditionalRollupFieldCore): void {
    this.generateConditionalRollupFieldCte(field);
  }
  visitSingleSelectField(_field: SingleSelectFieldCore): void {}
  visitMultipleSelectField(_field: MultipleSelectFieldCore): void {}
  visitFormulaField(_field: FormulaFieldCore): void {}
  visitCreatedTimeField(_field: CreatedTimeFieldCore): void {}
  visitLastModifiedTimeField(_field: LastModifiedTimeFieldCore): void {}
  visitUserField(_field: UserFieldCore): void {}
  visitCreatedByField(_field: CreatedByFieldCore): void {}
  visitLastModifiedByField(_field: LastModifiedByFieldCore): void {}
  visitButtonField(_field: ButtonFieldCore): void {}

  private ensureLinkCteJoined(cteName: string): void {
    if (this.state.isCteJoined(cteName)) {
      return;
    }
    const mainAlias = getTableAliasFromTable(this.table);
    this.qb.leftJoin(cteName, `${mainAlias}.${ID_FIELD_NAME}`, `${cteName}.main_record_id`);
    this.state.markCteJoined(cteName);
  }
}
const getLinkFieldId = (options: FieldCore['lookupOptions']): string | undefined => {
  return options && isLinkLookupOptions(options) ? options.linkFieldId : undefined;
};
