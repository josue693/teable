/* eslint-disable @typescript-eslint/naming-convention */
import { CellValueType, FieldType } from '../../field/constant';
import type { IOperator } from './operator';
import {
  getValidFilterOperators,
  is,
  isAfter,
  isBefore,
  isGreater,
  isGreaterEqual,
  isLess,
  isLessEqual,
  isNot,
  isOnOrAfter,
  isOnOrBefore,
} from './operator';

type FieldShape = {
  cellValueType: CellValueType;
  type: FieldType;
  isMultipleCellValue?: boolean;
};

export type FieldReferenceComparisonKind =
  | 'user'
  | 'link'
  | 'attachment'
  | 'number'
  | 'boolean'
  | 'dateTime'
  | 'string';

const USER_FIELD_TYPES = new Set<FieldType>([
  FieldType.User,
  FieldType.CreatedBy,
  FieldType.LastModifiedBy,
]);

const LINK_FIELD_TYPES = new Set<FieldType>([FieldType.Link]);

const ATTACHMENT_FIELD_TYPES = new Set<FieldType>([FieldType.Attachment]);

export function getFieldReferenceComparisonKind(field: FieldShape): FieldReferenceComparisonKind {
  if (USER_FIELD_TYPES.has(field.type)) {
    return 'user';
  }

  if (LINK_FIELD_TYPES.has(field.type)) {
    return 'link';
  }

  if (ATTACHMENT_FIELD_TYPES.has(field.type)) {
    return 'attachment';
  }

  switch (field.cellValueType) {
    case CellValueType.Number:
      return 'number';
    case CellValueType.Boolean:
      return 'boolean';
    case CellValueType.DateTime:
      return 'dateTime';
    case CellValueType.String:
    default:
      return 'string';
  }
}

export function isFieldReferenceComparable(field: FieldShape, reference: FieldShape): boolean {
  return getFieldReferenceComparisonKind(field) === getFieldReferenceComparisonKind(reference);
}

const FIELD_REFERENCE_OPERATOR_MAP: Record<CellValueType, ReadonlySet<IOperator>> = {
  [CellValueType.String]: new Set<IOperator>([is.value, isNot.value]),
  [CellValueType.Number]: new Set<IOperator>([
    is.value,
    isNot.value,
    isGreater.value,
    isGreaterEqual.value,
    isLess.value,
    isLessEqual.value,
  ]),
  [CellValueType.Boolean]: new Set<IOperator>([is.value, isNot.value]),
  [CellValueType.DateTime]: new Set<IOperator>([
    is.value,
    isNot.value,
    isBefore.value,
    isAfter.value,
    isOnOrBefore.value,
    isOnOrAfter.value,
  ]),
};

export function getFieldReferenceSupportedOperators(field: FieldShape): IOperator[] {
  const validOperators = getValidFilterOperators(field);
  const supported = FIELD_REFERENCE_OPERATOR_MAP[field.cellValueType] ?? new Set<IOperator>();

  return validOperators.filter((op) => supported.has(op));
}

export function isFieldReferenceOperatorSupported(
  field: FieldShape,
  operator?: IOperator | null
): boolean {
  if (!operator) {
    return false;
  }
  const supported = getFieldReferenceSupportedOperators(field);
  return supported.includes(operator);
}
