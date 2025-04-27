import { FieldType, timeZoneStringSchema } from '@teable/core';
import z from 'zod';

export enum SUPPORTEDTYPE {
  CSV = 'csv',
  EXCEL = 'excel',
}

export const analyzeRoSchema = z.object({
  attachmentUrl: z.string().url().trim(),
  fileType: z.nativeEnum(SUPPORTEDTYPE),
});

export const analyzeColumnSchema = z.object({
  type: z.nativeEnum(FieldType),
  name: z.string(),
});

export const analyzeVoSchema = z.object({
  worksheets: z.record(
    z.string(),
    z.object({
      name: z.string(),
      columns: analyzeColumnSchema.array(),
    })
  ),
});

export type IAnalyzeRo = z.infer<typeof analyzeRoSchema>;

export type IAnalyzeVo = z.infer<typeof analyzeVoSchema>;

export type IAnalyzeColumn = z.infer<typeof analyzeColumnSchema>;

export type IValidateTypes =
  | FieldType.Number
  | FieldType.Date
  | FieldType.LongText
  | FieldType.Checkbox
  | FieldType.SingleLineText;

export const importColumnSchema = analyzeColumnSchema.extend({
  sourceColumnIndex: z.number(),
});

export const importSheetItem = z.object({
  name: z.string().openapi({
    description: 'worksheet name',
  }),
  columns: importColumnSchema.array().openapi({
    description: 'import columns',
  }),
  useFirstRowAsHeader: z.boolean().openapi({
    description: 'if true, will use first row as header',
  }),
  importData: z.boolean().openapi({
    description: 'if true, will import data',
  }),
});

export const importOptionSchema = importSheetItem.pick({
  useFirstRowAsHeader: true,
  importData: true,
});

export const importCommonRoSchema = z.object({
  attachmentUrl: z.string().url().openapi({
    description: 'import file url',
  }),
  fileType: z.nativeEnum(SUPPORTEDTYPE).openapi({
    description: 'import file type, csv or excel',
  }),
  notification: z.boolean().optional().openapi({
    description: 'if true, will send import result notification after import',
  }),
});

export const importOptionRoSchema = importCommonRoSchema.extend({
  worksheets: z.record(z.string(), importSheetItem).openapi({
    description: 'import worksheets map',
  }),
  tz: timeZoneStringSchema,
});

export const inplaceImportOptionRoSchema = importCommonRoSchema
  .extend({
    insertConfig: z.object({
      sourceWorkSheetKey: z.string().openapi({
        description: 'source worksheet key',
      }),
      excludeFirstRow: z.boolean().openapi({
        description: 'if true, will exclude first row',
      }),
      sourceColumnMap: z.record(z.number().nullable()).openapi({
        description: 'source column to target column map',
      }),
    }),
  })
  .openapi({
    description: 'inplace import option',
  });

export type IImportColumn = z.infer<typeof importColumnSchema>;

export type IImportOptionRo = z.infer<typeof importOptionRoSchema>;

export type IImportSheetItem = z.infer<typeof importSheetItem>;

export type IImportOption = z.infer<typeof importOptionSchema>;

export type IInplaceImportOptionRo = z.infer<typeof inplaceImportOptionRoSchema>;
