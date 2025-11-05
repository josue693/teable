import { z } from 'zod';

export const localizationSchema = z.object({
  i18nKey: z.string(),
  context: z.record(z.string(), z.unknown()).optional(),
});

export type ILocalization = z.infer<typeof localizationSchema>;
