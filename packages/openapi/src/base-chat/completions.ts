import { getRecordsRoSchema } from '../record/get-list';
import { registerRoute } from '../utils';
import { z } from '../zod';
import { chatEventMessage } from './types';

export const CHAT_COMPLETIONS = '/base/{baseId}/chat/completions/{sessionId}';

export const chatCompletionsTableQuerySchema = getRecordsRoSchema
  .pick({
    filter: true,
    orderBy: true,
    projection: true,
    viewId: true,
    ignoreViewQuery: true,
    search: true,
  })
  .extend({
    tableId: z.string(),
  });

export type IChatCompletionsTableQuery = z.infer<typeof chatCompletionsTableQuerySchema>;

export const chatCompletionsRoSchema = z.object({
  question: z.string(),
  reasoning: z.boolean().optional(),
  tableQuery: chatCompletionsTableQuerySchema.optional(),
});

export type IChatCompletionsRo = z.infer<typeof chatCompletionsRoSchema>;

export const chatCompletionsRoute = registerRoute({
  method: 'post',
  path: CHAT_COMPLETIONS,
  description: 'Chat completions',
  request: {
    params: z.object({
      baseId: z.string(),
      sessionId: z.string(),
    }),
    body: {
      content: {
        'application/json': {
          schema: chatCompletionsRoSchema,
        },
      },
    },
  },
  responses: {
    200: {
      description: 'Chat completions',
      content: {
        'text/event-stream': {
          schema: chatEventMessage,
        },
      },
    },
  },
  tags: ['base-chat'],
});
