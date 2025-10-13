import type { RouteConfig } from '@asteasolutions/zod-to-openapi';
import { axios } from '../axios';
import { MailType } from '../mail/types';
import { registerRoute, urlBuilder } from '../utils';
import { z } from '../zod';

export const UNSUBSCRIBE = '/unsubscribe/{token}';

export const unsubscribeBaseSchema = z.object({
  type: z.nativeEnum(MailType),
  baseId: z.string(),
  email: z.string(),
  subscriptionStatus: z.boolean().optional(),
});

export type IUnsubscribeBase = z.infer<typeof unsubscribeBaseSchema>;

export const unsubscribeAutomationSendEmailActionSchema = unsubscribeBaseSchema.extend({
  type: z.literal(MailType.AutomationSendEmailAction),
  actionId: z.string(),
});

export type IUnsubscribeAutomationSendEmailAction = z.infer<
  typeof unsubscribeAutomationSendEmailActionSchema
>;

export type IUnsubscribe = IUnsubscribeAutomationSendEmailAction;

export const unsubscribeVoSchema = unsubscribeBaseSchema.extend({
  title: z.string().optional(),
  url: z.string().optional().openapi({ description: 'detail url' }),
});

export type IUnsubscribeVo = z.infer<typeof unsubscribeVoSchema>;

export const getUnSubscribeRoute: RouteConfig = registerRoute({
  method: 'get',
  path: UNSUBSCRIBE,
  description: 'unsubscribe a email',
  request: {
    params: z.object({
      token: z.string(),
    }),
  },
  responses: {
    200: {
      description: 'Successful response',
      content: {
        'application/json': {
          schema: unsubscribeVoSchema,
        },
      },
    },
  },
  tags: ['unsubscribe'],
});

export const getUnSubscribe = async (token: string) => {
  return axios.get<IUnsubscribeVo>(urlBuilder(UNSUBSCRIBE, { token }));
};
