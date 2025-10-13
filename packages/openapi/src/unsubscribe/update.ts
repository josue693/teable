import type { RouteConfig } from '@asteasolutions/zod-to-openapi';
import { axios } from '../axios';
import { registerRoute, urlBuilder } from '../utils';
import { z } from '../zod';
import { UNSUBSCRIBE } from './get';

export const updateSubscriptionRoSchema = z.object({
  subscriptionStatus: z.boolean(),
});

export type IUpdateSubscriptionRo = z.infer<typeof updateSubscriptionRoSchema>;

export const updateSubscriptionRoute: RouteConfig = registerRoute({
  method: 'post',
  path: UNSUBSCRIBE,
  description: 'unsubscribe a email',
  request: {
    params: z.object({
      token: z.string(),
    }),
    body: {
      content: {
        'application/json': {
          schema: updateSubscriptionRoSchema,
        },
      },
    },
  },
  responses: {
    200: {
      description: 'Successful response',
      content: {
        'application/json': {
          schema: z.boolean(),
        },
      },
    },
  },
  tags: ['unsubscribe'],
});

export const updateSubscription = async (token: string, ro: IUpdateSubscriptionRo) => {
  return axios.post<boolean>(urlBuilder(UNSUBSCRIBE, { token }), ro);
};
