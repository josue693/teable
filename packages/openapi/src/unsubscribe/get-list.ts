import type { RouteConfig } from '@asteasolutions/zod-to-openapi';
import { axios } from '../axios';
import { registerRoute, urlBuilder } from '../utils';
import { z } from '../zod';

export const UNSUBSCRIBE_LIST = '/unsubscribe/list/{baseId}';

export const unsubscribeListVoSchema = z.array(
  z.object({
    email: z.string(),
    createdTime: z.string(),
  })
);

export type IUnsubscribeListVo = z.infer<typeof unsubscribeListVoSchema>;

export const getUnSubscribeListRoute: RouteConfig = registerRoute({
  method: 'get',
  path: UNSUBSCRIBE_LIST,
  description: 'get unsubscribe list',
  request: {
    params: z.object({
      baseId: z.string(),
    }),
  },
  responses: {
    200: {
      description: 'Successful response',
      content: {
        'application/json': {
          schema: unsubscribeListVoSchema,
        },
      },
    },
  },
  tags: ['unsubscribe'],
});

export const getUnSubscribeList = async (baseId: string) => {
  return axios.get<IUnsubscribeListVo>(urlBuilder(UNSUBSCRIBE_LIST, { baseId }));
};
