import { axios } from '../axios';
import { registerRoute, urlBuilder } from '../utils';
import { z } from '../zod';

export const CREATE_CHAT_CONVERSATION = '/base/{baseId}/chat/conversation';

export const createChatConversationRoSchema = z.object({
  question: z.string().optional(),
});

export type ICreateChatConversationRo = z.infer<typeof createChatConversationRoSchema>;

export const createChatConversationVoSchema = z.object({
  sessionId: z.string(),
  name: z.string(),
});

export type ICreateChatConversationVo = z.infer<typeof createChatConversationVoSchema>;

export const createChatConversationRoute = registerRoute({
  method: 'post',
  path: CREATE_CHAT_CONVERSATION,
  description: 'Create chat conversation',
  request: {
    params: z.object({
      baseId: z.string(),
    }),
    body: {
      content: {
        'application/json': { schema: createChatConversationRoSchema },
      },
    },
  },
  responses: {
    201: {
      description: 'Create chat conversation',
      content: { 'application/json': { schema: createChatConversationVoSchema } },
    },
  },
  tags: ['base-chat'],
});

export const createChatConversation = async (baseId: string, question?: string) => {
  return axios.post<ICreateChatConversationVo>(urlBuilder(CREATE_CHAT_CONVERSATION, { baseId }), {
    question,
  });
};
