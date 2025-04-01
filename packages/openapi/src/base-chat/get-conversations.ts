import { axios } from '../axios';
import { registerRoute, urlBuilder } from '../utils';
import { z } from '../zod';

export const GET_CHAT_CONVERSATIONS = '/base/{baseId}/chat/conversations';

export const chatConversationsItemSchema = z.object({
  sessionId: z.string(),
  name: z.string(),
  createdTime: z.string(),
});

export type IChatConversationsItem = z.infer<typeof chatConversationsItemSchema>;

export const getChatConversationsVoSchema = z.object({
  conversations: z.array(chatConversationsItemSchema),
});

export type IGetChatConversationsVo = z.infer<typeof getChatConversationsVoSchema>;

export const getChatConversationsRoute = registerRoute({
  method: 'get',
  path: GET_CHAT_CONVERSATIONS,
  description: 'Get chat conversations',
  request: {
    params: z.object({
      baseId: z.string(),
    }),
  },
  responses: {
    200: {
      description: 'Get chat conversations',
      content: { 'application/json': { schema: getChatConversationsVoSchema } },
    },
  },
  tags: ['base-chat'],
});

export const getChatConversations = async (baseId: string) => {
  return axios.get<IGetChatConversationsVo>(urlBuilder(GET_CHAT_CONVERSATIONS, { baseId }));
};
