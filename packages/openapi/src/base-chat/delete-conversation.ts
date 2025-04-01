import { axios } from '../axios';
import { registerRoute, urlBuilder } from '../utils';
import { z } from '../zod';

export const DELETE_CHAT_CONVERSATION = '/base/{baseId}/chat/conversation/{sessionId}';

export const deleteChatConversationRoute = registerRoute({
  method: 'delete',
  path: DELETE_CHAT_CONVERSATION,
  description: 'Delete chat conversation',
  request: {
    params: z.object({
      baseId: z.string(),
      sessionId: z.string(),
    }),
  },
  responses: {
    200: {
      description: 'Delete chat conversation',
    },
  },
  tags: ['base-chat'],
});

export const deleteChatConversation = async (baseId: string, sessionId: string) => {
  return axios.delete<void>(urlBuilder(DELETE_CHAT_CONVERSATION, { baseId, sessionId }));
};
