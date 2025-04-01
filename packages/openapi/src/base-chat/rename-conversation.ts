import { axios } from '../axios';
import { registerRoute, urlBuilder } from '../utils';
import { z } from '../zod';

export const RENAME_CHAT_CONVERSATION = '/base/{baseId}/chat/conversation/{sessionId}';

export const renameChatConversationRoSchema = z.object({
  name: z.string(),
});

export type IRenameChatConversationRo = z.infer<typeof renameChatConversationRoSchema>;

export const renameChatConversationRoute = registerRoute({
  method: 'put',
  path: RENAME_CHAT_CONVERSATION,
  description: 'Rename chat conversation',
  request: {
    params: z.object({
      baseId: z.string(),
      sessionId: z.string(),
    }),
    body: {
      content: { 'application/json': { schema: renameChatConversationRoSchema } },
    },
  },
  responses: {
    200: {
      description: 'Rename chat conversation',
    },
  },
  tags: ['base-chat'],
});

export const renameChatConversation = async (baseId: string, sessionId: string, name: string) => {
  return axios.put<void>(urlBuilder(RENAME_CHAT_CONVERSATION, { baseId, sessionId }), { name });
};
