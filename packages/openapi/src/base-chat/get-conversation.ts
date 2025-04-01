import { axios } from '../axios';
import { registerRoute, urlBuilder } from '../utils';
import { z } from '../zod';
import { aiChatMessageContent, ChatMessageRole } from './types';

export const GET_CHAT_CONVERSATION = '/base/{baseId}/chat/conversation/{sessionId}';

const baseChatMessageContentSchema = z.object({
  messageId: z.string(),
  sessionId: z.string(),
  userId: z.string(),
  createdTime: z.string(),
});

const aiChatMessageContentSchema = baseChatMessageContentSchema.extend({
  data: z.array(aiChatMessageContent),
  role: z.literal(ChatMessageRole.Assistant),
});

const humanChatMessageContentSchema = baseChatMessageContentSchema.extend({
  data: z.string(),
  role: z.literal(ChatMessageRole.Human),
});

export const chatMessageItemSchema = z.discriminatedUnion('role', [
  aiChatMessageContentSchema,
  humanChatMessageContentSchema,
]);

export type IChatConversationItem = z.infer<typeof chatMessageItemSchema>;

export const getChatConversationVoSchema = z.object({
  messages: z.array(chatMessageItemSchema),
});

export type IGetChatConversationVo = z.infer<typeof getChatConversationVoSchema>;

export const getChatConversationRoute = registerRoute({
  method: 'get',
  path: GET_CHAT_CONVERSATION,
  description: 'Get chat conversation',
  request: {
    params: z.object({
      baseId: z.string(),
      sessionId: z.string(),
    }),
  },
  responses: {
    200: {
      description: 'Get chat conversation',
      content: { 'application/json': { schema: getChatConversationVoSchema } },
    },
  },
  tags: ['base-chat'],
});

export const getChatConversation = async (baseId: string, sessionId: string) => {
  return axios.get<IGetChatConversationVo>(
    urlBuilder(GET_CHAT_CONVERSATION, { baseId, sessionId })
  );
};
