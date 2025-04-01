import { z } from '../zod';

export enum ChatMessageRole {
  Human = 'human',
  Assistant = 'assistant',
}

export enum ChatMessageType {
  AI = 'ai',
  Ping = 'ping',
  Finish = 'finish',
  Error = 'error',
  Basic = 'basic',
}

export enum ChatMessageDataType {
  Text = 'text',
  Sql = 'sql',
  SqlResult = 'sql_result',
  Reasoning = 'reasoning',
}

export enum ChatGraphNode {
  Indent = 'indent',
  FieldSelector = 'fieldSelector',
  SqlGenerator = 'sqlGenerator',
  SqlQuery = 'sqlQuery',
  SqlFixer = 'sqlFixer',
  SqlResponse = 'sqlResponse',
  NormalResponse = 'normalResponse',
}

export const textChatMessageData = z.object({
  type: z.literal(ChatMessageDataType.Text),
  text: z.string(),
  node: z.nativeEnum(ChatGraphNode),
});

export const sqlChatMessageData = z.object({
  type: z.literal(ChatMessageDataType.Sql),
  text: z.string(),
  node: z.nativeEnum(ChatGraphNode),
  message: z.string().optional(),
});

export const sqlResultChatMessageData = z.object({
  type: z.literal(ChatMessageDataType.SqlResult),
  node: z.literal(ChatGraphNode.SqlQuery),
  text: z.string(),
  error: z.string().optional(),
});

export type ISqlResultChatMessageData = z.infer<typeof sqlResultChatMessageData>;

export const reasoningChatMessageData = z.object({
  type: z.literal(ChatMessageDataType.Reasoning),
  node: z.nativeEnum(ChatGraphNode),
  text: z.string(),
});

export type IReasoningChatMessageData = z.infer<typeof reasoningChatMessageData>;

export const nullChatMessageData = z.null().optional();

export const aiChatMessageContent = z.discriminatedUnion('type', [
  textChatMessageData,
  sqlChatMessageData,
  sqlResultChatMessageData,
  reasoningChatMessageData,
]);

export type IAiChatMessageContent = z.infer<typeof aiChatMessageContent>;

export const aiChatEventMessage = z.object({
  data: aiChatMessageContent,
  event: z.literal(ChatMessageType.AI),
});

export const pingChatEventMessage = z.object({
  data: nullChatMessageData,
  event: z.literal(ChatMessageType.Ping),
});

export const finishChatEventMessage = z.object({
  data: nullChatMessageData,
  event: z.literal(ChatMessageType.Finish),
});

export const errorChatEventMessage = z.object({
  data: textChatMessageData,
  event: z.literal(ChatMessageType.Error),
});

export const basicChatMessageData = z.object({
  messageId: z.string(),
});

export type IBasicChatMessageData = z.infer<typeof basicChatMessageData>;

export const basicChatEventMessage = z.object({
  data: basicChatMessageData,
  event: z.literal(ChatMessageType.Basic),
});

export type IBasicChatEventMessage = z.infer<typeof basicChatEventMessage>;

export type IErrorChatEventMessage = z.infer<typeof errorChatEventMessage>;

export const chatEventMessage = z.discriminatedUnion('event', [
  aiChatEventMessage,
  pingChatEventMessage,
  finishChatEventMessage,
  errorChatEventMessage,
  basicChatEventMessage,
]);

export type IChatEventMessage = z.infer<typeof chatEventMessage>;
