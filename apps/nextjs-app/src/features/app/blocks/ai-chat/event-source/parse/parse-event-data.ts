import { aiChatMessageContent, basicChatMessageData, textChatMessageData } from '@teable/openapi';

export const parseAIEventData = (data: string) => {
  const parsedData = aiChatMessageContent.safeParse(data);
  if (!parsedData.success) {
    console.error(`Invalid ai event data: ${JSON.stringify(parsedData.error.errors)}`, data);
    throw new Error('Invalid ai event data');
  }
  return parsedData.data;
};

export const parseBasicEventData = (data: unknown) => {
  const parsedData = basicChatMessageData.safeParse(data);
  if (!parsedData.success) {
    console.error(`Invalid basic event data: ${JSON.stringify(parsedData.error.errors)}`, data);
    throw new Error('Invalid basic event data');
  }
  return parsedData.data;
};

export const parseTextEventData = (data: unknown) => {
  const parsedData = textChatMessageData.safeParse(data);
  if (!parsedData.success) {
    console.error(`Invalid text event data: ${JSON.stringify(parsedData.error.errors)}`, data);
    throw new Error('Invalid text event data');
  }
  return parsedData.data;
};
