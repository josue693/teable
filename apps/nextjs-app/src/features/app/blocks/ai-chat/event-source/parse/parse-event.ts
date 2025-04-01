import { chatEventMessage } from '@teable/openapi';

export const parseEvent = (data: unknown) => {
  const parsedData = chatEventMessage.safeParse(data);
  if (!parsedData.success) {
    console.error(`Invalid event data: ${JSON.stringify(parsedData.error.errors)}`, data);
    throw new Error(`Invalid event data`);
  }

  return parsedData.data;
};
