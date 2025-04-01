import type { AIMessageChunk } from '@langchain/core/messages';
import type { IterableReadableStream } from '@langchain/core/utils/stream';

export const stream2result = async (
  stream: IterableReadableStream<AIMessageChunk>,
  onStreamChunk?: (chunk: AIMessageChunk) => void
) => {
  let result = '';
  const reader = stream.getReader();
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    result += value.content;
    onStreamChunk?.(value);
  }
  return result;
};
