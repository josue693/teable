/* eslint-disable sonarjs/cognitive-complexity */
/* eslint-disable no-constant-condition */
interface FetchEventSourceInit extends RequestInit {
  onmessage?: (event: { data: string; event?: string }) => void;
  onopen?: () => void;
  onerror?: (error: Error) => void;
  onclose?: () => void;
}

export const fetchEventSource = async (
  url: string,
  { onmessage, onopen, onerror, onclose, ...requestInit }: FetchEventSourceInit
) => {
  const abortController = new AbortController();

  let reader: ReadableStreamReader<Uint8Array> | null = null;
  try {
    const response = await fetch(url, {
      ...requestInit,
      headers: {
        ...requestInit.headers,
        Accept: 'text/event-stream',
      },
      signal: abortController.signal,
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    if (!response.body) {
      throw new Error('Response body is null');
    }

    onopen?.();

    reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { value, done } = await reader.read();

      if (done) {
        onclose?.();
        break;
      }

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');

      buffer = lines.pop() || '';

      let currentEvent = '';
      for (const line of lines) {
        if (line.startsWith('event: ')) {
          currentEvent = line.slice(7);
        } else if (line.startsWith('data: ')) {
          const data = line.slice(6);
          try {
            const parsedData = JSON.parse(data);
            onmessage?.({ data: parsedData, event: currentEvent });
          } catch (e) {
            onmessage?.({ data, event: currentEvent });
          }
        }
      }
    }
  } catch (error) {
    reader?.releaseLock();
    onerror?.(error as Error);
  } finally {
    abortController.abort();
    onclose?.();
  }

  return () => {
    abortController.abort();
  };
};
