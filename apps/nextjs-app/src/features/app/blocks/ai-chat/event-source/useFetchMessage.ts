import { CHAT_COMPLETIONS, ChatMessageType, urlBuilder } from '@teable/openapi';
import { useTableId, useViewId } from '@teable/sdk/hooks';
import { useCallback, useEffect, useRef, useState } from 'react';
import { useMessageStore } from '../store/useMessage';
import { fetchEventSource } from './fetch-event-source';
import {
  parseAIEventData,
  parseBasicEventData,
  parseTextEventData,
} from './parse/parse-event-data';

export const useFetchMessage = ({ baseId, sessionId }: { baseId: string; sessionId: string }) => {
  const abortControllerRef = useRef<AbortController | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const { messages } = useMessageStore();
  const [inputInner, setInputInner] = useState('');
  const inputRef = useRef(inputInner);
  const tableId = useTableId();
  const viewId = useViewId();
  const tableQueryRef = useRef({ tableId, viewId });

  useEffect(() => {
    tableQueryRef.current = { tableId, viewId };
  }, [tableId, viewId]);

  const setInput = useCallback((input: string) => {
    setInputInner(input);
    inputRef.current = input;
  }, []);

  const cleanup = useCallback(() => {
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
    setLoading(false);
    setInput('');
    setError(null);
  }, [setInput]);

  const handleSubmit = useCallback(
    async (event?: { preventDefault?: () => void }) => {
      event?.preventDefault?.();
      const questions = inputRef.current.trim();
      if (!questions) return;
      cleanup();
      abortControllerRef.current = new AbortController();
      setLoading(true);
      useMessageStore.getState().addHumanMessage(questions);
      let messageId = 'ai-' + new Date().getTime().toString();
      useMessageStore.getState().createEmptyMessage(messageId);
      await fetchEventSource(urlBuilder(`/api${CHAT_COMPLETIONS}`, { baseId, sessionId }), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          question: questions,
          tableQuery: tableQueryRef.current,
        }),
        signal: abortControllerRef.current?.signal,
        onmessage({ event, data }) {
          // if the server emits an error message, throw an exception
          // so it gets handled by the onerror callback below:
          switch (event) {
            case ChatMessageType.AI:
              useMessageStore.getState().addAIMessageContent(messageId, parseAIEventData(data));
              break;
            case ChatMessageType.Basic:
              {
                const newMessageId = parseBasicEventData(data).messageId;
                useMessageStore.getState().updateMessageId(messageId, newMessageId);
                messageId = newMessageId;
              }
              break;
            case ChatMessageType.Finish:
              setLoading(false);
              useMessageStore.getState().finishAIMessage(messageId);
              break;
            case ChatMessageType.Error:
              setError(parseTextEventData(data).text);
              setLoading(false);
              break;
            case ChatMessageType.Ping:
              console.log('Ping');
              break;
            default:
              console.log('Unknown event', event);
          }
        },
        onclose() {
          setLoading(false);
        },
        onerror(err) {
          setError(err.message);
        },
      });
    },
    [baseId, cleanup, sessionId]
  );

  return {
    messages,
    handleSubmit,
    input: inputInner,
    setInput,
    error,
    loading,
  };
};
