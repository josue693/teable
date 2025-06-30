import { useChat } from '@ai-sdk/react';
import type { UseChatOptions, UseChatHelpers } from '@ai-sdk/react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { generateChatId } from '@teable/core';
import { getAIConfig, getChatMessages } from '@teable/openapi';
import { ReactQueryKeys } from '@teable/sdk/config';
import { toast } from '@teable/ui-lib/shadcn/ui/sonner';
import { useRouter } from 'next/router';
import { forwardRef, useEffect, useImperativeHandle, useMemo, useRef } from 'react';
import { generateModelKeyList } from '@/features/app/blocks/admin/setting/components/ai-config/utils';
import { MessageInput } from '../components/MessageInput';
import { Messages } from '../components/Messages';
import type { IMessageMeta } from '../components/types';
import { ChatContext } from '../context/ChatContext';
import { useChatContext } from '../context/useChatContext';
import { useActiveChat } from '../hooks/useActiveChat';
import { useChatStore } from '../store/useChatStore';

export interface ChatContainerRef {
  setInputValue: (value: string) => void;
}

export const ChatContainer = forwardRef<
  ChatContainerRef,
  {
    baseId: string;
    autoOpen?: boolean;
    onToolCall?: (
      toolCall: Parameters<Required<UseChatOptions>['onToolCall']>[0] & { chatId: string }
    ) => void;
  }
>(({ baseId, autoOpen = false, onToolCall }, ref) => {
  const chatIdRef = useRef(generateChatId());
  const tableIdRef = useRef<string | undefined>();
  const { modelKey } = useChatStore();
  const activeChat = useActiveChat(baseId);
  const queryClient = useQueryClient();
  const chatContext = useChatContext();
  const { context, setActiveChatId } = chatContext;
  const isActiveChat = Boolean(activeChat);
  const chatId = isActiveChat ? activeChat!.id : chatIdRef.current;
  const router = useRouter();
  const tableId = router.query.tableId as string | undefined;
  const { setMessages: setStoreMessages } = useChatStore();

  useEffect(() => {
    tableIdRef.current = tableId;
  }, [tableId]);

  const { data: baseAiConfig, isLoading: isBaseAiConfigLoading } = useQuery({
    queryKey: ['ai-config', baseId],
    queryFn: () => getAIConfig(baseId).then(({ data }) => data),
  });

  const { data: chatMessage } = useQuery({
    queryKey: ['chat-message', chatId],
    queryFn: ({ queryKey }) => getChatMessages(baseId, queryKey[1]).then((res) => res.data),
    enabled: isActiveChat,
  });

  const messageMetaMap = useMemo(() => {
    return chatMessage?.messages?.reduce(
      (acc, message) => {
        acc[message.id] = {
          timeCost: message.timeCost,
          usage: message.usage,
        };
        return acc;
      },
      {} as Record<string, IMessageMeta>
    );
  }, [chatMessage]);

  const convertToUIMessages = useMemo<UseChatHelpers['messages']>(() => {
    if (!isActiveChat) {
      return [];
    }
    return (
      chatMessage?.messages?.map((message) => ({
        id: message.id,
        role: message.role as UseChatHelpers['messages'][number]['role'],
        parts: message.parts as UseChatHelpers['messages'][number]['parts'],
        content: '',
        createdAt: new Date(message.createdTime),
      })) ?? []
    );
  }, [isActiveChat, chatMessage]);

  const { llmProviders = [], codingModel } = baseAiConfig ?? {};
  const models = useMemo(() => {
    return generateModelKeyList(llmProviders);
  }, [llmProviders]);

  const validModelKey = useMemo(() => {
    return (
      models.find((model) => model.modelKey === modelKey)?.modelKey ||
      codingModel ||
      models[0]?.modelKey
    );
  }, [modelKey, models, codingModel]);

  const chatState = useChat({
    id: chatId,
    api: `/api/base/${baseId}/chat`,
    initialMessages: convertToUIMessages,
    body: {
      chatId,
      model: validModelKey,
      context,
    },
    onFinish: () => {
      if (isActiveChat) {
        queryClient.invalidateQueries({ queryKey: ['chat-message', chatId] });
        return;
      }
      queryClient.refetchQueries({ queryKey: ReactQueryKeys.chatHistory(baseId) }).then(() => {
        setActiveChatId(chatId);
        chatIdRef.current = generateChatId();
      });
    },
    onToolCall: ({ toolCall }) => {
      onToolCall?.({ toolCall, chatId });
    },
    onError: (error) => {
      toast.error(error.message);
    },
  });

  const { messages, setMessages, handleSubmit, input, setInput, status, stop, addToolResult } =
    chatState;

  useEffect(() => {
    setStoreMessages(messages);
  }, [messages, setStoreMessages]);

  useImperativeHandle(ref, () => ({
    setInputValue: (value: string) => {
      setInput(value);
    },
  }));

  return (
    <ChatContext.Provider
      value={{
        ...chatContext,
        addToolResult,
      }}
    >
      <div className="flex w-full flex-1 flex-col overflow-hidden pb-3">
        {(messages.length > 0 || !autoOpen) && (
          <Messages
            messages={messages}
            messageMetaMap={messageMetaMap}
            chatId={chatId}
            status={status}
          />
        )}
        <MessageInput
          modelKey={validModelKey}
          models={models}
          input={input}
          setInput={setInput}
          status={status}
          stop={stop}
          setMessages={setMessages}
          handleSubmit={handleSubmit}
          modelLoading={isBaseAiConfigLoading}
        />
      </div>
    </ChatContext.Provider>
  );
});

ChatContainer.displayName = 'ChatContainer';
