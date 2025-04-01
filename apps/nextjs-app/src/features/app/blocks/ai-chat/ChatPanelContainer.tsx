/* eslint-disable jsx-a11y/no-static-element-interactions */
/* eslint-disable jsx-a11y/click-events-have-key-events */
'use client';

import { useQuery } from '@tanstack/react-query';
import { ChatMessageRole, getChatConversation } from '@teable/openapi';
import { ReactQueryKeys } from '@teable/sdk/config';
import { useBaseId } from '@teable/sdk/hooks';
import { Button, cn } from '@teable/ui-lib/shadcn';
import { X, Bot, PanelLeftClose, PanelLeft } from 'lucide-react';
import { useTranslation } from 'next-i18next';
import { useEffect, useState } from 'react';
import { ChatContainer } from './ChatContainer';
import { ChatHistory } from './ChatHistory';
import { useChatVisible } from './store/useChatVisible';
import type { IMessage } from './store/useMessage';
import { useMessageStore } from './store/useMessage';

interface ChatPanelProps {
  onClose: () => void;
}

export const ChatPanelContainer = ({ onClose }: ChatPanelProps) => {
  const { t } = useTranslation(['ai-chat']);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const baseId = useBaseId()!;
  const { visible } = useChatVisible();
  const [showHistory, setShowHistory] = useState<boolean>();

  const { data: chatConversation, refetch } = useQuery({
    enabled: Boolean(sessionId),
    queryKey: ReactQueryKeys.getChatConversation(baseId, sessionId!),
    queryFn: ({ queryKey }) =>
      getChatConversation(queryKey[1], queryKey[2]).then((res) => res.data),
    staleTime: Infinity,
  });

  useEffect(() => {
    if (sessionId) {
      refetch();
    }
  }, [refetch, sessionId]);

  const initMessages = chatConversation?.messages;
  useEffect(() => {
    const reset = useMessageStore.getState().reset;
    if (initMessages) {
      const messages: IMessage[] = [];
      initMessages.forEach((message) => {
        if (message.role === ChatMessageRole.Human) {
          messages.push({
            id: message.messageId,
            role: message.role,
            content: message.data,
            createdTime: message.createdTime,
          });
        } else {
          messages.push({
            id: message.messageId,
            role: message.role,
            content: message.data,
            createdTime: message.createdTime,
            processingIndex: Infinity,
          });
        }
      });
      reset(messages);
    } else {
      reset();
    }
  }, [initMessages]);

  return (
    <div className="relative flex h-full">
      <div
        className={cn(
          'absolute top-0 left-0 bottom-0 w-[300px] bg-background transition-transform duration-300 ease-in-out border',
          {
            '-translate-x-full': showHistory && visible,
          }
        )}
      >
        <ChatHistory sessionId={sessionId} baseId={baseId} onSelectChat={setSessionId} />
      </div>
      <div className="relative flex h-full w-[600px] max-w-full flex-1 flex-col bg-background">
        <div className="flex h-14 items-center justify-between border-b px-2">
          <div className="flex items-center space-x-2">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setShowHistory((prev) => !prev)}
              className="mr-1 size-8 sm:mr-2"
              aria-label={showHistory ? 'hide sidebar' : 'show sidebar'}
            >
              {showHistory ? (
                <PanelLeftClose className="size-4" />
              ) : (
                <PanelLeft className="size-4" />
              )}
            </Button>
            <Bot className="size-5 text-primary" />
            <h2 className="text-base font-medium md:text-lg">{t('ai-chat:chat.name')}</h2>
          </div>
          <Button
            variant="ghost"
            size="icon"
            onClick={onClose}
            className="size-8 rounded-full hover:bg-muted"
            aria-label="close chat"
          >
            <X className="size-4" />
          </Button>
        </div>

        {sessionId && <ChatContainer baseId={baseId} sessionId={sessionId} />}
      </div>
    </div>
  );
};
