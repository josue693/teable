import { ChatMessageRole } from '@teable/openapi';
import { Button, Input } from '@teable/ui-lib/shadcn';
import { Send } from 'lucide-react';
import { useTranslation } from 'next-i18next';
import { useEffect, useRef } from 'react';
import { AIMessage } from './components/AIMessage';
import { HumanMessage } from './components/HumanMessage';
import { useFetchMessage } from './event-source/useFetchMessage';

export const ChatContainer = ({ baseId, sessionId }: { baseId: string; sessionId: string }) => {
  const {
    messages,
    handleSubmit,
    input,
    setInput,
    loading: isLoading,
  } = useFetchMessage({ baseId, sessionId });
  const { t } = useTranslation(['ai-chat']);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // detect new message and scroll to bottom
  useEffect(() => {
    if (isLoading) {
      scrollToBottom();
    }
  }, [messages, isLoading]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  return (
    <>
      <div className="flex-1 overflow-y-auto overflow-x-hidden px-5 py-3">
        <div className="max-w-full space-y-6 pb-4">
          {messages.map((message) => (
            <div key={message.id} className="group w-full">
              {/* human message */}
              {message.role === ChatMessageRole.Human && <HumanMessage message={message} />}
              {/* ai message */}
              {message.role === ChatMessageRole.Assistant && <AIMessage message={message} />}
            </div>
          ))}
          <div ref={messagesEndRef} />
        </div>
      </div>

      <div className="border-t bg-background/95 p-4 backdrop-blur supports-[backdrop-filter]:bg-background/60">
        <form onSubmit={handleSubmit} className="flex items-center space-x-2">
          <Input
            value={input}
            onChange={(e) => {
              setInput(e.target.value);
            }}
            placeholder={t('ai-chat:chat.placeholder')}
            className="flex-1 rounded-full border-muted-foreground/20 shadow-sm focus-visible:ring-primary/50"
            disabled={isLoading}
          />
          <Button
            type="submit"
            size="icon"
            className="size-10 rounded-full shadow-sm"
            disabled={!input.trim() || isLoading}
            aria-label={t('ai-chat:chat.send')}
          >
            <Send className="size-4" />
          </Button>
        </form>
      </div>
    </>
  );
};
