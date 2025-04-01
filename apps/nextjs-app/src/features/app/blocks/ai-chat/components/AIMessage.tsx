import { Bot } from 'lucide-react';
import type { IAIMessage } from '../store/useMessage';
import { BotLoading } from './BotLoading';
import { MessageSteps } from './MessageSteps';

export const AIMessage = ({ message }: { message: IAIMessage }) => {
  return (
    <div className="flex max-w-[90%] items-start gap-3">
      <div className="flex size-8 shrink-0 items-center justify-center rounded-full bg-muted">
        <Bot className="size-4 text-primary" />
      </div>
      <div className="flex-1 overflow-hidden">
        <MessageSteps message={message} />
        {message.processingIndex !== Infinity && <BotLoading />}
        <p className="mt-1 pl-1 text-xs text-muted-foreground">
          {new Date(message.createdTime).toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
          })}
        </p>
      </div>
    </div>
  );
};
