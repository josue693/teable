import { User } from '@teable/icons';
import type { IHumanMessage } from '../store/useMessage';

export const HumanMessage = ({ message }: { message: IHumanMessage }) => {
  return (
    <div className="ml-auto flex max-w-[85%] items-start gap-3">
      <div className="flex-1 overflow-hidden">
        <div className="rounded-2xl bg-primary px-4 py-2 text-primary-foreground shadow-sm">
          <p className="whitespace-pre-wrap break-words text-sm leading-relaxed">
            {message.content}
          </p>
        </div>
        <p className="mt-1 pr-1 text-right text-xs text-muted-foreground">
          {new Date(message.createdTime).toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
          })}
        </p>
      </div>
      <div className="flex size-8 shrink-0 items-center justify-center rounded-full bg-primary/10">
        <User className="size-4 text-primary" />
      </div>
    </div>
  );
};
