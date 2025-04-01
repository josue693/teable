import { cn } from '@teable/ui-lib/shadcn';
import { ChatPanelContainer } from './ChatPanelContainer';
import { useChatVisible } from './store/useChatVisible';

export const ChatPanel = () => {
  const { visible, toggleVisible } = useChatVisible();
  return (
    <div
      className={cn(
        'fixed inset-y-0 right-0 max-w-full z-40 border-l shadow-xl transform transition-transform duration-300 ease-in-out bg-background',
        visible ? 'translate-x-0' : 'translate-x-full'
      )}
    >
      <ChatPanelContainer onClose={toggleVisible} />
    </div>
  );
};
