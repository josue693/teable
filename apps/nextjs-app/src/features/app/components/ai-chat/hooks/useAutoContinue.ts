import type { UseChatHelpers } from '@ai-sdk/react';
import { useEffect, useRef } from 'react';

interface IUseAutoContinueProps {
  status: UseChatHelpers['status'];
  messages: UseChatHelpers['messages'];
  handleSubmit: (e: React.FormEvent<HTMLFormElement>) => void;
  setInput: (value: string) => void;
}

export const useAutoContinue = ({
  status,
  messages,
  handleSubmit,
  setInput,
}: IUseAutoContinueProps) => {
  const continueCountRef = useRef(0);

  useEffect(() => {
    if (status !== 'ready' || messages.length === 0) {
      return;
    }

    const lastMessage = messages[messages.length - 1];
    if (lastMessage.role === 'assistant' && !lastMessage.content.trim().endsWith('EOF')) {
      if (continueCountRef.current >= 3) {
        return;
      }

      setInput('continue');
      continueCountRef.current += 1;

      const fakeEvent = new Event('submit', {
        cancelable: true,
      }) as unknown as React.FormEvent<HTMLFormElement>;
      handleSubmit(fakeEvent);
    } else {
      continueCountRef.current = 0;
    }
  }, [status, messages, handleSubmit, setInput]);

  return {
    continueCount: continueCountRef.current,
  };
};
