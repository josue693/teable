import { ChatMessageRole } from '@teable/openapi';
import type { IAiChatMessageContent } from '@teable/openapi';
import { create } from 'zustand';

export interface IHumanMessage {
  id: string;
  role: ChatMessageRole.Human;
  content: string;
  createdTime: string;
}

export interface IAIMessage {
  id: string;
  role: ChatMessageRole.Assistant;
  content: IAiChatMessageContent[];
  createdTime: string;
  processingIndex: number;
}

export type IMessage = IHumanMessage | IAIMessage;

type MessageStore = {
  messages: IMessage[];
  reset: (messages?: IMessage[]) => void;
  addHumanMessage: (questions: string) => void;
  finishAIMessage: (messageId: string) => void;
  addAIMessageContent: (messageId: string, content: IAiChatMessageContent) => void;
  updateMessageId: (oldId: string, newId: string) => void;
  createEmptyMessage: (messageId: string) => void;
};

export const useMessageStore = create<MessageStore>((set) => ({
  messages: [],
  reset: (messages?: IMessage[]) => set({ messages: messages ?? [] }),
  addHumanMessage: (questions: string) =>
    set((state) => ({
      messages: [
        ...state.messages,
        {
          id: new Date().getTime().toString(),
          content: questions,
          role: ChatMessageRole.Human,
          createdTime: new Date().toISOString(),
        },
      ],
    })),
  addAIMessageContent: (messageId: string, content: IAiChatMessageContent) =>
    set((state) => {
      const message = state.messages.find(
        (m) => m.id === messageId && m.role === ChatMessageRole.Assistant
      ) as IAIMessage | undefined;
      if (!message) {
        return state;
      }
      const lastContent = message.content[message.content.length - 1];
      if (!lastContent) {
        return {
          messages: state.messages.map((m) =>
            m.id === messageId && m.role === ChatMessageRole.Assistant
              ? { ...m, content: [content] }
              : m
          ),
        };
      }
      // if last content and current content is text, then update last content
      if (lastContent.node === content.node) {
        return {
          messages: state.messages.map((m) =>
            m.id === messageId
              ? {
                  ...message,
                  content: [
                    ...message.content.slice(0, -1),
                    {
                      ...lastContent,
                      ...content,
                      text: lastContent.text + content.text,
                    },
                  ],
                }
              : m
          ),
        };
      } else {
        return {
          messages: state.messages.map((m) =>
            m.id === messageId
              ? {
                  ...message,
                  content: [...message.content, content],
                  processingIndex: message.processingIndex + 1,
                  createdTime: new Date().toISOString(),
                }
              : m
          ),
        };
      }
    }),
  finishAIMessage: (messageId: string) =>
    set((state) => ({
      messages: state.messages.map((m) =>
        m.id === messageId && m.role === ChatMessageRole.Assistant
          ? { ...m, processingIndex: Infinity }
          : m
      ),
    })),
  updateMessageId: (oldId: string, newId: string) =>
    set((state) => ({
      messages: state.messages.map((m) => (m.id === oldId ? { ...m, id: newId } : m)),
    })),

  createEmptyMessage: (messageId: string) =>
    set((state) => ({
      messages: [
        ...state.messages,
        {
          id: messageId,
          role: ChatMessageRole.Assistant,
          content: [],
          createdTime: new Date().toISOString(),
          processingIndex: 0,
        },
      ],
    })),
}));
