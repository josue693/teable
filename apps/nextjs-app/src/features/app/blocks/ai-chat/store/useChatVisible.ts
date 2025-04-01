import { create } from 'zustand';

export const useChatVisible = create<{
  visible: boolean;
  toggleVisible: () => void;
}>((set) => ({
  visible: false,
  toggleVisible: () => set((state) => ({ visible: !state.visible })),
}));
