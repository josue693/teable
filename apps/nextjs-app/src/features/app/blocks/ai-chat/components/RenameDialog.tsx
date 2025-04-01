/* eslint-disable jsx-a11y/no-autofocus */
'use client';

import { useMutation, useQueryClient } from '@tanstack/react-query';
import { renameChatConversation } from '@teable/openapi';
import { ReactQueryKeys } from '@teable/sdk/config';
import { Spin } from '@teable/ui-lib/base';
import {
  Button,
  Input,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@teable/ui-lib/shadcn';
import { useTranslation } from 'next-i18next';
import React, { useEffect, useState } from 'react';

interface RenameDialogProps {
  baseId: string;
  sessionId: string;
  open: boolean;
  name: string;
  onClose: () => void;
}

export function RenameDialog({ baseId, sessionId, open, onClose, name }: RenameDialogProps) {
  const [title, setTitle] = useState(name);
  const { t } = useTranslation(['aiChat', 'common']);
  const queryClient = useQueryClient();

  const { mutate: renameChatConversationMutation, isLoading: renameLoading } = useMutation({
    mutationFn: ({ name, sessionId }: { name: string; sessionId: string }) =>
      renameChatConversation(baseId, sessionId, name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ReactQueryKeys.getChatConversations(baseId) });
      onClose();
    },
  });

  useEffect(() => {
    if (open) {
      setTitle(name);
    } else {
      setTitle('');
    }
  }, [open, name]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const newName = title.trim();
    if (newName && newName !== name && !renameLoading) {
      renameChatConversationMutation({ name: newName, sessionId });
    }
  };

  return (
    <Dialog open={open} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle>{t('aiChat:renameDialog.title')}</DialogTitle>
          <DialogDescription>{t('aiChat:renameDialog.description')}</DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit}>
          <div className="grid gap-4 py-4">
            <Input
              id="name"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder={t('aiChat:renameDialog.placeholder')}
              className="col-span-3"
              autoFocus
            />
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose}>
              {t('common:actions.cancel')}
            </Button>
            <Button type="submit" disabled={!title.trim()}>
              {renameLoading && <Spin className="mr-2 size-4" />}
              {t('common:actions.save')}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
