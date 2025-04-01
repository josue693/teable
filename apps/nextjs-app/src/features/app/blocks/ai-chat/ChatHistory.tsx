/* eslint-disable jsx-a11y/no-autofocus */
/* eslint-disable jsx-a11y/no-static-element-interactions */
/* eslint-disable jsx-a11y/click-events-have-key-events */
'use client';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { IChatConversationsItem } from '@teable/openapi';
import {
  createChatConversation,
  deleteChatConversation,
  getChatConversations,
} from '@teable/openapi';
import { ReactQueryKeys } from '@teable/sdk/config';
import { useLanDayjs } from '@teable/sdk/hooks';
import { ConfirmDialog, Spin } from '@teable/ui-lib/base';
import {
  Button,
  cn,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  ScrollArea,
} from '@teable/ui-lib/shadcn';
import { Edit2, Trash2, MessageSquarePlus, MoreHorizontal } from 'lucide-react';
import { useTranslation } from 'next-i18next';
import React, { useState } from 'react';
import { RenameDialog } from './components/RenameDialog';

interface ChatHistoryProps {
  baseId: string;
  sessionId: string | null;
  onSelectChat: (sessionId: string) => void;
}

export function ChatHistory({ baseId, sessionId, onSelectChat }: ChatHistoryProps) {
  const [openRenameDialog, setOpenRenameDialog] = useState(false);
  const [openDeleteDialog, setOpenDeleteDialog] = useState(false);
  const [dialogContent, setDialogContent] = useState<IChatConversationsItem | null>(null);
  const dayjs = useLanDayjs();
  const queryClient = useQueryClient();
  const { t } = useTranslation(['aiChat', 'common']);

  const { data: chatMessages } = useQuery({
    queryKey: ReactQueryKeys.getChatConversations(baseId),
    queryFn: ({ queryKey }) => getChatConversations(queryKey[1]).then((res) => res.data),
  });

  const chatHistory = chatMessages?.conversations;

  const { mutate: deleteChatMessageMutation, isLoading: deleteLoading } = useMutation({
    mutationFn: (sessionId: string) => deleteChatConversation(baseId, sessionId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ReactQueryKeys.getChatConversations(baseId) });
    },
  });

  const { mutate: createChatConversationMutation, isLoading: createLoading } = useMutation({
    mutationFn: () => createChatConversation(baseId),
    onSuccess: (res) => {
      queryClient.invalidateQueries({ queryKey: ReactQueryKeys.getChatConversations(baseId) });
      onSelectChat(res.data.sessionId);
    },
  });

  return (
    <div className="flex h-full flex-col">
      <div className="flex h-14 items-center border-b px-3">
        <Button
          onClick={() => !createLoading && createChatConversationMutation()}
          variant="outline"
          className="flex w-full items-center justify-center gap-2"
        >
          {createLoading ? <Spin className="size-4" /> : <MessageSquarePlus className="size-4" />}
          <span>{t('aiChat:chat.newChat')}</span>
        </Button>
      </div>

      <ScrollArea className="flex-1">
        <div className="space-y-1.5 p-1 sm:p-2">
          {chatHistory?.length === 0 && (
            <div className="py-6 text-center text-sm text-muted-foreground sm:py-8">
              {t('aiChat:chat.noChat')}
            </div>
          )}
          {chatHistory?.map((chat) => (
            <div
              key={chat.sessionId}
              className={cn(
                'group flex flex-col p-3 rounded-lg cursor-pointer transition-all duration-200 border hover:bg-muted/30 mb-2 bg-background border-border hover:border-primary/20',
                {
                  'bg-background border-primary/30 shadow-sm': sessionId === chat.sessionId,
                }
              )}
              onClick={() => onSelectChat(chat.sessionId)}
            >
              <div className="flex items-start justify-between">
                <h3 className="line-clamp-1 text-sm ">{chat.name || '未命名'}</h3>
              </div>
              <div className="mt-3 flex items-center justify-between">
                <span className="text-xs text-muted-foreground">
                  {dayjs(chat.createdTime).fromNow()}
                </span>
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button variant="ghost" size="sm" className="-mr-1 -mt-1 rounded-full">
                      <MoreHorizontal className="size-4" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem
                      onClick={(e) => {
                        e.stopPropagation();
                        setDialogContent(chat);
                        setOpenRenameDialog(true);
                      }}
                    >
                      <Edit2 className="mr-2 size-4" />
                      {t('common:actions.rename')}
                    </DropdownMenuItem>
                    <DropdownMenuItem
                      className="text-destructive focus:text-destructive"
                      onClick={(e) => {
                        e.stopPropagation();
                        setDialogContent(chat);
                        setOpenDeleteDialog(true);
                      }}
                    >
                      <Trash2 className="mr-2 size-4" />
                      {t('common:actions.delete')}
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </div>
            </div>
          ))}
        </div>
      </ScrollArea>
      <RenameDialog
        baseId={baseId}
        sessionId={dialogContent?.sessionId || ''}
        open={openRenameDialog}
        onClose={() => {
          setOpenRenameDialog(false);
          setDialogContent(null);
        }}
        name={dialogContent?.name || ''}
      />
      <ConfirmDialog
        open={openDeleteDialog}
        closeable={true}
        onOpenChange={(val) => {
          if (!val) {
            setDialogContent(null);
          }
        }}
        title={t('aiChat:deleteDialog.title')}
        description={t('aiChat:deleteDialog.description')}
        onCancel={() => {
          setDialogContent(null);
          setOpenDeleteDialog(false);
        }}
        cancelText={t('common:actions.cancel')}
        confirmText={t('common:actions.confirm')}
        confirmLoading={deleteLoading}
        onConfirm={() => {
          if (dialogContent) {
            deleteChatMessageMutation(dialogContent.sessionId);
          }
        }}
      />
    </div>
  );
}
