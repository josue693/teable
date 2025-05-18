import { useQuery } from '@tanstack/react-query';
import { getTemplateList } from '@teable/openapi';
import { ReactQueryKeys } from '@teable/sdk/config';
import { useRouter } from 'next/router';
import { useTranslation } from 'next-i18next';
import { useRef } from 'react';
import { useChatContext } from '@/features/app/components/ai-chat/context/useChatContext';
import {
  ChatContainer,
  type ChatContainerRef,
} from '@/features/app/components/ai-chat/panel/ChatContainer';
import { useChatPanelStore } from '@/features/app/components/ai-chat/store/useChatPanelStore';
import { TemplateModal } from '@/features/app/components/space/template';
import { TemplateContext } from '@/features/app/components/space/template/context';
import { useSpaceId } from '@/features/app/components/space/template/hooks/use-space-id';
import { tableConfig } from '@/features/i18n/table.config';

const ActionButton = ({
  emoji,
  text,
  prompt,
  onClick,
}: {
  emoji: string;
  text: string;
  prompt: string;
  onClick: (prompt: string) => void;
}) => (
  <button
    className="flex items-center gap-2 rounded-xl border border-gray-200 px-2 text-xs text-gray-600 hover:bg-gray-50"
    onClick={() => onClick(prompt)}
  >
    <span className="text-lg">{emoji}</span>
    <span>{text}</span>
  </button>
);

const TemplatesCard = ({
  title,
  useCount,
  initial,
  bgColor = '#4F46E5',
  preview,
}: {
  title: string;
  useCount: number;
  initial: string;
  bgColor?: string;
  preview?: string;
  description?: string;
}) => {
  const { t } = useTranslation(tableConfig.i18nNamespaces);
  return (
    <div className="flex flex-col gap-2">
      <div
        className="aspect-video w-full overflow-hidden rounded-lg border border-gray-200 bg-gray-100"
        style={
          preview
            ? {
                backgroundImage: `url(${preview})`,
                backgroundSize: 'cover',
                backgroundPosition: 'center',
              }
            : {}
        }
      ></div>
      <div className="flex gap-2">
        <div
          className="flex size-6 items-center justify-center rounded-full text-xs text-white"
          style={{ backgroundColor: bgColor }}
        >
          {initial}
        </div>
        <div className="flex-1">
          <h3 className="font-medium">{title}</h3>
          <p className="text-sm text-gray-500">
            {useCount} {t('common:settings.templateAdmin.usageCount', { count: useCount })}
          </p>
        </div>
      </div>
    </div>
  );
};

export const ChatPage = () => {
  const { t } = useTranslation(tableConfig.i18nNamespaces);
  const baseId = useRouter().query.baseId as string;
  const spaceId = useSpaceId() as string;
  const chatContainerRef = useRef<ChatContainerRef>(null);
  const { open: openChatPanel } = useChatPanelStore();
  const { setActiveChatId } = useChatContext();
  const { data: templateData } = useQuery({
    queryKey: ReactQueryKeys.templateList(),
    queryFn: () => getTemplateList().then((data) => data.data),
  });

  const TEMPLATE_PROMPTS = [
    {
      emoji: '📈',
      title: t('table:prompt.crm.title'),
      prompt: t('table:prompt.crm.description'),
    },
    {
      emoji: '📋',
      title: t('table:prompt.projectManagement.title'),
      prompt: t('table:prompt.projectManagement.description'),
    },
    {
      emoji: '📣',
      title: t('table:prompt.marketingCampaign.title'),
      prompt: t('table:prompt.marketingCampaign.description'),
    },
    {
      emoji: '🤝',
      title: t('table:prompt.teamCollaboration.title'),
      prompt: t('table:prompt.teamCollaboration.description'),
    },
  ];

  const getRandomColor = () => {
    const colors = ['#6366F1', '#EC4899', '#8B5CF6', '#10B981', '#F59E0B', '#3B82F6'];
    return colors[Math.floor(Math.random() * colors.length)];
  };

  const handlePromptClick = (prompt: string) => {
    chatContainerRef.current?.setInputValue(prompt);
  };

  return (
    <div className="flex h-full flex-col overflow-auto">
      <div className="flex flex-col items-center px-4 py-8 pt-32 lg:pt-48">
        <h1 className="mb-8 text-5xl font-semibold">{t('common:template.aiTitle')}</h1>

        <div className="mb-2 w-full max-w-3xl">
          <ChatContainer
            ref={chatContainerRef}
            baseId={baseId}
            autoOpen
            onToolCall={({ chatId }) => {
              setActiveChatId(chatId);
              openChatPanel();
            }}
          />
        </div>

        <div className="mb-12 flex flex-wrap justify-center gap-4">
          {TEMPLATE_PROMPTS.map((prompt) => (
            <ActionButton
              key={prompt.title}
              emoji={prompt.emoji}
              text={prompt.title}
              prompt={prompt.prompt}
              onClick={handlePromptClick}
            />
          ))}
        </div>

        <div className="w-full max-w-6xl">
          <div className="mb-6 flex items-center justify-between">
            <div>
              <h2 className="text-xl font-semibold">{t('common:template.title')}</h2>
              <p className="text-gray-500">{t('common:template.description')}</p>
            </div>
            <TemplateContext.Provider value={{ spaceId }}>
              <TemplateModal spaceId={spaceId}>
                <button className="text-sm text-gray-600 hover:text-gray-900">
                  {t('common:template.browseAll')} →
                </button>
              </TemplateModal>
            </TemplateContext.Provider>
          </div>

          <div className="grid grid-cols-1 gap-8 sm:grid-cols-2 lg:grid-cols-3">
            {templateData?.map((template) => (
              <TemplatesCard
                key={template.id}
                title={template.name || 'Untitled Template'}
                useCount={template.usageCount}
                initial={template.name?.[0]?.toUpperCase() || 'T'}
                bgColor={getRandomColor()}
                preview={template.cover?.url}
                description={template.description}
              />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};
