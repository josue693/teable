import { Plus } from '@teable/icons';
import { Button } from '@teable/ui-lib/shadcn';
import { CornerDownRight } from 'lucide-react';
import { useTranslation } from 'next-i18next';
import React, { useState } from 'react';
import { useChatPanelStore } from '../../components/ai-chat/store/useChatPanelStore';

interface IPromptBoxProps {
  onEnter: (text: string) => void;
}

export const PromptBox: React.FC<IPromptBoxProps> = ({ onEnter }) => {
  const { t } = useTranslation(['common']);
  const [prompt, setPrompt] = useState('');
  const { open } = useChatPanelStore();
  const suggestions = [
    {
      title: t('common:template.promptBox.ideasList.crm.title'),
      prompt: t('common:template.promptBox.ideasList.crm.prompt'),
    },
    {
      title: t('common:template.promptBox.ideasList.projectManagement.title'),
      prompt: t('common:template.promptBox.ideasList.projectManagement.prompt'),
    },
    {
      title: t('common:template.promptBox.ideasList.marketingCampaign.title'),
      prompt: t('common:template.promptBox.ideasList.marketingCampaign.prompt'),
    },
    {
      title: t('common:template.promptBox.ideasList.teamCollaboration.title'),
      prompt: t('common:template.promptBox.ideasList.teamCollaboration.prompt'),
    },
  ];
  return (
    <div className="mx-auto flex px-4 sm:px-6 lg:px-8">
      <div className="shadow-black/6 w-full max-w-3xl rounded-2xl border border-gray-200 bg-white p-4 text-left shadow-sm">
        <div className="relative space-y-2 rounded-2xl bg-gray-50 px-6 py-3">
          <textarea
            id="prompt-box"
            className="h-[60px] w-full resize-none bg-gray-50 text-sm focus:outline-none focus:ring-0"
            placeholder="Build your business app with Teable"
            rows={3}
            value={prompt}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.stopPropagation();
                e.preventDefault();
                onEnter(prompt);
                open();
              }
            }}
            onChange={(e) => setPrompt(e.target.value)}
          />

          <div className="flex items-center justify-between">
            {/* Plus Icon */}
            <Plus className="size-5 text-gray-600 opacity-0" strokeWidth={1.5} />

            {/* Start Button */}
            <Button
              variant={'ghost'}
              size={'xs'}
              className="flex items-center gap-2 rounded-lg transition-colors hover:bg-gray-100"
              onClick={() => {
                onEnter(prompt);
                open();
              }}
            >
              <span className="text-sm text-gray-600">{t('common:template.promptBox.start')}</span>
              <CornerDownRight className="size-5 text-gray-600" strokeWidth={1.5} />
            </Button>
          </div>
        </div>

        <div className="mt-4 space-y-2.5">
          <p className="text-sm text-gray-400">{t('common:template.promptBox.ideas')}</p>

          <div className="flex flex-wrap gap-2">
            {suggestions.map((suggestion) => (
              <Button
                key={suggestion.title}
                variant={'ghost'}
                size={'xs'}
                className="rounded-md bg-gray-50 px-5 py-1.5 text-xs text-gray-600 transition-colors hover:bg-gray-100"
                onClick={() => {
                  setPrompt(suggestion.prompt);
                }}
              >
                {suggestion.title}
              </Button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};
