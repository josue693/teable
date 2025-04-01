/* eslint-disable jsx-a11y/no-static-element-interactions */
/* eslint-disable jsx-a11y/click-events-have-key-events */
import {
  AlertCircle,
  Check,
  ChevronDown,
  ChevronUp,
  Code,
  Database,
  Loader2,
  Play,
} from '@teable/icons';
import type { IAiChatMessageContent } from '@teable/openapi';
import { ChatGraphNode, ChatMessageDataType } from '@teable/openapi';
import { cn } from '@teable/ui-lib/shadcn';
import { Bot, FileSearch } from 'lucide-react';
import { useTranslation } from 'next-i18next';
import { useCallback, useMemo, useState } from 'react';
import { MarkdownPreview } from '@/features/app/components/mark-down-preview';
import type { IAIMessage } from '../store/useMessage';

export const MessageSteps = ({ message }: { message: IAIMessage }) => {
  const [expandedSteps, setExpandedSteps] = useState<Record<string, boolean>>({});
  const { t } = useTranslation(['ai-chat']);
  const toggleStepExpand = (stepId: string) => {
    setExpandedSteps((prev) => ({
      ...prev,
      [stepId]: !prev[stepId],
    }));
  };

  const tepStatusIconMap = useMemo(
    () => ({
      processing: <Loader2 className="size-4 animate-spin text-primary" />,
      completed: <Check className="size-4 text-green-500" />,
      error: <AlertCircle className="size-4 text-destructive" />,
    }),
    []
  );

  const nameMap = useMemo(
    () => ({
      [ChatGraphNode.Indent]: t('ai-chat:graph.node.indent'),
      [ChatGraphNode.FieldSelector]: t('ai-chat:graph.node.fieldSelector'),
      [ChatGraphNode.SqlGenerator]: t('ai-chat:graph.node.sqlGenerator'),
      [ChatGraphNode.SqlQuery]: t('ai-chat:graph.node.sqlQuery'),
      [ChatGraphNode.SqlFixer]: t('ai-chat:graph.node.sqlFixer'),
      [ChatGraphNode.SqlResponse]: t('ai-chat:graph.node.sqlResponse'),
      [ChatGraphNode.NormalResponse]: t('ai-chat:graph.node.normalResponse'),
    }),
    [t]
  );

  const getMessageContent = (messageContent: IAiChatMessageContent) => {
    if (messageContent.type === ChatMessageDataType.SqlResult) {
      return messageContent.error || `\`\`\`json\n${messageContent.text}\n\`\`\``;
    }
    return messageContent.text;
  };

  const iconMap = useCallback((node: ChatGraphNode, status: string) => {
    if (status === 'error') {
      return <AlertCircle className="size-4 text-destructive" />;
    }
    switch (node) {
      case ChatGraphNode.Indent:
        return <FileSearch className="size-4" />;
      case ChatGraphNode.FieldSelector:
        return <Database className="size-4" />;
      case ChatGraphNode.SqlGenerator:
        return <Code className="size-4" />;
      case ChatGraphNode.SqlQuery:
        return <Play className="size-4" />;
      case ChatGraphNode.SqlFixer:
        return <Bot className="size-4" />;
      default:
        return null;
    }
  }, []);
  if (!message.content.length) {
    return;
  }
  return (
    <div className="mb-3 space-y-2">
      {message.content.map((nodeData, stepIndex) => {
        const isProcessing = message.processingIndex === stepIndex;
        const isError = nodeData.type === ChatMessageDataType.SqlResult && nodeData.error;
        const isExpanded = expandedSteps[`${message.id}-${stepIndex}`];
        const isCompleted = stepIndex < message.processingIndex;
        const stepStatus = isProcessing ? 'processing' : isError ? 'error' : 'completed';
        if (
          nodeData.node === ChatGraphNode.NormalResponse ||
          nodeData.node === ChatGraphNode.SqlResponse
        ) {
          return (
            <div
              key={stepIndex}
              className="rounded-xl rounded-t-sm border bg-muted px-4 py-3 shadow-sm"
            >
              <MarkdownPreview className="!bg-muted">{nodeData.text}</MarkdownPreview>
            </div>
          );
        }
        return (
          <div
            key={stepIndex}
            className={cn(
              'border rounded-xl overflow-hidden transition-all duration-500 ease-in-out',
              {
                'border-primary shadow-sm': isProcessing,
                'border-destructive shadow-sm': isError,
                'hover:bg-muted/50': isExpanded,
              }
            )}
            style={{
              animationDelay: `${stepIndex * 100}ms`,
              animationFillMode: 'both',
            }}
          >
            <div
              className={cn(
                'flex items-center justify-between p-3 cursor-pointer transition-all duration-300',
                {
                  'bg-primary/5': isProcessing,
                  'bg-destructive/5': isError,
                  'hover:bg-muted/50': isCompleted && !isExpanded,
                }
              )}
              onClick={() => toggleStepExpand(`${message.id}-${stepIndex}`)}
            >
              <div className="flex items-center gap-2">
                <div
                  className={cn(
                    'w-6 h-6 rounded-full flex items-center justify-center transition-colors duration-300',
                    {
                      'bg-primary/10 text-primary': isProcessing,
                      'bg-destructive/10 text-destructive': isError,
                      'bg-green-500/10 text-green-500': isCompleted,
                    }
                  )}
                >
                  {iconMap(nodeData.node, stepStatus)}
                </div>
                <span
                  className={cn('text-sm font-medium transition-colors duration-300', {
                    'text-primary': isProcessing,
                    'text-destructive': isError,
                    'text-foreground': isCompleted,
                  })}
                >
                  {nameMap[nodeData.node]}
                </span>
              </div>
              <div className="flex items-center gap-2">
                {tepStatusIconMap[stepStatus]}
                {isExpanded ? (
                  <ChevronUp className="size-4 text-muted-foreground transition-transform duration-300" />
                ) : (
                  <ChevronDown className="size-4 text-muted-foreground transition-transform duration-300" />
                )}
              </div>
            </div>

            <div
              className={cn(
                'overflow-hidden transition-all duration-500 ease-in-out max-h-0 opacity-0',
                {
                  'max-h-[500px] opacity-100': isExpanded,
                }
              )}
            >
              {nodeData && (
                <div className="border-t p-3 pt-0 text-sm">
                  <div className="prose prose-sm dark:prose-invert max-w-none pt-3">
                    <MarkdownPreview>{getMessageContent(nodeData)}</MarkdownPreview>
                  </div>
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
};
