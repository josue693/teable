/* eslint-disable jsx-a11y/no-static-element-interactions */
/* eslint-disable jsx-a11y/click-events-have-key-events */
import type { UseChatHelpers } from '@ai-sdk/react';
import { ViewType } from '@teable/core';
import { Check, ChevronDown, X, LocateFixed } from '@teable/icons';
import type { IToolInvocationUIPart, IUpdateRecordsToolParams } from '@teable/openapi';
import { McpToolInvocationName } from '@teable/openapi';
import { useTables, useView } from '@teable/sdk/hooks';
import { Spin } from '@teable/ui-lib/base';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
  cn,
  useToast,
} from '@teable/ui-lib/shadcn';
import { isEqual } from 'lodash';
import { ChevronRight } from 'lucide-react';
import { useRouter } from 'next/router';
import { useTranslation } from 'next-i18next';
import { memo, useContext, useMemo, useState } from 'react';
import { useGridSearchStore } from '@/features/app/blocks/view/grid/useGridSearchStore';
import { ChatContext } from '../../context/ChatContext';
import { useMcpToolExecution } from '../../hooks/useMcpToolExecution';
import { useChatControlStore } from '../../store/useChatControl';
import { Markdown } from './Markdown';
import { AIChangePreview } from './preview/AIChangePreview';

export interface IToolMessagePart {
  id: string;
  part: UseChatHelpers['messages'][number]['parts'][number] & {
    type: 'tool-invocation';
  };
  mode?: 'manual' | 'agent';
}

export const PureToolMessagePart = ({ id, part }: IToolMessagePart) => {
  const { toolInvocation } = part;

  const { toast } = useToast();

  const [isExpanded, setIsExpanded] = useState(false);
  const { t } = useTranslation(['table']);
  const [loading, setLoading] = useState(false);

  const toolExecutionMap = useMcpToolExecution();

  const { addToolResult } = useContext(ChatContext);

  const needConfirm = toolInvocation.state === 'call';

  const { gridRef, recordMap } = useGridSearchStore();

  const { setToolCallInfo } = useChatControlStore();

  const rejectedTool =
    (toolInvocation as unknown as IToolInvocationUIPart)?.toolInvocation?.result?.toolCallStatus ===
    'rejected';

  const shouldLocateDiffPosition = useMemo(() => {
    const locatePositionActions = [
      McpToolInvocationName.UpdateRecords,
      McpToolInvocationName.DeleteRecords,
    ];

    return locatePositionActions.includes(toolInvocation.toolName as McpToolInvocationName);
  }, [toolInvocation.toolName]);

  const toolName = useMemo(() => {
    switch (toolInvocation.toolName) {
      case McpToolInvocationName.GetTableFields:
        return t('aiChat.tool.getTableFields');
      case McpToolInvocationName.GetTablesMeta:
        return t('aiChat.tool.getTablesMeta');
      case McpToolInvocationName.GetRecords:
        return t('aiChat.tool.getRecords');
      case McpToolInvocationName.GetTableViews:
        return t('aiChat.tool.getTableViews');
      case McpToolInvocationName.SqlQuery:
        return t('aiChat.tool.sqlQuery');
      case McpToolInvocationName.CreateTable:
        return t('aiChat.tool.createTable');
      case McpToolInvocationName.CreateView:
        return t('aiChat.tool.createView');
      case McpToolInvocationName.CreateField:
        return t('aiChat.tool.createField');
      case McpToolInvocationName.CreateRecords:
        return t('aiChat.tool.createRecords');
      case McpToolInvocationName.RunScripts:
        return t('aiChat.tool.runScripts');

      case McpToolInvocationName.UpdateTableName:
        return t('aiChat.tool.updateTableName');
      case McpToolInvocationName.UpdateField:
        return t('aiChat.tool.updateField');
      case McpToolInvocationName.UpdateRecords:
        return t('aiChat.tool.updateRecords');
      case McpToolInvocationName.DeleteFields:
        return t('aiChat.tool.deleteField');
      case McpToolInvocationName.DeleteTable:
        return t('aiChat.tool.deleteTable');
      case McpToolInvocationName.GetUpdateFieldsParams:
        return t('aiChat.tool.getUpdateFieldsParams');
      case McpToolInvocationName.UpdateViewName:
        return t('aiChat.tool.updateViewName');
      case McpToolInvocationName.DeleteView:
        return t('aiChat.tool.deleteView');
      case McpToolInvocationName.DeleteRecords:
        return t('aiChat.tool.deleteRecords');
      default:
        return toolInvocation.toolName;
    }
  }, [toolInvocation.toolName, t]);

  const router = useRouter();
  const currentTableId = router.query.tableId;
  const currentBaseId = router.query.baseId;

  const tables = useTables();

  const view = useView();

  const tableIds = useMemo(() => {
    return tables.map((table) => table.id);
  }, [tables]);

  /* eslint-disable sonarjs/cognitive-complexity */
  const locatePosition = () => {
    switch (toolInvocation.toolName) {
      case McpToolInvocationName.UpdateRecords: {
        const { updateRecordsRo, tableId } = toolInvocation.args as IUpdateRecordsToolParams;

        if (tableId !== currentTableId && tableIds.includes(tableId)) {
          router.push(
            {
              pathname: `/base/[baseId]/[tableId]/`,
              query: {
                baseId: currentBaseId,
                tableId: tableId,
              },
            },
            undefined,
            {
              shallow: Boolean(tableId),
            }
          );
        }

        if (view?.type !== ViewType.Grid) {
          toast({
            title: t('aiChat.tool.tips.title'),
            description: t('aiChat.tool.tips.description'),
          });
          return;
        }

        const { records } = updateRecordsRo;
        const lastRecordId = records.map((record) => record.id).at(-1);
        if (!lastRecordId) {
          return;
        }

        for (const [key, value] of Object.entries(recordMap || {})) {
          if (value?.id === lastRecordId) {
            gridRef?.current?.scrollToItem([0, Number(key)]);
          }
        }
        break;
      }
      case McpToolInvocationName.DeleteRecords: {
        const { recordIds, tableId } = toolInvocation.args;
        const lastRecordId = recordIds.at(-1);
        if (!lastRecordId) {
          return;
        }

        if (tableId !== currentTableId && tableIds.includes(tableId)) {
          router.push(
            {
              pathname: `/base/[baseId]/[tableId]/`,
              query: {
                baseId: currentBaseId,
                tableId: tableId,
              },
            },
            undefined,
            {
              shallow: Boolean(tableId),
            }
          );
        }

        for (const [key, value] of Object.entries(recordMap || {})) {
          if (value?.id === lastRecordId) {
            gridRef?.current?.scrollToItem([0, Number(key)]);
          }
        }
        break;
      }
      case McpToolInvocationName.DeleteFields: {
        break;
      }
      default: {
        return;
      }
    }
  };

  const isResult = toolInvocation.state === 'result';

  return (
    <Accordion
      type="single"
      collapsible
      value={isExpanded ? 'expanded' : 'collapsed'}
      onValueChange={(value) => {
        setIsExpanded(value === 'expanded');
      }}
      className="w-full"
    >
      <AccordionItem
        value="expanded"
        className="font-sm rounded-lg border bg-neutral-50 px-2 dark:bg-neutral-900/80"
      >
        <AccordionTrigger
          headerClassName="flex-1"
          hiddenChevron
          className="gap-1 py-2 text-xs font-normal text-muted-foreground hover:no-underline"
        >
          <div className="flex items-center gap-2">
            {isExpanded ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
            <div>{toolName}</div>
          </div>
          <div className="flex items-center gap-2">
            {rejectedTool ? (
              <X className="size-4 text-red-500" />
            ) : loading ? (
              <Spin className="size-4" />
            ) : (
              <div className="flex items-center gap-2">
                {needConfirm && shouldLocateDiffPosition && (
                  <LocateFixed
                    className="size-4 hover:opacity-80"
                    onClick={(e) => {
                      e.stopPropagation();
                      locatePosition();
                    }}
                  />
                )}
                <Check
                  className={cn('size-4 hover:opacity-80', {
                    'text-green-500': !needConfirm,
                  })}
                  onClick={async (e) => {
                    e.stopPropagation();
                    if (!needConfirm) {
                      return;
                    }
                    setLoading(true);
                    try {
                      const { execute, callBack } = toolExecutionMap[toolInvocation.toolName];
                      if (!execute || !callBack) {
                        return;
                      }
                      const result = await execute(toolInvocation.args);
                      callBack(result, toolInvocation.args);
                      addToolResult({
                        toolCallId: toolInvocation.toolCallId,
                        result: {
                          content: [
                            {
                              text: JSON.stringify(result),
                              type: 'text',
                            },
                          ],
                          toolCallStatus: 'success',
                        },
                      });
                      setToolCallInfo(
                        toolInvocation?.args?.tableId || null,
                        toolInvocation.toolName as McpToolInvocationName
                      );
                    } catch (error) {
                      addToolResult({
                        toolCallId: toolInvocation.toolCallId,
                        result: {
                          content: [
                            {
                              text: `execute tool failed reason: ${JSON.stringify(error)}`,
                              type: 'text',
                            },
                          ],
                          toolCallStatus: 'error',
                        },
                      });
                    }
                    setLoading(false);
                  }}
                />
                {needConfirm && (
                  <X
                    className="size-4 hover:opacity-80"
                    onClick={(e) => {
                      e.stopPropagation();
                      addToolResult({
                        toolCallId: toolInvocation.toolCallId,
                        result: {
                          content: [
                            {
                              text: `reject to execute this tool.`,
                              type: 'text',
                            },
                          ],
                          toolCallStatus: 'rejected',
                        },
                      });
                    }}
                  />
                )}
              </div>
            )}
          </div>
        </AccordionTrigger>
        <AccordionContent>
          <div className="space-y-2 px-3 text-muted-foreground">
            {needConfirm ? (
              <AIChangePreview toolInvocation={toolInvocation} />
            ) : (
              <>
                <div className="space-y-1">
                  <div className="text-sm">{t('table:aiChat.tool.args')}: </div>
                  <ContentRenderer id={id} content={JSON.stringify(toolInvocation.args, null, 2)} />
                </div>
                {isResult && (
                  <div className="space-y-1">
                    <div className="text-sm">{t('table:aiChat.tool.result')}: </div>
                    <ToolsResultRenderer id={id} toolInvocation={toolInvocation} />
                  </div>
                )}
              </>
            )}
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
};

const PureToolsResultRenderer = ({
  id,
  toolInvocation,
}: {
  id: string;
  toolInvocation: IToolMessagePart['part']['toolInvocation'] & { state: 'result' };
}) => {
  const result = toolInvocation.result?.['content']?.[0]?.text;

  const content = useMemo(() => {
    switch (toolInvocation.toolName) {
      case McpToolInvocationName.CreateField:
      case McpToolInvocationName.CreateRecords:
      case McpToolInvocationName.CreateTable:
      case McpToolInvocationName.RunScripts:
      case McpToolInvocationName.UpdateField:
      case McpToolInvocationName.SqlQuery: {
        let res = result;
        try {
          res = JSON.stringify(JSON.parse(result), null, 2);
        } catch (error) {
          console.error(error);
        }
        return res;
      }
      case McpToolInvocationName.GetTableFields:
      case McpToolInvocationName.GetTablesMeta:
        return result;
      default:
        return JSON.stringify(toolInvocation.result, null, 2);
    }
  }, [result, toolInvocation.result, toolInvocation.toolName]);

  return <ContentRenderer id={id} content={content} />;
};

const ToolsResultRenderer = memo(PureToolsResultRenderer, (prev, next) => {
  if (prev.id !== next.id) return false;
  if (isEqual(prev.toolInvocation, next.toolInvocation)) return true;
  return false;
});

const ContentRenderer = ({ id, content }: { id: string; content: string }) => {
  return (
    <Markdown
      id={id}
      className="p-0"
      components={{
        pre(props) {
          const { children, ...rest } = props;
          return (
            <pre
              {...rest}
              style={{
                padding: 0,
              }}
            >
              {children}
            </pre>
          );
        },
      }}
    >{`\`\`\`json\n${content}`}</Markdown>
  );
};

export const ToolMessagePart = memo(PureToolMessagePart, (prev, next) => {
  return isEqual(prev.part, next.part);
});
