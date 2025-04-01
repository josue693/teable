import { BadRequestException, Injectable } from '@nestjs/common';
import { ChatGraphNode, ChatMessageDataType, ChatMessageType } from '@teable/openapi';
import type { IAiChatMessageContent, IChatCompletionsRo, IChatEventMessage } from '@teable/openapi';
import type { IChatGraphState } from '../ai/langchain/graph/chat-graph';
import {
  stringifyFieldSelectorNodeResult,
  stringifyIntentNodeResult,
  stringifySqlFixerNodeResult,
  stringifySqlGeneratorNodeResult,
} from '../ai/langchain/graph/utils';
import { ChatMessageService } from './chat-message.service';
import { GraphService } from './graph/graph.service';
import { isResponseNode } from './utils';

@Injectable()
export class ChatService {
  constructor(
    private readonly graphService: GraphService,
    private readonly chatMessageService: ChatMessageService
  ) {}

  async chat(
    sessionId: string,
    body: IChatCompletionsRo,
    {
      baseId,
      signal,
      messageHandler,
    }: {
      baseId: string;
      signal?: AbortSignal | undefined;
      messageHandler: (chatEventMessage: IChatEventMessage) => void;
    }
  ) {
    const { question, tableQuery } = body;
    const tableId = tableQuery?.tableId;
    const tableInfo = tableId
      ? await this.graphService.getTableInfo(tableId, tableQuery)
      : undefined;
    const aiMessage: IAiChatMessageContent[] = [];
    let lastNode: ChatGraphNode | undefined;
    const innerMessageHandler = (message: IChatEventMessage) => {
      if (message.event === ChatMessageType.AI) {
        aiMessage.push(message.data);
      }
      messageHandler(message);
    };
    await this.chatMessageService.completionsQuestions(sessionId, question);
    const aiMessageId = (await this.chatMessageService.completions(sessionId, undefined, aiMessage))
      .id;
    innerMessageHandler({
      event: ChatMessageType.Basic,
      data: {
        messageId: aiMessageId,
      },
    });
    const graph = await this.graphService.getGraph(baseId, sessionId, tableInfo, {
      onStreamChunk: (node, chunk) => {
        if (!isResponseNode(node)) {
          return;
        }
        const content = chunk.content;
        const isReasoning = chunk.additional_kwargs?.reasoning_content;
        if (isReasoning) {
          const reasoning = chunk.additional_kwargs?.reasoning_content;
          innerMessageHandler({
            event: ChatMessageType.AI,
            data: {
              type: ChatMessageDataType.Reasoning,
              text: reasoning as string,
              node,
            },
          });
          return;
        }
        const contentText = typeof content === 'string' ? content : JSON.stringify(content);
        innerMessageHandler({
          event: ChatMessageType.AI,
          data: {
            type: ChatMessageDataType.Text,
            text: contentText,
            node,
          },
        });
      },
      onNodeProgress: (node) => {
        lastNode = node;
        switch (node) {
          case ChatGraphNode.Indent:
          case ChatGraphNode.FieldSelector:
            innerMessageHandler({
              event: ChatMessageType.AI,
              data: {
                type: ChatMessageDataType.Text,
                text: '',
                node,
              },
            });
            break;
          case ChatGraphNode.SqlFixer:
          case ChatGraphNode.SqlGenerator:
            innerMessageHandler({
              event: ChatMessageType.AI,
              data: {
                type: ChatMessageDataType.Sql,
                text: '',
                node,
              },
            });
            break;
          case ChatGraphNode.SqlQuery:
            innerMessageHandler({
              event: ChatMessageType.AI,
              data: {
                type: ChatMessageDataType.SqlResult,
                text: '',
                node,
              },
            });
            break;
          default:
            break;
        }
      },
      onNodeResult: (node, result) => {
        if (isResponseNode(node)) {
          return;
        }
        switch (node) {
          case ChatGraphNode.Indent:
            {
              const res = result as IChatGraphState['indentResult'];
              innerMessageHandler({
                event: ChatMessageType.AI,
                data: {
                  type: ChatMessageDataType.Text,
                  text: stringifyIntentNodeResult(res),
                  node,
                },
              });
            }
            break;
          case ChatGraphNode.FieldSelector:
            {
              const res = result as IChatGraphState['fieldSelectorResult'];
              innerMessageHandler({
                event: ChatMessageType.AI,
                data: {
                  type: ChatMessageDataType.Text,
                  text: stringifyFieldSelectorNodeResult(res),
                  node,
                },
              });
            }
            break;
          case ChatGraphNode.SqlFixer:
            {
              const res = result as IChatGraphState['sqlFixerResult'];
              innerMessageHandler({
                event: ChatMessageType.AI,
                data: {
                  type: ChatMessageDataType.Sql,
                  text: stringifySqlFixerNodeResult(res),
                  node,
                },
              });
            }
            break;
          case ChatGraphNode.SqlGenerator:
            {
              const res = result as IChatGraphState['sqlGeneratorResult'];
              innerMessageHandler({
                event: ChatMessageType.AI,
                data: {
                  type: ChatMessageDataType.Sql,
                  text: stringifySqlGeneratorNodeResult(res),
                  node,
                },
              });
            }
            break;
          case ChatGraphNode.SqlQuery:
            {
              const sqlResult = result as IChatGraphState['sqlQueryResult'];
              innerMessageHandler({
                event: ChatMessageType.AI,
                data: {
                  type: ChatMessageDataType.SqlResult,
                  node,
                  text: sqlResult?.result || '',
                  error: sqlResult?.error,
                },
              });
            }
            break;
          default:
            throw new BadRequestException(`Unknown node: ${node}`);
        }
      },
    });
    try {
      await graph.invoke(
        {
          question,
        },
        { signal }
      );
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (e: any) {
      if (e.message === 'Aborted') {
        return;
      }
      innerMessageHandler({
        event: ChatMessageType.Error,
        data: {
          type: ChatMessageDataType.Text,
          text: e?.message,
          node: lastNode || ChatGraphNode.Indent,
        },
      });
    }
    await this.chatMessageService.completions(sessionId, aiMessageId, aiMessage);
    innerMessageHandler({
      event: ChatMessageType.Finish,
      data: null,
    });
  }
}
