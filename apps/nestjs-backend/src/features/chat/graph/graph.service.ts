import { AIMessage, HumanMessage } from '@langchain/core/messages';
import { ChatOpenAI } from '@langchain/openai';
import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { CellFormat, FieldKeyType, FieldType } from '@teable/core';
import type { ISelectFieldOptions } from '@teable/core';
import { PrismaService } from '@teable/db-main-prisma';
import { ChatMessageRole } from '@teable/openapi';
import type { IAiChatMessageContent, IChatCompletionsTableQuery } from '@teable/openapi';
import { convertNameToValidCharacter } from '../../../utils';
import { AiService } from '../../ai/ai.service';
import type { IChatGraphCallbacks, IChatGraphTableInfo } from '../../ai/langchain/graph/chat-graph';
import { createChatGraph } from '../../ai/langchain/graph/chat-graph';
import { BaseSqlExecutorService } from '../../base-sql-executor/base-sql-executor.service';
import { FieldService } from '../../field/field.service';
import { RecordService } from '../../record/record.service';
import { isResponseNode } from '../utils';

@Injectable()
export class GraphService {
  constructor(
    private readonly prismaService: PrismaService,
    private readonly fieldService: FieldService,
    private readonly recordService: RecordService,
    private readonly aiService: AiService,
    private readonly baseSqlExecutorService: BaseSqlExecutorService
  ) {}

  private async runSafe(
    sql: string,
    opts: {
      baseId?: string;
      view?: IChatGraphTableInfo['view'];
      tableDbName?: string;
    }
  ) {
    const { baseId, view, tableDbName } = opts;
    if (!tableDbName) {
      throw new BadRequestException('Table db name is required');
    }
    if (!baseId) {
      throw new BadRequestException('Base id is required');
    }
    try {
      let querySql = sql;
      if (view) {
        querySql = `WITH ${view.name} AS (${view.sql}) ${sql}`;
      }
      const result = await this.baseSqlExecutorService.executeQuerySql(baseId, querySql, {
        projectionTableDbNames: [tableDbName],
      });
      return {
        result,
        error: '',
      };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (e: any) {
      return {
        result: '',
        error: e.message,
      };
    }
  }

  private async getView(tableId: string, query?: IChatCompletionsTableQuery) {
    const { viewId } = query ?? {};
    if (!query || !viewId) {
      return undefined;
    }
    const view = await this.prismaService.view.findUnique({
      where: {
        id: viewId,
      },
    });
    if (!view) {
      return undefined;
    }
    const { queryBuilder: viewTempBuilder } = await this.recordService.buildFilterSortQuery(
      tableId,
      query
    );
    return {
      name: convertNameToValidCharacter(view.name).toLowerCase(),
      sql: viewTempBuilder.toQuery(),
    };
  }

  async getTableInfo(
    tableId: string,
    query?: IChatCompletionsTableQuery
  ): Promise<IChatGraphTableInfo> {
    const { viewId, projection, filter, orderBy, ignoreViewQuery } = query ?? {};
    const table = await this.prismaService.tableMeta.findUnique({
      where: {
        id: tableId,
      },
      select: {
        name: true,
        dbTableName: true,
      },
    });
    if (!table) {
      throw new NotFoundException('Table not found');
    }
    const fields = await this.fieldService.getFieldsByQuery(tableId, {
      filterHidden: true,
      viewId,
      projection,
    });
    const preRows = await this.recordService.getRecordsFields(tableId, {
      viewId,
      projection,
      filter,
      orderBy,
      ignoreViewQuery,
      skip: 0,
      take: 5,
      cellFormat: CellFormat.Text,
      fieldKeyType: FieldKeyType.Name,
    });
    const view = await this.getView(tableId, query);

    const [schema, dbTableName] = table.dbTableName.split('.');

    return {
      name: table.name,
      schema,
      dbTableName,
      view,
      fields: fields.map((field) => ({
        name: field.name,
        type: field.type.toLowerCase(),
      })),
      detailFields: fields.map((field) => ({
        name: field.name,
        description: field.description ?? '',
        type: field.type.toLowerCase(),
        dbFieldType: field.dbFieldType,
        dbFieldName: field.dbFieldName,
        cellValueType: field.cellValueType,
        options: [FieldType.SingleSelect, FieldType.MultipleSelect].includes(field.type)
          ? (field.options as ISelectFieldOptions).choices.map((choice) => choice.name)
          : undefined,
      })),
      preRows: preRows.map((row) => row.fields),
    };
  }

  private async getHistoryMessages(sessionId: string) {
    const session = await this.prismaService.chatMessage.findMany({
      where: {
        sessionId,
      },
      orderBy: {
        createdTime: 'desc',
      },
      skip: 2,
      take: 4,
      select: {
        role: true,
        content: true,
      },
    });
    const aiMessages = session
      .filter((message) => message.role === ChatMessageRole.Assistant)
      .map((message) => {
        const aiMessageContent = message?.content
          ? (JSON.parse(message.content) as IAiChatMessageContent[]).slice(-1)[0]
          : undefined;
        return aiMessageContent && isResponseNode(aiMessageContent?.node) && aiMessageContent?.text
          ? new AIMessage(aiMessageContent?.text)
          : undefined;
      })
      .filter(Boolean) as AIMessage[];
    const humanMessages = session
      .filter((message) => message.role === ChatMessageRole.Human)
      .map((message) => (message.content ? new HumanMessage(message.content) : undefined))
      .filter(Boolean) as HumanMessage[];
    return [...humanMessages, ...aiMessages];
  }

  async getGraph(
    baseId: string,
    sessionId: string,
    tableInfo?: IChatGraphTableInfo,
    callbacks?: IChatGraphCallbacks
  ) {
    let totalTokens = 0;
    const { codingModel, llmProviders } = await this.aiService.getAIConfig(baseId);
    const llmProvider = await this.aiService.getModelConfig(codingModel, llmProviders);
    const llm = new ChatOpenAI({
      model: llmProvider.model,
      apiKey: llmProvider.apiKey,
      configuration: {
        baseURL: llmProvider.baseUrl,
      },
      temperature: 0,
      callbacks: [
        {
          handleLLMEnd(output) {
            totalTokens += output.llmOutput?.tokenUsage?.totalTokens || 0;
            console.log('totalTokens', totalTokens);
          },
        },
      ],
    });
    return createChatGraph({
      llm,
      db: {
        driverClient: this.baseSqlExecutorService.driver,
        runSafe: (sql) =>
          this.runSafe(sql, {
            baseId,
            view: tableInfo?.view,
            tableDbName: tableInfo?.dbTableName,
          }),
        tableInfo: tableInfo,
      },
      historyMessages: await this.getHistoryMessages(sessionId),
      callbacks,
    });
  }
}
