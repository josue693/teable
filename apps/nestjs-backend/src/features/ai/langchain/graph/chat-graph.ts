import { HumanMessage, SystemMessage } from '@langchain/core/messages';
import type { AIMessage, AIMessageChunk } from '@langchain/core/messages';
import { StructuredOutputParser } from '@langchain/core/output_parsers';
import { ChatPromptTemplate, MessagesPlaceholder } from '@langchain/core/prompts';
import type { LangGraphRunnableConfig } from '@langchain/langgraph';
import { Annotation, END, START, StateGraph } from '@langchain/langgraph';
import type { ChatOpenAI } from '@langchain/openai';
import { BadRequestException } from '@nestjs/common';
import type { DriverClient } from '@teable/core';
import { ChatGraphNode } from '@teable/openapi';
import { CHAT_INTENT_PROMPT, chatIntentSchema, ChatIntentType } from '../prompts/chat-indent';
import { FIELD_SELECTOR_PROMPT, fieldSelectorSchema } from '../prompts/field-selector';
import { SQL_FIXER_PROMPT, sqlFixSchema } from '../prompts/sql-fixer';
import { getSQLGeneratorPrompt, sqlGeneratorSchema } from '../prompts/sql-generator';
import { SQL_RESPONSE_PROMPT } from '../prompts/sql-response';
import { stream2result } from '../utils/stream2result';

export interface IChatGraphTableInfo {
  name: string;
  view?: { name: string; sql: string };
  schema: string;
  dbTableName: string;
  fields: { name: string; type: string }[];
  detailFields: {
    name: string;
    description: string;
    type: string;
    dbFieldType: string;
    dbFieldName: string;
    cellValueType: string;
    options?: unknown;
  }[];
  preRows: Record<string, unknown>[];
}

const chatStateAnnotation = Annotation.Root({
  indentResult: Annotation<{ type: ChatIntentType; message: string }>,
  fieldSelectorResult: Annotation<{ fields: string[]; message: string }>,
  sqlGeneratorResult: Annotation<{ sql: string; message: string }>,
  sqlQueryResult: Annotation<{ result?: string; error?: string }>,
  sqlFixerResult: Annotation<{ sql?: string; message?: string }>,
  sqlResponseResult: Annotation<string>,
  normalResponseResult: Annotation<string>,
  question: Annotation<string>,
  sql: Annotation<string>,
  sqlError: Annotation<string>,
  sqlFixAttempts: Annotation<number>,
});

export enum EdgeType {
  Success = 'success',
  Error = 'error',
  End = 'end',
}

export type IChatGraphState = typeof chatStateAnnotation.State;

type IOnChatGraphNodeResult = {
  (node: ChatGraphNode.Indent, result: IChatGraphState['indentResult']): void;
  (node: ChatGraphNode.FieldSelector, result: IChatGraphState['fieldSelectorResult']): void;
  (node: ChatGraphNode.SqlGenerator, result: IChatGraphState['sqlGeneratorResult']): void;
  (node: ChatGraphNode.SqlQuery, result: IChatGraphState['sqlQueryResult']): void;
  (node: ChatGraphNode.SqlFixer, result: IChatGraphState['sqlFixerResult']): void;
  (node: ChatGraphNode.SqlResponse, result: IChatGraphState['sqlResponseResult']): void;
  (node: ChatGraphNode.NormalResponse, result: IChatGraphState['normalResponseResult']): void;
};

export type IChatGraphCallbacks = {
  onStreamChunk?: (node: ChatGraphNode, chunk: AIMessageChunk) => void;
  onNodeProgress?: (node: ChatGraphNode) => void;
  onNodeResult?: IOnChatGraphNodeResult;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const baseRetryOn = (error: any) => {
  return error.message !== 'Aborted';
};

const systemMessage = new SystemMessage(
  'You are a helpful assistant. You must respond in the EXACT SAME LANGUAGE as the input question.'
);

export const createChatGraph = ({
  llm,
  db,
  historyMessages,
  callbacks,
}: {
  llm: ChatOpenAI;
  db: {
    driverClient: DriverClient;
    tableInfo?: IChatGraphTableInfo;
    runSafe: (sql: string) => Promise<{ result: unknown; error: string }>;
  };
  historyMessages?: (HumanMessage | AIMessage)[];
  callbacks?: IChatGraphCallbacks;
}) => {
  const { onStreamChunk, onNodeProgress, onNodeResult } = callbacks || {};
  const indentNode = async (state: IChatGraphState, config: LangGraphRunnableConfig) => {
    onNodeProgress?.(ChatGraphNode.Indent);
    const chatIntentChain = ChatPromptTemplate.fromMessages([
      systemMessage,
      new MessagesPlaceholder('history'),
      new HumanMessage(await CHAT_INTENT_PROMPT.format({ question: state.question })),
    ]).pipe(llm);
    const result = await chatIntentChain.stream(
      {
        history: historyMessages,
      },
      { signal: config.signal }
    );
    const resultString = await stream2result(result, (chunk) => {
      onStreamChunk?.(ChatGraphNode.Indent, chunk);
    });
    const resultJson = await new StructuredOutputParser(chatIntentSchema).parse(resultString);
    if (resultJson.type === ChatIntentType.SQL && !db.tableInfo) {
      throw new BadRequestException(`tableInfo is required for ${ChatIntentType.SQL}`);
    }
    onNodeResult?.(ChatGraphNode.Indent, resultJson);
    return {
      indentResult: resultJson,
    };
  };

  const tableInfo = db.tableInfo!;

  const fieldSelectorNode = async (state: IChatGraphState, config: LangGraphRunnableConfig) => {
    onNodeProgress?.(ChatGraphNode.FieldSelector);
    const columns = tableInfo.fields;
    const data = tableInfo.preRows.map((row) => columns.map((column) => row[column.name]));
    const fieldSelectorChain = ChatPromptTemplate.fromMessages([
      systemMessage,
      new MessagesPlaceholder('history'),
      new HumanMessage(
        await FIELD_SELECTOR_PROMPT.format({
          question: state.question,
          columns: columns.map((column) => ({
            name: column.name,
            type: column.type,
          })),
          data,
          data_length: data.length,
        })
      ),
    ]).pipe(llm);
    const result = await fieldSelectorChain.stream(
      {
        history: historyMessages,
      },
      { signal: config.signal }
    );
    const resultString = await stream2result(result, (chunk) => {
      onStreamChunk?.(ChatGraphNode.FieldSelector, chunk);
    });
    const resultJson = await new StructuredOutputParser(fieldSelectorSchema).parse(resultString);
    onNodeResult?.(ChatGraphNode.FieldSelector, resultJson);
    return {
      fieldSelectorResult: resultJson,
    };
  };

  const sqlGeneratorPrompt = getSQLGeneratorPrompt(db.driverClient);
  const sqlGeneratorNode = async (
    { question, fieldSelectorResult }: IChatGraphState,
    config: LangGraphRunnableConfig
  ) => {
    onNodeProgress?.(ChatGraphNode.SqlGenerator);
    const { view, dbTableName, schema, detailFields } = tableInfo;
    const { fields } = fieldSelectorResult;
    const sqlQueryChain = ChatPromptTemplate.fromMessages([
      systemMessage,
      new MessagesPlaceholder('history'),
      new HumanMessage(
        await sqlGeneratorPrompt.format({
          question,
          table_info: {
            schema: view ? undefined : schema,
            dbTableName: view ? view.name : dbTableName,
            columns: fields.map((field) =>
              detailFields.find((detailField) => detailField.name === field)
            ),
          },
          top_k: 5,
        })
      ),
    ]).pipe(llm);
    const result = await sqlQueryChain.stream(
      {
        history: historyMessages,
      },
      { signal: config.signal }
    );
    const resultString = await stream2result(result, (chunk) => {
      onStreamChunk?.(ChatGraphNode.SqlGenerator, chunk);
    });
    const resultJson = await new StructuredOutputParser(sqlGeneratorSchema).parse(resultString);
    onNodeResult?.(ChatGraphNode.SqlGenerator, resultJson);
    return {
      sqlGeneratorResult: resultJson,
      sql: resultJson.sql,
    };
  };
  const sqlQueryNode = async ({ sqlFixAttempts, sql }: IChatGraphState) => {
    onNodeProgress?.(ChatGraphNode.SqlQuery);
    const result = await db.runSafe(sql);
    const resultJson = {
      result:
        typeof result.result === 'string' ? result.result : JSON.stringify(result.result, null, 2),
      error: result.error,
    };
    onNodeResult?.(ChatGraphNode.SqlQuery, resultJson);
    return {
      sqlQueryResult: resultJson,
      sqlFixAttempts,
      sqlError: resultJson.error,
    };
  };

  const sqlFixerNode = async (
    { sql, question, fieldSelectorResult, sqlFixAttempts, sqlError }: IChatGraphState,
    config: LangGraphRunnableConfig
  ) => {
    onNodeProgress?.(ChatGraphNode.SqlFixer);
    const { view, dbTableName, schema, detailFields } = tableInfo;
    const { fields } = fieldSelectorResult;
    const sqlFixChain = ChatPromptTemplate.fromMessages([
      systemMessage,
      new MessagesPlaceholder('history'),
      new HumanMessage(
        await SQL_FIXER_PROMPT.format({
          question,
          sql,
          error: sqlError,
          dialect: db.driverClient,
          table_schemas: {
            schema: view ? undefined : schema,
            dbTableName: view ? view.name : dbTableName,
            columns: fields.map((field) =>
              detailFields.find((detailField) => detailField.name === field)
            ),
          },
        })
      ),
    ]).pipe(llm);
    const result = await sqlFixChain.stream(
      {
        history: historyMessages,
      },
      { signal: config.signal }
    );
    const resultString = await stream2result(result, (chunk) => {
      onStreamChunk?.(ChatGraphNode.SqlFixer, chunk);
    });
    const resultJson = await new StructuredOutputParser(sqlFixSchema).parse(resultString);
    onNodeResult?.(ChatGraphNode.SqlFixer, resultJson);
    return {
      sqlFixerResult: resultJson,
      sqlFixAttempts: (sqlFixAttempts || 0) + 1,
      sqlQueryResult: undefined,
      sql: resultJson.sql,
    };
  };

  const responseNode = async (
    { sqlQueryResult, sqlGeneratorResult, sqlFixerResult, question }: IChatGraphState,
    config: LangGraphRunnableConfig
  ) => {
    onNodeProgress?.(ChatGraphNode.SqlResponse);
    const responseChain = ChatPromptTemplate.fromMessages([
      systemMessage,
      new MessagesPlaceholder('history'),
      new HumanMessage(
        await SQL_RESPONSE_PROMPT.format({
          question,
          sql: sqlFixerResult?.sql || sqlGeneratorResult.sql,
          sql_result: sqlQueryResult.result,
        })
      ),
    ]).pipe(llm);
    console.log('ddddd', historyMessages);
    const result = await responseChain.stream(
      {
        history: historyMessages,
      },
      { signal: config.signal }
    );
    const resultString = await stream2result(result, (chunk) => {
      onStreamChunk?.(ChatGraphNode.SqlResponse, chunk);
    });
    onNodeResult?.(ChatGraphNode.SqlResponse, resultString);
    return {
      sqlResponseResult: resultString,
    };
  };

  const normalResponseNode = async (state: IChatGraphState, config: LangGraphRunnableConfig) => {
    onNodeProgress?.(ChatGraphNode.NormalResponse);
    const chatPrompt = ChatPromptTemplate.fromMessages([
      systemMessage,
      new MessagesPlaceholder('history'),
      new HumanMessage(state.question),
    ]);
    const normalResponseChain = chatPrompt.pipe(llm);
    const result = await normalResponseChain.stream(
      {
        history: historyMessages,
      },
      { signal: config.signal }
    );
    const resultString = await stream2result(result, (chunk) => {
      onStreamChunk?.(ChatGraphNode.NormalResponse, chunk);
    });
    onNodeResult?.(ChatGraphNode.NormalResponse, resultString);
    return {
      normalResponse: resultString,
    };
  };
  return new StateGraph(chatStateAnnotation)
    .addNode(ChatGraphNode.Indent, indentNode, {
      retryPolicy: {
        maxAttempts: 3,
        retryOn: baseRetryOn,
      },
    })
    .addNode(ChatGraphNode.FieldSelector, fieldSelectorNode, {
      retryPolicy: {
        maxAttempts: 3,
        retryOn: baseRetryOn,
      },
    })
    .addNode(ChatGraphNode.SqlGenerator, sqlGeneratorNode, {
      retryPolicy: {
        maxAttempts: 3,
        retryOn: baseRetryOn,
      },
    })
    .addNode(ChatGraphNode.SqlQuery, sqlQueryNode, {
      retryPolicy: {
        maxAttempts: 3,
        retryOn: baseRetryOn,
      },
    })
    .addNode(ChatGraphNode.SqlFixer, sqlFixerNode, {
      retryPolicy: {
        maxAttempts: 3,
        retryOn: (error) => {
          return baseRetryOn(error) && !error.sql;
        },
      },
    })
    .addNode(ChatGraphNode.SqlResponse, responseNode)
    .addNode(ChatGraphNode.NormalResponse, normalResponseNode)
    .addEdge(START, ChatGraphNode.Indent)
    .addConditionalEdges(
      ChatGraphNode.Indent,
      (state) => {
        return state.indentResult.type;
      },
      {
        [ChatIntentType.NORMAL]: ChatGraphNode.NormalResponse,
        [ChatIntentType.SQL]: ChatGraphNode.FieldSelector,
      }
    )
    .addConditionalEdges(
      ChatGraphNode.FieldSelector,
      (state) => {
        if (state.fieldSelectorResult.fields.length === 0) {
          return EdgeType.Error;
        }
        return EdgeType.Success;
      },
      {
        [EdgeType.End]: END,
        [EdgeType.Error]: END,
        [EdgeType.Success]: ChatGraphNode.SqlGenerator,
      }
    )
    .addEdge(ChatGraphNode.SqlGenerator, ChatGraphNode.SqlQuery)
    .addConditionalEdges(
      ChatGraphNode.SqlQuery,
      (state) => {
        if (state.sqlError) {
          return EdgeType.Error;
        }
        if (state.sqlFixAttempts > 5) {
          return EdgeType.Success;
        }
        return EdgeType.Success;
      },
      {
        [EdgeType.End]: END,
        [EdgeType.Error]: ChatGraphNode.SqlFixer,
        [EdgeType.Success]: ChatGraphNode.SqlResponse,
      }
    )
    .addEdge(ChatGraphNode.SqlFixer, ChatGraphNode.SqlQuery)
    .addEdge(ChatGraphNode.SqlResponse, END)
    .addEdge(ChatGraphNode.NormalResponse, END)
    .compile();
};
