import { PromptTemplate } from '@langchain/core/prompts';
import { DriverClient } from '@teable/core';
import { z } from 'zod';

// eslint-disable-next-line @typescript-eslint/naming-convention
export const SQL_POSTGRES_PROMPT = new PromptTemplate({
  template: `You are a PostgreSQL expert. Given an input question, first create a syntactically correct PostgreSQL query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most {top_k} results using the LIMIT clause as per PostgreSQL. You can order the results to return the most informative data in the database.
Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Note: You generate the sql not need to include CTE.
Note: If the user's question explicitly requires sorting, then the sql you generate should only contain "order by".

Schema Awareness:
- If table includes schema, explicitly specify schema using quoted notation: FROM "target_schema"."table"  
- Never use implicit schema paths like schema.table

Use the following format:
1.Use strict JSON format: {{"sql": "", "message": ""}}
2.No explanatory text
3.Ensure valid JSON syntax
4.Format SQL with line breaks/indentation while retaining valid JSON string syntax.
5.The message field must include detailed reasons if SQL cannot be generated.
6.If exist history messages, you should consider the history messages when generating the response.
7.column name should be quoted

Example:
{{
  "sql": "select count(*) from users",
  "message": "Statistic the total number of users"
}}

Only use the following tables:
{table_info}

Question: {question}`,
  inputVariables: ['table_info', 'question', 'top_k'],
});

// eslint-disable-next-line @typescript-eslint/naming-convention
export const SQL_SQLITE_PROMPT = new PromptTemplate({
  template: `You are a SQLite expert. Given an input question, first create a syntactically correct SQLite query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most {top_k} results using the LIMIT clause as per SQLite. You can order the results to return the most informative data in the database.
Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Note: You generate the sql not need to include CTE.

Use the following format:
1.Use strict JSON format: {{"sql": "","message": ""}}, not need to include any other text.
2.Format SQL with line breaks/indentation while retaining valid JSON string syntax
3.The message field must include detailed reasons if SQL cannot be generated
4. If exist history messages, you should consider the history messages when generating the response.

Example:
{{
  "sql": "select count(*) from users",
  "message": "Statistic the total number of users"
}}

Only use the following tables:
{table_info}

Question: {question}`,
  inputVariables: ['table_info', 'question', 'top_k'],
});

export const sqlGeneratorSchema = z.object({
  sql: z.string(),
  message: z.string(),
});

export const getSQLGeneratorPrompt = (driverClient: DriverClient) => {
  switch (driverClient) {
    case DriverClient.Pg:
      return SQL_POSTGRES_PROMPT;
    case DriverClient.Sqlite:
      return SQL_SQLITE_PROMPT;
    default:
      throw new Error(`Unsupported dialect: ${driverClient}`);
  }
};
