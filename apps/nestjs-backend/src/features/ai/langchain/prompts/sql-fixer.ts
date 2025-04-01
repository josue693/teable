import { PromptTemplate } from '@langchain/core/prompts';
import { z } from 'zod';

// eslint-disable-next-line @typescript-eslint/naming-convention
export const SQL_FIXER_PROMPT = new PromptTemplate({
  template: `
You are an {dialect} Validator tasked with fixing invalid AI-generated SQL statements that failed execution. Follow these steps:
1. Analyze the SQL query and the error message.
2. Identify the specific syntax error or issue in the SQL query.
3. Generate a corrected version of the SQL query that adheres to the correct syntax and logic.
4. Return the corrected SQL query in valid JSON format: {{"sql": "", "message": ""}}.
5. No explanatory text
6. Ensure valid JSON syntax
7. The message field must include detailed reasons if SQL cannot be generated.

Input:
Table schemas: {table_schemas}
Question: {question}
SQL Query: {sql}
Error Message: {error}

Example1:
{{
  "sql": "SELECT * FROM users WHERE id = 1",
  "message": "The table name 'users' is not found in the schema. Please check the table name and try again."
}}

Example2:
{{
  "sql": "",
  "message": "Missing table structure info: Unable to validate column existence."
}}
`,
  inputVariables: ['question', 'sql', 'error', 'dialect', 'table_schemas'],
});

export const sqlFixSchema = z.object({
  sql: z.string(),
  message: z.string(),
});
