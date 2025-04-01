import { PromptTemplate } from '@langchain/core/prompts';

// eslint-disable-next-line @typescript-eslint/naming-convention
export const SQL_RESPONSE_PROMPT = new PromptTemplate({
  template: `
#Task Description
Generate a natural language response based on the following three elements:

##Input Content
User Question: {question}
Generated SQL: {sql}
Query Results: {sql_result}

##Output Requirements
Friendly Tone: Use conversational language. Begin with a greeting (e.g., "Hello!").
Natural Flow: Restate the key points of the question first, then explain the findings.
Structured Presentation:
- For single-value results (e.g., statistics): "Based on your query, [specific metric] is [value][unit]."
- For list results: "Found [count] records in total, including:"
- For multi-field results: Describe relationships between key fields in natural language.
- Expert Tips: Include explanatory phrases (e.g., "This result indicates...").
- Closing Prompt: Add a helpful closing line (e.g., "Let me know if you need further analysis!")
- You can only analysis the data, you don't have the ability to add, modify or delete data
- Readability: Use Markdown formatting (e.g., bullet points, bolding) to enhance clarity.

##Notes
- Automatically add thousand separators for numbers over three digits (e.g., 356,800).
- Avoid technical jargon.
- Highlight anomalies if detected (e.g., "Note that...").
- If exist history messages, you should consider the history messages when generating the response.
`,
  inputVariables: ['question', 'sql', 'sql_result'],
});
