import { PromptTemplate } from '@langchain/core/prompts';
import { z } from 'zod';

export enum ChatIntentType {
  SQL = 'sql',
  NORMAL = 'normal',
}

export type IChatIntent = z.infer<typeof chatIntentSchema>;

export const chatIntentSchema = z.object({
  type: z.nativeEnum(ChatIntentType),
  message: z.string(),
});

// eslint-disable-next-line @typescript-eslint/naming-convention
export const CHAT_INTENT_PROMPT = new PromptTemplate({
  template: `
You are an intent classifier that determines the type of user queries. Supported intent types are:

1. SQL Task: Queries requiring database operations, including but not limited to:
- Quantity statistics (e.g., 'How many phones are there?')
- Data queries (e.g., 'What products exist?')
- Attribute filtering (e.g., 'Phones priced over 5000')
- Aggregation calculations (e.g., 'Total orders this month')

2. Normal Conversation: General non-data-related interactions, including:
- Social greetings (e.g., 'Hello')
- Functional inquiries (e.g., 'What can you do?')
- Unstructured requests (e.g., 'Tell me a story')

Requirements:
- Use strict JSON format: {{"type": "sql" | "normal", "message": ""}}
- No explanatory text
- Ensure valid JSON syntax
- message is friendly and helpful

-- Examples:
Input: "What's the total order amount this month?"
Analysis: Involves amount aggregation → SELECT SUM(amount) FROM orders WHERE month='current'
Output: {{"type": "sql", "message": "Sure! I'll help you calculate the total order amount for this month."}}

Input: "Hello, can you help me query data?"
Analysis: Greeting + vague request → lacks concrete SQL features
Output: {{"type": "normal", "message": "Sure! I can help you with that. What data do you want to query?"}}

Determine the intent for:
{question}
  `,
  inputVariables: ['question'],
});
