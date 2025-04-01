import type { IChatGraphState } from './chat-graph';

export const stringifyIntentNodeResult = (result: IChatGraphState['indentResult']) => {
  return `
  ${result.message}\n\`\`\`json\n${result.type}\n\`\`\`
  `;
};

export const stringifyFieldSelectorNodeResult = (
  result: IChatGraphState['fieldSelectorResult']
) => {
  return `
  ${result.message}\n\`\`\`json\n${JSON.stringify(result.fields, null, 2)}\n\`\`\`
  `;
};

export const stringifySqlGeneratorNodeResult = (result: IChatGraphState['sqlGeneratorResult']) => {
  return `
  ${result.message}\n\`\`\`sql\n${result.sql}\n \`\`\`
  `;
};

// eslint-disable-next-line sonarjs/no-identical-functions
export const stringifySqlFixerNodeResult = (result: IChatGraphState['sqlFixerResult']) => {
  return `
  ${result.message}\n\`\`\`sql\n${result.sql}\n\`\`\`
  `;
};
