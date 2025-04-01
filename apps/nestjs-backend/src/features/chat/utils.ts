import { ChatGraphNode } from '@teable/openapi';

export const isResponseNode = (node: ChatGraphNode) =>
  [ChatGraphNode.SqlResponse, ChatGraphNode.NormalResponse].includes(node);
