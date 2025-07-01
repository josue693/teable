import { McpToolInvocationName } from '@teable/openapi';
import {
  AnchorContext,
  FieldProvider,
  RowCountProvider,
  TablePermissionProvider,
} from '@teable/sdk/context';
import { useBaseId } from '@teable/sdk/hooks/use-base-id';
import type { IToolMessagePart } from '../ToolMessagePart';
import { GridPreView } from './GridPreview';
import { TableListDiffPreview } from './TableListDiffPreview';
import { ViewListDiffPreview } from './ViewListDiffPreview';

interface IAIChangePreviewProps {
  toolInvocation: IToolMessagePart['part']['toolInvocation'];
}

export const AIChangePreview = (props: IAIChangePreviewProps) => {
  const { toolInvocation } = props;
  const baseId = useBaseId()!;
  const tableId = toolInvocation.args?.['tableId'];
  const toolName = toolInvocation.toolName;

  const previewRender = () => {
    switch (toolName) {
      case McpToolInvocationName.CreateTable:
      case McpToolInvocationName.DeleteTable:
      case McpToolInvocationName.UpdateTableName: {
        return <TableListDiffPreview toolInvocation={toolInvocation} />;
      }
      case McpToolInvocationName.CreateView:
      case McpToolInvocationName.DeleteView:
      case McpToolInvocationName.UpdateViewName: {
        return <ViewListDiffPreview toolInvocation={toolInvocation} />;
      }
      default: {
        return (
          <div className="relative h-48">
            <RowCountProvider>
              <GridPreView toolInvocation={toolInvocation} />
            </RowCountProvider>
          </div>
        );
      }
    }
  };
  return (
    <>
      <AnchorContext.Provider key={tableId} value={{ baseId, tableId }}>
        <TablePermissionProvider baseId={baseId}>
          <FieldProvider>{previewRender()}</FieldProvider>
        </TablePermissionProvider>
      </AnchorContext.Provider>
    </>
  );
};
