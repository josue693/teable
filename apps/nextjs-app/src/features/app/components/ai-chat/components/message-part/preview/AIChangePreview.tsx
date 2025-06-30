import { McpToolInvocationName } from '@teable/openapi';
import { AnchorContext, FieldProvider, TablePermissionProvider } from '@teable/sdk/context';
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
      case McpToolInvocationName.DeleteTable:
      case McpToolInvocationName.UpdateTableName:
      case McpToolInvocationName.CreateTable: {
        return <TableListDiffPreview toolInvocation={toolInvocation} />;
      }
      case McpToolInvocationName.DeleteView:
      case McpToolInvocationName.UpdateViewName:
      case McpToolInvocationName.CreateView: {
        return <ViewListDiffPreview toolInvocation={toolInvocation} />;
      }
      default: {
        return (
          <div className="relative h-48">
            <GridPreView rowCount={3} toolInvocation={toolInvocation} />
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
