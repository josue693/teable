import { generateTableId } from '@teable/core';
import { Table2 } from '@teable/icons';
import { McpToolInvocationName } from '@teable/openapi';
import { hexToRGBA } from '@teable/sdk/components';
import { useTables } from '@teable/sdk/hooks';
import { useEffect, useMemo, useRef } from 'react';
import { Emoji } from '@/features/app/components/emoji/Emoji';
import type { IToolMessagePart } from '../ToolMessagePart';
import { PreviewActionColorMap } from './constant';

interface ITableListPreviewProps {
  toolInvocation: IToolMessagePart['part']['toolInvocation'];
}

export const TableListDiffPreview = (props: ITableListPreviewProps) => {
  const { toolInvocation } = props;
  const tables = useTables();

  const changeTableId = useMemo(() => {
    switch (toolInvocation.toolName) {
      case McpToolInvocationName.CreateTable: {
        return generateTableId();
      }
      case McpToolInvocationName.UpdateTableName:
      case McpToolInvocationName.DeleteTable: {
        const { tableId } = toolInvocation.args;
        return tableId;
      }
      default: {
        return null;
      }
    }
  }, [toolInvocation.args, toolInvocation.toolName]);

  const tableList = useMemo<
    {
      id: string;
      name: string;
      icon: string | undefined;
      style?: React.CSSProperties;
    }[]
  >(() => {
    switch (toolInvocation.toolName) {
      case McpToolInvocationName.CreateTable: {
        const { tableRo } = toolInvocation.args;
        const newTables = tables.map((table) => {
          return {
            id: table.id,
            name: table.name,
            icon: table.icon,
          };
        });
        newTables.push({
          id: changeTableId,
          style: {
            backgroundColor: hexToRGBA(PreviewActionColorMap['create'], 0.5),
            borderColor: PreviewActionColorMap['create'],
          },
          ...tableRo,
        });

        return newTables;
      }
      case McpToolInvocationName.UpdateTableName: {
        const { tableId, updateTableNameRo } = toolInvocation.args;
        const { name: newName } = updateTableNameRo;
        return tables.map((table) => ({
          id: table.id,
          name: tableId === table.id ? newName : table.name,
          icon: table.icon,
          style:
            tableId === table.id
              ? {
                  backgroundColor: hexToRGBA(PreviewActionColorMap['update'], 0.5),
                  borderColor: PreviewActionColorMap['update'],
                }
              : undefined,
        }));
      }
      case McpToolInvocationName.DeleteTable: {
        const { tableId } = toolInvocation.args;
        return tables.map((table) => {
          return {
            id: table.id,
            name: table.name,
            icon: table.icon,
            style:
              tableId === table.id
                ? {
                    backgroundColor: hexToRGBA(PreviewActionColorMap['delete'], 0.5),
                    borderColor: hexToRGBA(PreviewActionColorMap['delete'], 0.5),
                  }
                : undefined,
          };
        });
      }
      default: {
        return tables.map((table) => ({ id: table.id, name: table.name, icon: table.icon }));
      }
    }
  }, [changeTableId, tables, toolInvocation.args, toolInvocation.toolName]);

  const TableItem = (props: {
    id: string;
    name: string;
    icon: string | undefined;
    style?: React.CSSProperties;
  }) => {
    const { id, name, icon, style } = props;
    const ref = useRef<HTMLDivElement>(null);
    useEffect(() => {
      if (!changeTableId) {
        return;
      }
      if (ref.current && id === changeTableId) {
        ref.current?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }, [id]);
    return (
      <div
        style={style}
        className={'flex h-7 items-center gap-2 rounded border p-1 px-2 text-foreground'}
        ref={ref}
      >
        {icon ? (
          <Emoji emoji={icon} size={'1rem'} className="size-4 shrink-0" />
        ) : (
          <Table2 className="size-4 shrink-0" />
        )}
        <p className="grow truncate text-sm">{' ' + name}</p>
      </div>
    );
  };

  return (
    <div className="flex max-h-48 max-w-48 flex-col gap-2 overflow-y-auto">
      {tableList.map((table) => (
        <TableItem key={table.id} {...table} />
      ))}
    </div>
  );
};
