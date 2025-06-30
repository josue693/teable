import type { IViewOptions } from '@teable/core';
import { generateViewId, ViewType } from '@teable/core';
import { Lock } from '@teable/icons';
import { McpToolInvocationName } from '@teable/openapi';
import { hexToRGBA } from '@teable/sdk/components';
import { VIEW_ICON_MAP } from '@teable/sdk/components/view/constant';
import { useViews } from '@teable/sdk/hooks';
import Image from 'next/image';
import { Fragment, useEffect, useMemo, useRef } from 'react';
import type { IToolMessagePart } from '../ToolMessagePart';
import { PreviewActionColorMap } from './constant';

interface IViewListPreviewProps {
  toolInvocation: IToolMessagePart['part']['toolInvocation'];
}

export const ViewListDiffPreview = (props: IViewListPreviewProps) => {
  const { toolInvocation } = props;
  const views = useViews();

  const changeViewIdId = useMemo(() => {
    switch (toolInvocation.toolName) {
      case McpToolInvocationName.CreateView: {
        return generateViewId();
      }
      case McpToolInvocationName.UpdateViewName:
      case McpToolInvocationName.DeleteView: {
        const { viewId } = toolInvocation.args;
        return viewId;
      }
      default: {
        return null;
      }
    }
  }, [toolInvocation.args, toolInvocation.toolName]);

  const viewList = useMemo<
    {
      id: string;
      name: string;
      type: ViewType;
      isLocked?: boolean;
      options?: IViewOptions;
      style?: React.CSSProperties;
    }[]
  >(() => {
    switch (toolInvocation.toolName) {
      case McpToolInvocationName.CreateView: {
        const { viewRo } = toolInvocation.args;
        const newTables = views.map((view) => {
          return {
            id: view.id,
            name: view.name,
            type: view.type,
            isLocked: view.isLocked,
            options: view.options,
          };
        });
        newTables.push({
          id: changeViewIdId,
          style: {
            backgroundColor: hexToRGBA(PreviewActionColorMap['create'], 0.5),
            borderColor: PreviewActionColorMap['create'],
          },
          ...viewRo,
        });

        return newTables;
      }
      case McpToolInvocationName.UpdateViewName: {
        const { viewId, updateViewNameRo } = toolInvocation.args;
        const { name: newName } = updateViewNameRo;
        return views.map((view) => ({
          id: view.id,
          name: viewId === view.id ? newName : view.name,
          type: view.type,
          isLocked: view.isLocked,
          options: view.options,
          style:
            viewId === view.id
              ? {
                  backgroundColor: hexToRGBA(PreviewActionColorMap['update'], 0.5),
                  borderColor: hexToRGBA(PreviewActionColorMap['update'], 0.5),
                }
              : undefined,
        }));
      }
      case McpToolInvocationName.DeleteView: {
        return views.map((view) => {
          const style =
            changeViewIdId === view.id
              ? {
                  backgroundColor: hexToRGBA(PreviewActionColorMap['delete'], 0.5),
                  borderColor: hexToRGBA(PreviewActionColorMap['delete'], 0.5),
                }
              : undefined;
          return {
            id: view.id,
            name: view.name,
            type: view.type,
            isLocked: view.isLocked,
            options: view.options,
            style,
          };
        });
      }
      default: {
        return views.map((view) => ({
          id: view.id,
          name: view.name,
          type: view.type,
          isLocked: view.isLocked,
          options: view.options,
        }));
      }
    }
  }, [toolInvocation.toolName, toolInvocation.args, views, changeViewIdId]);

  const ViewItem = (props: {
    id: string;
    name: string;
    type: ViewType;
    isLocked?: boolean;
    options?: IViewOptions;
    style?: React.CSSProperties;
  }) => {
    const { id, name, type, isLocked, style, options } = props;
    const ref = useRef<HTMLDivElement>(null);
    useEffect(() => {
      if (!changeViewIdId) {
        return;
      }
      if (ref.current && id === changeViewIdId) {
        ref.current?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }, [id]);
    const ViewIcon = VIEW_ICON_MAP[type];

    return (
      <div
        style={style}
        className={
          'flex h-7 min-w-20 shrink-0 items-center gap-2 overflow-hidden rounded border p-1 text-foreground'
        }
        ref={ref}
      >
        {type === ViewType.Plugin ? (
          <Image
            className="mr-1 size-4 shrink-0"
            width={16}
            height={16}
            src={(options as IViewOptions & { pluginLogo: string })?.pluginLogo}
            alt={name}
          />
        ) : (
          <Fragment>
            {isLocked && <Lock className="mr-[2px] size-4 shrink-0" />}
            <ViewIcon className="mr-1 size-4 shrink-0" />
          </Fragment>
        )}
        <div className="flex flex-1 items-center justify-center overflow-hidden">
          <div className="truncate text-xs font-medium leading-5">{name}</div>
        </div>
      </div>
    );
  };

  return (
    <div className="flex gap-2 overflow-x-auto p-2">
      {viewList.map((view) => (
        <ViewItem key={view.id} {...view} />
      ))}
    </div>
  );
};
