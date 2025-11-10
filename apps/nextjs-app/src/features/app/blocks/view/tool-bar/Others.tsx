import { ArrowUpRight, Code2, MoreHorizontal } from '@teable/icons';
import { useBaseId, useTableId, useTablePermission } from '@teable/sdk/hooks';
import { Button, cn, Popover, PopoverContent, PopoverTrigger } from '@teable/ui-lib/shadcn';
import { SearchButton } from '../search/SearchButton';
import { PersonalViewSwitch } from './components';
import { UndoRedoButtons } from './components/UndoRedoButtons';
import { SharePopover } from './SharePopover';
import { ToolBarButton } from './ToolBarButton';

const OthersList = ({
  classNames,
  className,
  foldButton,
}: {
  classNames?: { textClassName?: string; buttonClassName?: string };
  className?: string;
  foldButton?: boolean;
}) => {
  const permission = useTablePermission();
  const baseId = useBaseId() as string;
  const tableId = useTableId() as string;

  const { textClassName, buttonClassName } = classNames ?? {};

  const onAPIClick = () => {
    const path = `/developer/tool/query-builder`;
    const url = new URL(path, window.location.origin);
    url.searchParams.set('baseId', baseId);
    url.searchParams.set('tableId', tableId);

    window.open(url.toString(), '_blank');
  };

  return (
    <div className={cn('gap-1 flex items-center', className)}>
      <SharePopover>
        {(text, isActive) => (
          <ToolBarButton
            isActive={isActive}
            text={text}
            textClassName={textClassName}
            className={foldButton ? 'w-full justify-start' : buttonClassName}
            disabled={!permission['view|update']}
          >
            <ArrowUpRight className="size-4" />
          </ToolBarButton>
        )}
      </SharePopover>
      <ToolBarButton
        text="API"
        className={foldButton ? 'w-full justify-start' : buttonClassName}
        textClassName={textClassName}
        onClick={onAPIClick}
      >
        <Code2 className="size-4" />
      </ToolBarButton>
      {!foldButton && <div className="mx-1 h-4 w-px shrink-0 bg-border" />}
      <PersonalViewSwitch
        textClassName={textClassName}
        buttonClassName={foldButton ? 'w-full justify-start pl-2' : buttonClassName}
      />
    </div>
  );
};

const OthersMenu = ({ className }: { className?: string }) => {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          variant={'ghost'}
          size={'xs'}
          className={cn('font-normal shrink-0 truncate', className)}
        >
          <MoreHorizontal className="size-4" />
        </Button>
      </PopoverTrigger>
      <PopoverContent side="bottom" align="start" className="w-40 p-1">
        <OthersList
          className="flex flex-col items-start w-full"
          classNames={{ textClassName: 'inline', buttonClassName: 'justify-start rounded-none' }}
          foldButton={true}
        />
      </PopoverContent>
    </Popover>
  );
};

export const Others: React.FC = () => {
  return (
    <div className="flex flex-1 justify-end @container/toolbar-others items-center md:gap-0">
      <SearchButton />
      <UndoRedoButtons />
      <div className="mx-1 h-4 w-px shrink-0 bg-border"></div>
      <OthersList
        className="hidden @md/toolbar:flex"
        classNames={{ textClassName: '@[300px]/toolbar-others:inline' }}
      />
      <OthersMenu className="@md/toolbar:hidden" />
    </div>
  );
};
