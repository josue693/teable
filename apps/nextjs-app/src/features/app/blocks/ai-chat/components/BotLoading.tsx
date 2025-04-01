export const BotLoading = () => {
  return (
    <div className="flex-1 overflow-hidden">
      <div className="rounded-2xl bg-muted px-4 py-3 shadow-sm">
        <div className="flex items-center gap-2">
          <div
            className="size-2 animate-bounce rounded-full bg-primary/60"
            style={{ animationDelay: '0ms' }}
          ></div>
          <div
            className="size-2 animate-bounce rounded-full bg-primary/60"
            style={{ animationDelay: '150ms' }}
          ></div>
          <div
            className="size-2 animate-bounce rounded-full bg-primary/60"
            style={{ animationDelay: '300ms' }}
          ></div>
        </div>
      </div>
    </div>
  );
};
