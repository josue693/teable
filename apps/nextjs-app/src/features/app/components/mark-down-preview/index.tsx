import { cn } from '@teable/ui-lib/shadcn';
import Markdown from 'react-markdown';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/cjs/styles/prism';
import rehypeRaw from 'rehype-raw';
import remarkGfm from 'remark-gfm';

export const MarkdownPreview = (props: { children?: string; className?: string }) => {
  return (
    <Markdown
      className={cn(
        'markdown-body !bg-background px-3 py-2 !text-sm !text-foreground',
        props.className
      )}
      rehypePlugins={[rehypeRaw]}
      remarkPlugins={[remarkGfm]}
      components={{
        code(props) {
          const { children, className, node, ...rest } = props;
          const match = /language-(\w+)/.exec(className || '');
          return match ? (
            // eslint-disable-next-line prettier/prettier, @typescript-eslint/no-explicit-any
            <SyntaxHighlighter {...rest as any} PreTag="div" language={match[1]} style={oneDark}>
              {String(children).replace(/\n$/, '')}
            </SyntaxHighlighter>
          ) : (
            <code {...rest} className={className}>
              {children}
            </code>
          );
        },
      }}
    >
      {props.children}
    </Markdown>
  );
};
