import Markdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import rehypeSlug from 'rehype-slug'
import hljs from 'highlight.js/lib/core'
import protobuf from 'highlight.js/lib/languages/protobuf'
import abnf from 'highlight.js/lib/languages/abnf'
import { Collapsible } from '@base-ui/react/collapsible'
import { byRoute, groups, route, type Spec } from './specs'
import { StatusDot } from './Status'

// Only the languages the specs use. `text` fences (byte layouts) stay plain.
// highlight.js core rather than rehype-highlight: the plugin bundles ~37
// languages whatever it is told to register.
hljs.registerLanguage('protobuf', protobuf)
hljs.registerLanguage('abnf', abnf)

function Code({ className, children }: { className?: string; children?: React.ReactNode }) {
  const lang = className?.match(/language-(\w+)/)?.[1]
  if (!lang || !hljs.getLanguage(lang)) return <code className={className}>{children}</code>
  // highlight.js escapes the source text; the HTML it returns is spans only.
  const html = hljs.highlight(String(children).replace(/\n$/, ''), { language: lang }).value
  return <code className={`${className} hljs`} dangerouslySetInnerHTML={{ __html: html }} />
}

export function SpecPage({ spec }: { spec: Spec }) {
  return (
    <div className="grid gap-10 px-6 py-10 md:grid-cols-[14rem_1fr] md:px-12">
      <nav className="text-sm">
        <a href="/" className="font-display text-wordmark italic">ethp2p</a>
        {groups.map(g => (
          <Collapsible.Root key={g.slug} defaultOpen={g.slug === spec.group} className="mt-3">
            <Collapsible.Trigger className="cursor-pointer text-[0.9375rem] font-medium">{g.slug}</Collapsible.Trigger>
            <Collapsible.Panel className="mt-0.5 ml-3 space-y-0.5">
              {g.specs.map(s => (
                <div key={s.route} className="flex items-center gap-2">
                  <StatusDot status={s.status} />
                  <a
                    href={s.route}
                    className={s === spec ? 'font-medium text-neutral-950' : 'text-neutral-600 hover:text-neutral-950'}
                  >
                    {s.title}
                  </a>
                </div>
              ))}
            </Collapsible.Panel>
          </Collapsible.Root>
        ))}
      </nav>
      <article className="typeset max-w-[68ch]">
        <h1 className="flex items-center gap-3">
          {spec.title}
          <StatusDot status={spec.status} />
        </h1>
        {spec.requires.length > 0 && (
          <p className="text-sm text-(--typeset-muted)">
            Builds on{' '}
            {spec.requires.map((r, i) => (
              <span key={r}>
                {i > 0 && ', '}
                <a href={r}>{byRoute.get(r)!.title}</a>
              </span>
            ))}
          </p>
        )}
        <Markdown
          remarkPlugins={[remarkGfm]}
          rehypePlugins={[rehypeSlug]}
          components={{
            a: ({ href = '', node: _, ...p }) => <a href={route(spec.file, href)} {...p} />,
            code: ({ className, children }) => <Code className={className}>{children}</Code>,
            // Registries and frame-type tables are wider than the column: keep
            // their width and scroll instead of squeezing them.
            table: ({ node: _, ...p }) => (
              <div className="typeset-scroll">
                <table {...p} />
              </div>
            ),
          }}
        >
          {spec.body}
        </Markdown>
      </article>
    </div>
  )
}
