// The site's model of ../specs: a numbered folder is a group, a numbered .md
// file in it is a page. Front matter and titles are parsed and checked at
// build time by the ethp2p-specs plugin in vite.config.ts; links between specs
// are checked below when the page loads (the e2e walk reports a failure).

export type Status = 'implemented' | 'proposed' | 'idea'

export type Spec = {
  route: string      // /transport/quic-and-tls
  group: string      // transport
  title: string      // the first "# " heading
  status: Status
  requires: string[] // routes of the specs it builds on
  body: string       // markdown without front matter and title
  file: string       // 01-transport/01-quic-and-tls.md, for resolving relative links
}

export type Group = { slug: string; specs: Spec[] }

type Parsed = { status: Status; requires: string[]; title: string; body: string }

// Eager: the map shows every title and status on first paint, so the whole
// tree is in the bundle.
const parsed = import.meta.glob('../../specs/[0-9]*/[0-9]*.md', {
  query: '?spec',
  import: 'default',
  eager: true,
}) as Record<string, Parsed>

const unnumber = (s: string) => s.replace(/^\d+-/, '').replace(/\.md$/, '')

// Maps a link written in a spec (relative to that spec's file) to a site route,
// keeping any #anchor. Links that don't point at a .md file pass through.
export function route(fromFile: string, href: string): string {
  if (/^[a-z]+:/.test(href) || !/\.md(#|$)/.test(href)) return href
  const url = new URL(href, `file:///${fromFile}`)
  const [dir, name] = url.pathname.split('/').slice(-2)
  return `/${unnumber(dir)}/${unnumber(name)}${url.hash}`
}

function load(path: string, p: Parsed): Spec {
  const file = path.split('/').slice(-2).join('/')
  return {
    ...p,
    route: route(file, file.split('/')[1]),
    group: unnumber(file.split('/')[0]),
    requires: p.requires.map(r => route(file, r)),
    file,
  }
}

// Folders and files are numbered, so path order is the tree's order.
export const specs: Spec[] = Object.entries(parsed)
  .sort(([a], [b]) => a.localeCompare(b))
  .map(([path, p]) => load(path, p))

export const groups: Group[] = [...new Set(specs.map(s => s.group))]
  .map(slug => ({ slug, specs: specs.filter(s => s.group === slug) }))

export const byRoute = new Map(specs.map(s => [s.route, s]))

for (const s of specs) {
  for (const r of s.requires) {
    if (!byRoute.has(r)) throw new Error(`${s.file}: requires ${r}, which is not a spec`)
  }
}
