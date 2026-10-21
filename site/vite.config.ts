import { readFile } from 'node:fs/promises'
import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import tailwind from '@tailwindcss/vite'
import { parse } from 'yaml'

const STATUSES = ['implemented', 'proposed', 'idea']

// `import x from 'spec.md?spec'` yields { status, requires, title, body }.
// Parsing here keeps the YAML parser out of the browser bundle, and a spec
// with bad front matter or no title fails the build instead of a page load.
function specs(): Plugin {
  return {
    name: 'ethp2p-specs',
    async load(id) {
      if (!id.endsWith('.md?spec')) return
      const file = id.slice(0, -'?spec'.length)
      const text = await readFile(file, 'utf8')
      const m = text.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/)
      if (!m) throw new Error(`${file}: no front matter`)
      const meta = parse(m[1]) as { status?: string; requires?: string[] | null }
      if (!STATUSES.includes(meta.status ?? '')) throw new Error(`${file}: status ${meta.status}`)
      const title = m[2].match(/^# (.+)$/m)
      if (!title) throw new Error(`${file}: no title`)
      const spec = {
        status: meta.status,
        requires: meta.requires ?? [],
        title: title[1],
        body: m[2].replace(/^# .+\n/m, ''),
      }
      return `export default ${JSON.stringify(spec)}`
    },
  }
}

export default defineConfig({
  plugins: [specs(), react(), tailwind()],
  // The specs live in ../specs, outside the site root; the dev server refuses
  // to serve files outside its root without this.
  server: { fs: { allow: ['..'] } },
})
