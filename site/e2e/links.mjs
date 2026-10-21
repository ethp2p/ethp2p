// Link walk: serves the built site, visits every page reachable from the map,
// and fails on any internal link that doesn't land on a page or any #anchor
// with no matching element. Also checks the map tree: progress chips and
// collapsing a group. Run after `pnpm build`.
//
// Pages mark what they rendered with data-page on #root ("map", "spec",
// "not-found"); the walk reads that instead of guessing from content.
//
// Writes e2e-report/links.json: every page visited and every link checked.

import { spawn } from 'node:child_process'
import { mkdir, writeFile } from 'node:fs/promises'
import { createServer } from 'node:net'
import { chromium } from 'playwright'

const freePort = () => new Promise((resolve, reject) => {
  const srv = createServer()
  srv.listen(0, '127.0.0.1', () => {
    const { port } = srv.address()
    srv.close(() => resolve(port))
  })
  srv.on('error', reject)
})

const port = await freePort()
const base = `http://127.0.0.1:${port}`

// Own port and --strictPort, so the walk never talks to a server it didn't start.
const server = spawn('node_modules/.bin/vite', ['preview', '--host', '127.0.0.1', '--port', String(port), '--strictPort'], {
  stdio: ['ignore', 'pipe', 'inherit'],
})
await new Promise((resolve, reject) => {
  server.stdout.on('data', d => { if (String(d).includes(String(port))) resolve() })
  server.on('exit', code => reject(new Error(`vite preview exited with ${code}`)))
})

const browser = await chromium.launch()
const page = await browser.newPage()
const errors = []
page.on('pageerror', e => errors.push(`${page.url()}: ${e.message}`))

try {
  // Visit each page once: what it rendered, its element ids, its links.
  const pages = new Map() // path → { kind, ids: Set, hrefs: [] }
  const queue = ['/']
  while (queue.length) {
    const path = queue.shift()
    if (pages.has(path)) continue
    await page.goto(base + path)
    await page.waitForSelector('#root[data-page]')
    const seen = await page.evaluate(() => ({
      kind: document.getElementById('root').dataset.page,
      ids: [...document.querySelectorAll('[id]')].map(el => el.id),
      hrefs: [...new Set([...document.querySelectorAll('a[href]')].map(a => a.getAttribute('href')))],
    }))
    pages.set(path, { ...seen, ids: new Set(seen.ids) })
    if (seen.kind === 'not-found') continue
    for (const href of seen.hrefs) {
      if (/^[a-z]+:/.test(href)) continue // external
      queue.push(new URL(href, base + path).pathname)
    }
  }

  // Map tree: every group's progress chip agrees with its leaves' statuses,
  // and collapsing a group hides its leaves until it is expanded again.
  // Rows carry data-node ("group" | "leaf") and data-status for this.
  const treeFailures = []
  await page.goto(base + '/')
  await page.waitForSelector('#root[data-page="map"]')
  const groupsOnMap = await page.$$eval('[data-node="group"]', gs => gs.map(g => ({
    name: g.dataset.name,
    chip: g.querySelector('[data-chip]')?.textContent ?? '',
    leaves: [...g.querySelectorAll('[data-node="leaf"]')].map(l => l.dataset.status),
  })))
  if (groupsOnMap.length === 0) treeFailures.push('no groups on the map')
  for (const g of groupsOnMap) {
    const want = `${g.leaves.filter(s => s === 'implemented').length}/${g.leaves.length} built`
    if (g.chip !== want) treeFailures.push(`${g.name}: chip "${g.chip}", leaves say "${want}"`)
  }
  const first = groupsOnMap[0]?.name
  if (first) {
    const leaves = `[data-node="group"][data-name="${first}"] [data-node="leaf"]`
    const trigger = page.locator(`[data-node="group"][data-name="${first}"] [data-trigger]`)
    const visible = async () => (await page.locator(leaves).filter({ visible: true }).count())
    // A toggle settles within a frame or two; 2s only bounds a broken one.
    const settle = fn => page.waitForFunction(fn, leaves, { timeout: 2000 }).catch(() => {})
    const before = await visible()
    await trigger.click()
    await settle(sel => [...document.querySelectorAll(sel)].every(el => !el.checkVisibility()))
    const collapsed = await visible()
    await trigger.click()
    await settle(sel => [...document.querySelectorAll(sel)].some(el => el.checkVisibility()))
    const expanded = await visible()
    if (!(before > 0 && collapsed === 0 && expanded === before)) {
      treeFailures.push(`${first}: visible leaves ${before} → collapse ${collapsed} → expand ${expanded}`)
    }
  }

  const checked = []
  for (const [from, { hrefs, kind }] of pages) {
    if (kind === 'not-found') continue
    for (const href of hrefs) {
      if (/^[a-z]+:/.test(href)) continue
      const url = new URL(href, base + from)
      const target = pages.get(url.pathname)
      const id = decodeURIComponent(url.hash.slice(1))
      let reason = ''
      if (target.kind === 'not-found') reason = `no page at ${url.pathname}`
      else if (id && !target.ids.has(id)) reason = `no element with id "${id}" on ${url.pathname}`
      checked.push({ from, href, ok: !reason, reason })
    }
  }
  const failures = checked.filter(c => !c.ok)
  const kinds = Object.fromEntries([...pages].map(([p, v]) => [p, v.kind]))
  const specPages = Object.values(kinds).filter(k => k === 'spec').length

  await mkdir('e2e-report', { recursive: true })
  await writeFile('e2e-report/links.json', JSON.stringify({ base, pages: kinds, specPages, linksChecked: checked.length, failures, groupsOnMap, treeFailures, pageErrors: errors, checked }, null, 2))

  console.log(`pages ${pages.size} (spec ${specPages}), links checked ${checked.length}, failures ${failures.length}, page errors ${errors.length}`)
  console.log(`map groups ${groupsOnMap.length}, tree failures ${treeFailures.length}`)
  for (const t of treeFailures) console.log(`FAIL tree ${t}`)
  for (const f of failures) console.log(`FAIL ${f.from} → ${f.href}: ${f.reason}`)
  for (const e of errors) console.log(`PAGE ERROR ${e}`)
  if (specPages === 0) console.log('FAIL no spec pages reached from the map')
  process.exitCode = failures.length || treeFailures.length || errors.length || specPages === 0 ? 1 : 0
} finally {
  await browser.close()
  server.kill()
}
