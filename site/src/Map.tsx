import { Collapsible } from '@base-ui/react/collapsible'
import { groups, type Group } from './specs'
import { StatusDot, rollup } from './Status'
import { Chip } from './Chip'

// Groups listed here go in the right-hand column, as trees of their own; the
// rest hang off the ethp2p trunk, top to bottom.
const ASIDE = ['adoption', 'roadmap']

// data-node, data-name, data-status, data-chip and data-trigger are read by
// the e2e walk (e2e/links.mjs).
function GroupNode({ group }: { group: Group }) {
  const statuses = group.specs.map(s => s.status)
  const built = statuses.filter(s => s === 'implemented').length
  return (
    <Collapsible.Root defaultOpen render={<li className="tree-group" data-node="group" data-name={group.slug} />}>
      <div className="flex items-center gap-2 leading-7">
        <StatusDot status={rollup(statuses)} />
        <Collapsible.Trigger data-trigger className="group flex cursor-pointer items-center gap-1 text-group font-medium">
          {group.slug}
          <svg viewBox="0 0 8 8" className="size-2 text-neutral-400 transition-transform group-data-panel-open:rotate-90" aria-hidden>
            <path d="M2.5 1 5.5 4 2.5 7" fill="none" stroke="currentColor" strokeWidth="1.2" />
          </svg>
        </Collapsible.Trigger>
        <Chip data-chip>{`${built}/${statuses.length} built`}</Chip>
      </div>
      <Collapsible.Panel hiddenUntilFound render={<ul className="tree-children" />}>
        {group.specs.map(s => (
          <li key={s.route} className="tree-leaf" data-node="leaf" data-status={s.status}>
            <div className="flex items-center gap-2 text-leaf leading-6">
              <StatusDot status={s.status} />
              <a href={s.route} className="text-neutral-600 hover:text-neutral-950 hover:underline">{s.title}</a>
            </div>
          </li>
        ))}
      </Collapsible.Panel>
    </Collapsible.Root>
  )
}

export function Map() {
  return (
    <main className="px-6 py-10 md:px-12">
      <h1 className="font-display text-wordmark italic leading-none">ethp2p</h1>
      <div className="grid gap-x-16 gap-y-8 md:grid-cols-[24rem_1fr]">
        <ul className="tree tree-trunk">
          {groups.filter(g => !ASIDE.includes(g.slug)).map(g => <GroupNode key={g.slug} group={g} />)}
        </ul>
        <ul className="tree tree-detached">
          {groups.filter(g => ASIDE.includes(g.slug)).map(g => <GroupNode key={g.slug} group={g} />)}
        </ul>
      </div>
    </main>
  )
}
