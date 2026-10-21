import { Tooltip } from '@base-ui/react/tooltip'
import type { Status as SpecStatus } from './specs'

// A spec's status, plus "partial" for a group whose specs are only partly built.
export type DotStatus = SpecStatus | 'partial'

const LABEL: Record<DotStatus, string> = {
  implemented: 'Implemented',
  partial: 'Partly implemented',
  proposed: 'Proposed, not implemented',
  idea: 'Idea, not yet specified',
}

const LOOK: Record<DotStatus, string> = {
  implemented: 'bg-emerald-600 border-emerald-600',
  // Left half filled: built on one side, not on the other.
  partial: 'border-emerald-600 bg-[linear-gradient(90deg,var(--color-emerald-600)_50%,transparent_50%)]',
  proposed: 'border-amber-500',
  idea: 'border-neutral-400 border-dashed',
}

// A group's dot: built if every spec is, partial if some are, otherwise the
// furthest any spec has got (proposed beats idea).
export function rollup(statuses: SpecStatus[]): DotStatus {
  const built = statuses.filter(s => s === 'implemented').length
  if (built === statuses.length) return 'implemented'
  if (built > 0) return 'partial'
  return statuses.includes('proposed') ? 'proposed' : 'idea'
}

export function StatusDot({ status }: { status: DotStatus }) {
  return (
    <Tooltip.Root>
      <Tooltip.Trigger
        aria-label={LABEL[status]}
        className={`inline-block size-[7px] shrink-0 cursor-default rounded-full border ${LOOK[status]}`}
      />
      <Tooltip.Portal>
        <Tooltip.Positioner sideOffset={6}>
          <Tooltip.Popup className="rounded bg-neutral-900 px-2 py-1 text-xs text-white">{LABEL[status]}</Tooltip.Popup>
        </Tooltip.Positioner>
      </Tooltip.Portal>
    </Tooltip.Root>
  )
}
