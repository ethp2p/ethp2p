import type { ReactNode, HTMLAttributes } from 'react'

export function Chip({ children, ...rest }: { children: ReactNode } & HTMLAttributes<HTMLSpanElement>) {
  return (
    <span
      {...rest}
      className="inline-flex items-center rounded-full border border-neutral-200 px-1.5 text-[0.6875rem] leading-4 text-neutral-500 tabular-nums"
    >
      {children}
    </span>
  )
}
