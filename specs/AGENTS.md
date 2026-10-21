# Writing specs

Rules particular to the specs in this tree.
What belongs in a spec rather than in Go docs, the layout of a spec, its voice, and the reference implementation's package docs are in the `spec-writing` skill (`.claude/skills/spec-writing`); this file adds what is specific to `specs`.
When both are silent, match `01-transport/02-streams.md`.
The meaning of MUST and SHOULD and the notation are in [Conventions](00-foundations/02-conventions.md).
Defined terms are in [Terminology](00-foundations/03-terminology.md).

## How to work

- Do everything the user asked for, then stop and report.
  Stop earlier only when you cannot go on without the user, or before a risky step.
- Do not add specs, sections, files, tooling or refactors that were not asked for.
  If one would help, name it at the end of your report.
- Before editing a spec, read the code and tests it describes, and the specs it links to.
  Check specifics from the source, not from memory: field numbers, limits, deadlines, codes.
- Before reporting a change done, run `rumdl check specs` and confirm every link and anchor resolves.
  If a byte example changed, encode it with the real code.
  Say which checks you did not run and why.
- When a spec and the code disagree, do not pick a side.
  Report the mismatch with a recommendation; the user decides whether the code or the spec changes.
- Do not review your own work in extra rounds or start reviewer agents unless asked.

## The tree

- Each spec covers one concept or topic, small and self-contained, described as the code is today.
  Split a spec that covers two topics.
- Unbuilt work is recorded in the `status` front matter, and nowhere else: `proposed` for a specified design, `idea` for one that is not.
  An idea for an existing area goes in `09-roadmap/`: the problem, why it waits, related specs.
  A new area without a spec gets its own numbered folder with a stub of status `idea`.
  Roadmap files and stubs do not follow the spec layout.
- Folders are numbered in stack order (`00-foundations/` up to `05-control/`, then `06-adoption/`), and files within a folder in reading order (`03-connection-lifecycle.md`).
  Place a new topic where a reader needs it.
- The site in `site/` renders this tree: a folder is a group named after it without its number, a numbered file is a page titled by its first heading.
- Every assigned name and number (application protocol names, protocol ids, kinds, frame types) is listed in [Registries](00-foundations/04-registries.md).
  A spec that assigns one adds it there.
- A protocol's spec opens its Wire format with a `### Stream` section: the stream grammar and a table of its frame types.
  Then one `###` section per frame type, in type order, then `### States`, then codes and constants where it has them, then `### Examples`.
  The [chunk kind](03-broadcast/02-broadcast-protocol.md#chunk-kind) of the broadcast protocol shows the layout.
- A term is defined once, in [Terminology](00-foundations/03-terminology.md).
  A spec links to the page for a term and adds an entry for each new term it introduces.
  Say what a term means inline only when the sentence needs it.
- Every numbered file has front matter with `status` (`implemented`, `proposed` or `idea`) and `requires` (the specs it depends on).

## Notation

- Write byte layouts in the notation of [Conventions](00-foundations/02-conventions.md) and messages in proto3, copied from the repo; do not invent fields.
- Name each constant in UPPER_SNAKE_CASE after the code's constant where one exists (`maxStreamsPendingClassify` is `MAX_STREAMS_PENDING_CLASSIFY`).
  A rule cites the name with the value in parentheses at its first use in each section.
- Write a byte in prose as `0x2f`.
- Write hex bytes with one annotation per line.
- Name a frame type in capitals and its message in CamelCase: a `HELLO` frame carries a `Hello`.
- Put type names, field names, codes, files and identifiers in backticks in prose: `Hello`, `GoAway`, `ControlViolation`, `Hello.protocols`, `control.go`.
  Leave code blocks, byte layouts and byte-example annotations unfenced.
  Defined concepts (view, selector, kind) are plain words.
- Leave out IETF boilerplate that carries no information here:
  header blocks, status memos, author addresses, acknowledgements, IANA considerations,
  registration procedures, publication notes.

## Headings and links

- Sentence case: "Failure modes", not "Failure Modes".
  Names and types keep their case.
- No numbered headings and no table of contents.
- Link to a heading or file by relative path.
  Do not write "Section 4.6" and do not use bracketed tags such as `[ETHP2P-CONTROL]`.
- External references are inline links, with the URLs listed at the end of the file.

## Formatting

- One sentence per line, with no length limit; editors soft-wrap.
  Run `rumdl fmt specs` instead of reflowing by hand.
- Do not add tooling that detects spec drift.
