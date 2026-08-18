# Hedloom Flow agent guidance

Inherit the project guidance from `../AGENTS.md`. Before work here, read
`../MANIFESTO.md`, `../ONTOLOME.md`, local `ONTOLOME.md`, local `README.md`,
local `unit.toml`, both planning trackers, and `docs/architecture.md`, then
inspect the relevant implementation and tests.

This unit owns executor-neutral operation/flow authoring, early graph
validation, immutable normalized Plan IR, and its tool-free evidence.
Keep execution semantics, scheduling and transport, persistence,
recovery, plugins, the complete study lifecycle, and archived sequential
convenience outside this boundary. Update the local ontology when the unit's
being changes; place a changed cross-unit planning contract in the closest
containing ontology.
## Where to read, and what to trust

| Surface | Where | Maintained against the code? |
| --- | --- | --- |
| Contracts | `ONTOLOME.md` | **Yes** — update it in the same change that alters a contract. |
| Documentation | `docs/index.md`, `docs/architecture.md` (published) | **Yes.** |
| Design record | `PLANNING.md`, `IMPLEMENTATION.md`, `design/` | **No.** Phase records describe what a phase delivered, including contracts that have since changed — schema 2, for instance, where this unit now emits and validates schema 3 only. |

Do not cite a phase tracker or a `design/` file as evidence of current
behaviour, and do not edit one to match the code.
