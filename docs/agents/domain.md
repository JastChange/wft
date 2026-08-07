# Domain Docs

This repository uses a single-context domain documentation layout.

## Before exploring

Read these files when they exist:

- `CONTEXT.md`
- Relevant ADRs under `docs/adr/`

Proceed silently when they do not exist. Domain-modeling workflows create them
when terminology or architectural decisions are resolved.

## Layout

```text
/
├── CONTEXT.md
├── docs/adr/
└── src/
```

## Vocabulary

Use domain terms defined in `CONTEXT.md`. Avoid introducing conflicting
synonyms. Record genuine terminology gaps for domain modeling.

## ADR conflicts

Explicitly identify proposed work that contradicts an existing ADR rather than
silently overriding the decision.
