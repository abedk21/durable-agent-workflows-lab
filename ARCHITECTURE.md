# Design notes

`Engine` stores one row per run and a sequence of immutable event records per run. In a single SQLite transaction, a stage reads the latest checkpoint, computes a deterministic state transition, writes its versioned checkpoint, and writes the matching event. The first event is `started` at sequence 0.

### Consistency boundary

This architecture has **atomic local state and event updates**, not exactly-once delivery to external systems. In a production worker, a network request after a checkpoint can succeed while the worker fails before recording its result. For non-idempotent work, a transactional outbox and an external idempotency contract are required. The current example makes **no external network calls**.

### Human review and trust

A `P1`/`P2` run reaches `awaiting_approval`. `decide(run_id, actor, approved)` moves it to active or rejected. The actor is only a testing label. A production service must authenticate approvers and enforce RBAC before calling this method. The demonstration does not execute or send remediation actions.

### Data safety

Case schema is fixed and bounded, with a tenant field for reporting. It does *not* implement multi-tenant isolation or user authentication. Case identifiers, summaries and evidence are persisted in plaintext SQLite. Use only the fabricated fixtures in this repository.
