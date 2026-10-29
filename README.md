# Durable Agent Workflows Lab

A **working local multi-stage agent orchestration engine** that checkpoints work to SQLite, supports restart/recovery, enforces a review gate on higher-severity cases, and records an ordered audit trail. The specialist stages are deliberately deterministic so that persistence, idempotency and concurrency can be tested without API keys or hosted LLMs.

## Why this project exists

An agent workflow is more than a prompt chained to tools. Real systems need explicit states, bounded steps, resumability, safe handoffs, review gates, and a record of what happened when a worker dies mid-step. This lab focuses on **those orchestration fundamentals**, rather than recreating the separate agent-triage and ADK repositories.

## Execution path

```text
start -> classify -> enrich -> plan -> review ----------------> finalize -> completed
                                      |              ^
                                      +-- P1/P2 --> human approval
                                                     | reject -> rejected
                               P3 = automatic low-risk review
```

- **Specialist roles:** intake classification, evidence enrichment, evidence-grounded planning, review, and report generation.
- **Atomic checkpoints:** each step's state and its event commit in the *same SQLite transaction* (`BEGIN IMMEDIATE`).
- **Recoverability:** reopen the DB and resume from the recorded stage. A simulated crash before commit leaves no partial step/event.
- **Local concurrency:** DB writer serialization prevents two local workers from writing a checkpoint simultaneously; concurrent advances may execute **different successive stages** and are not a claim about distributed workers.
- **Safety:** high-severity drafts require a review decision; the engine never calls external tools or publishes to real systems.
- **Evidence:** plan hypotheses require a log plus a metric or configuration observation. Otherwise the result is explicitly `unconfirmed`.

## Run (Python 3.10+, standard library only)

```bash
python -m durable_workflows demo
python -m durable_workflows --db lab.sqlite3 start run-001 --case urgent
python -m durable_workflows --db lab.sqlite3 advance run-001
python -m durable_workflows --db lab.sqlite3 advance run-001
python -m durable_workflows --db lab.sqlite3 advance run-001
python -m durable_workflows --db lab.sqlite3 advance run-001  # pauses
python -m durable_workflows --db lab.sqlite3 decide run-001 --actor demo-reviewer --approve yes
python -m durable_workflows --db lab.sqlite3 advance run-001
python -m durable_workflows --db lab.sqlite3 events run-001
python -m unittest discover -s tests -v
```

Three fabricated scenarios are in `fixtures/cases.json`: urgent rate limiting, routine OAuth scope mismatch, and insufficient evidence. Run `python -m durable_workflows demo` for a fully isolated, end-to-end walkthrough with no persistent files.

## What it demonstrates / doesn't

| Demonstrated | Out of scope |
| --- | --- |
| SQLite transactions, event history, resumable checkpoints | Distributed exactly-once side effects |
| Five bounded specialist stages | LLM-generated plans or Google ADK |
| Explicit approval transitions | Real reviewer identity verification |
| Deterministic evidence-grounded suggestions | Live customer incidents or remediation |
| Synthetic concurrency and crash tests | Kubernetes deployment or production HA |

**Security boundary:** The actor passed to `decide` is a *label*, not an authenticated principal. Do not expose this interface as a real approval API. All inputs are fictional; don't submit real customer data. A production version needs worker leasing, independent policy enforcement, signed approvals, secret management, telemetry and a transactional outbox / idempotent external consumers.

## Future extensions

Integrate an LLM or Google ADK adapter behind specialist interfaces (with tool input validation), an outbox with reliable delivery, a queue-based worker pool, OpenTelemetry traces, and authenticated approval APIs.

MIT licensed. See the [tests](tests/) for transaction rollback, reloading checkpoints, role transitions, evidence requirements and concurrency.
