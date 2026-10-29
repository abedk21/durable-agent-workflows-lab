"""Crash-consistent, local multi-step workflow engine for fabricated support cases.

All agent functions are pure: the database transaction atomically saves a checkpoint
and its event. No external tool effects are performed, so this does not claim
exactly-once execution for real APIs.
"""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

STAGES = ("classify", "enrich", "plan", "review", "finalize")
MAX_PAYLOAD_BYTES = 32_768


class WorkflowError(Exception):
    """Invalid input or state transition."""


class ApprovalRequired(WorkflowError):
    """The workflow is paused for a human approval decision."""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _validate_case(case: dict[str, Any]) -> None:
    if not isinstance(case, dict) or set(case) != {"id", "tenant", "severity", "summary", "signals"}:
        raise WorkflowError("Case must have id, tenant, severity, summary and signals only")
    for name in ("id", "tenant"):
        if not isinstance(case[name], str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,60}", case[name]):
            raise WorkflowError(f"Invalid {name}")
    if case["severity"] not in ("P1", "P2", "P3"):
        raise WorkflowError("severity must be P1, P2 or P3")
    if not isinstance(case["summary"], str) or not 4 <= len(case["summary"]) <= 1000:
        raise WorkflowError("Invalid summary")
    if not isinstance(case["signals"], list) or not 1 <= len(case["signals"]) <= 12:
        raise WorkflowError("signals must be a nonempty list of up to 12 observations")
    ids = set()
    for item in case["signals"]:
        if not isinstance(item, dict) or set(item) != {"id", "kind", "detail"}:
            raise WorkflowError("Each signal needs id, kind and detail")
        if (not isinstance(item["id"], str) or not re.fullmatch(r"[A-Z0-9_-]{1,40}", item["id"])
                or item["id"] in ids):
            raise WorkflowError("Signal IDs must be unique uppercase identifiers")
        ids.add(item["id"])
        if item["kind"] not in ("log", "metric", "config"):
            raise WorkflowError("Unsupported evidence kind")
        if not isinstance(item["detail"], str) or len(item["detail"]) > 500:
            raise WorkflowError("Invalid evidence detail")
    if len(_json(case).encode()) > MAX_PAYLOAD_BYTES:
        raise WorkflowError("Case payload too large")


def classify(case: dict[str, Any], _: dict[str, Any]) -> dict[str, Any]:
    return {"queue": "urgent" if case["severity"] == "P1" else "standard",
            "requires_approval": case["severity"] in ("P1", "P2")}


def enrich(case: dict[str, Any], _: dict[str, Any]) -> dict[str, Any]:
    """Read-only specialist agent: select and classify available synthetic evidence."""
    return {"evidence": [{"ref": s["id"], "source": s["kind"], "observation": s["detail"]}
                         for s in case["signals"]]}


def plan(case: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    """Deterministic specialist agent; does not treat observation text as commands."""
    evidence = state["enrich"]["evidence"]
    logs = " ".join(e["observation"].lower() for e in evidence if e["source"] == "log")
    metrics = " ".join(e["observation"].lower() for e in evidence if e["source"] == "metric")
    cfg = " ".join(e["observation"].lower() for e in evidence if e["source"] == "config")
    if ("429" in logs or "resource_exhausted" in logs) and "quota_utilization=" in metrics:
        action = "Inspect per-minute quota utilization and bounded retry policy"
        refs = [e["ref"] for e in evidence if ("429" in e["observation"].lower()
                or "quota_utilization=" in e["observation"].lower())]
        hypothesis = "rate_limit_pressure"
    elif "403" in logs and "required_scope=" in cfg:
        action = "Compare consented OAuth scopes with required connector scopes"
        refs = [e["ref"] for e in evidence if "403" in e["observation"] or "required_scope=" in e["observation"]]
        hypothesis = "oauth_scope_issue"
    else:
        hypothesis, action, refs = "unconfirmed", "Gather additional independent observations", []
    types = {e["source"] for e in evidence if e["ref"] in refs}
    if len(types) < 2:
        hypothesis, action, refs = "unconfirmed", "Gather additional independent observations", []
    return {"hypothesis": hypothesis, "recommendation": action, "evidence_refs": refs,
            "certainty": "corroborated" if refs else "unconfirmed", "external_actions": 0}


def finalize(case: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    proposal = state["plan"]
    assert state["review"]["approved"]
    return {"case_id": case["id"], "tenant": case["tenant"], "severity": case["severity"],
            "summary": case["summary"], "hypothesis": proposal["hypothesis"],
            "evidence_refs": proposal["evidence_refs"], "recommendation": proposal["recommendation"],
            "approved_by": state["review"]["actor"], "sent_to_external_system": False}


@dataclass(frozen=True)
class Snapshot:
    run_id: str
    status: str
    next_stage: str | None
    version: int
    state: dict[str, Any]


class ClosingConnection(sqlite3.Connection):
    """Commit/rollback and explicitly close when leaving a `with` block."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


class Engine:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, status TEXT NOT NULL, stage_index INTEGER NOT NULL,
                    version INTEGER NOT NULL, case_json TEXT NOT NULL, state_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (
                    run_id TEXT NOT NULL, sequence INTEGER NOT NULL, name TEXT NOT NULL,
                    detail_json TEXT NOT NULL, PRIMARY KEY (run_id, sequence),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id));
            """)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=10, isolation_level=None, factory=ClosingConnection)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=10000")
        return db

    @staticmethod
    def _snapshot(row: sqlite3.Row) -> Snapshot:
        index = row["stage_index"]
        return Snapshot(row["run_id"], row["status"], STAGES[index] if index < len(STAGES) else None,
                        row["version"], json.loads(row["state_json"]))

    def start(self, run_id: str, case: dict[str, Any]) -> Snapshot:
        if not isinstance(run_id, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,60}", run_id):
            raise WorkflowError("Invalid run ID")
        _validate_case(case)
        payload = _json(case)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row:
                if row["case_json"] != payload:
                    raise WorkflowError("Cannot reuse run ID for a different case")
                return self._snapshot(row)
            db.execute("INSERT INTO runs VALUES (?, 'active', 0, 0, ?, ?)", (run_id, payload, "{}"))
            db.execute("INSERT INTO events VALUES (?, 0, 'started', ?)",
                       (run_id, _json({"case_id": case["id"], "tenant": case["tenant"]})))
            return self.get(run_id, db)

    def get(self, run_id: str, db: sqlite3.Connection | None = None) -> Snapshot:
        if db is None:
            with self._connect() as conn:
                return self.get(run_id, conn)
        row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise WorkflowError("Unknown run ID")
        return self._snapshot(row)

    def advance(self, run_id: str, *, inject_failure: bool = False) -> Snapshot:
        """Exactly one checkpoint and event in a transaction; all handlers are pure."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise WorkflowError("Unknown run ID")
            status, idx = row["status"], row["stage_index"]
            if status == "awaiting_approval":
                raise ApprovalRequired("Human approval is required before finalizing")
            if status in ("rejected", "completed"):
                return self._snapshot(row)
            case, state = json.loads(row["case_json"]), json.loads(row["state_json"])
            stage = STAGES[idx]
            if stage == "review":
                if state["classify"]["requires_approval"]:
                    status = "awaiting_approval"
                    detail = {"approval_needed": True, "proposed_action": state["plan"]["recommendation"]}
                else:
                    state["review"] = {"approved": True, "actor": "automated-low-severity-policy"}
                    status, idx = "active", idx + 1
                    detail = state["review"]
            else:
                handler = {"classify": classify, "enrich": enrich, "plan": plan, "finalize": finalize}[stage]
                state[stage] = handler(case, state)
                detail = {"stage": stage, "outcome": "stored", "evidence_refs": state[stage].get("evidence_refs", [])}
                idx += 1
                status = "completed" if idx == len(STAGES) else "active"
            version = row["version"] + 1
            db.execute("UPDATE runs SET status=?, stage_index=?, version=?, state_json=? WHERE run_id=?",
                       (status, idx, version, _json(state), run_id))
            db.execute("INSERT INTO events VALUES (?, ?, ?, ?)",
                       (run_id, version, stage if status != "completed" else "completed", _json(detail)))
            if inject_failure:
                raise RuntimeError("Simulated crash before transaction commit")
            return self.get(run_id, db)

    def decide(self, run_id: str, actor: str, approved: bool) -> Snapshot:
        """Demo approval marker, **not** user authentication or authorization."""
        if not isinstance(actor, str) or not re.fullmatch(r"[a-zA-Z0-9_.@-]{2,80}", actor):
            raise WorkflowError("Invalid approval actor label")
        if type(approved) is not bool:
            raise WorkflowError("approved must be boolean")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["status"] != "awaiting_approval":
                raise WorkflowError("Run is not awaiting approval")
            state = json.loads(row["state_json"])
            state["review"] = {"approved": approved, "actor": actor}
            idx = row["stage_index"] + 1 if approved else row["stage_index"]
            status = "active" if approved else "rejected"
            version = row["version"] + 1
            db.execute("UPDATE runs SET status=?, stage_index=?, version=?, state_json=? WHERE run_id=?",
                       (status, idx, version, _json(state), run_id))
            db.execute("INSERT INTO events VALUES (?, ?, 'approval_decision', ?)",
                       (run_id, version, _json({"actor": actor, "approved": approved})))
            return self.get(run_id, db)

    def events(self, run_id: str) -> list[dict[str, Any]]:
        self.get(run_id)
        with self._connect() as db:
            rows = db.execute("SELECT sequence, name, detail_json FROM events WHERE run_id=? ORDER BY sequence",
                              (run_id,)).fetchall()
            return [{"sequence": r["sequence"], "name": r["name"],
                     "detail": json.loads(r["detail_json"])} for r in rows]
