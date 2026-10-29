"""CLI for starting, stepping, resuming and auditing synthetic workflows."""
from __future__ import annotations
import argparse
import json
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory

from .engine import ApprovalRequired, Engine

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Crash-consistent synthetic agent workflow runner")
    parser.add_argument("--db", default="workflow.sqlite3", help="SQLite checkpoint path")
    sub = parser.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start")
    start.add_argument("run_id")
    start.add_argument("--case", choices=["urgent", "routine", "unknown"], default="urgent")
    for cmd in ("advance", "show", "events"):
        sub.add_parser(cmd).add_argument("run_id")
    decision = sub.add_parser("decide")
    decision.add_argument("run_id")
    decision.add_argument("--actor", required=True)
    decision.add_argument("--approve", choices=["yes", "no"], required=True)
    sub.add_parser("demo")
    args = parser.parse_args()
    fixture = json.loads((ROOT / "fixtures" / "cases.json").read_text(encoding="utf-8"))
    if args.command == "demo":
        with TemporaryDirectory() as folder:
            engine = Engine(Path(folder) / "demo.sqlite3")
            engine.start("demo-001", fixture["urgent"])
            while engine.get("demo-001").status == "active":
                engine.advance("demo-001")
            engine.decide("demo-001", "demo-reviewer", True)
            while engine.get("demo-001").status == "active":
                engine.advance("demo-001")
            print(json.dumps({"snapshot": asdict(engine.get("demo-001")),
                              "events": engine.events("demo-001")}, indent=2))
        return
    engine = Engine(args.db)
    if args.command == "start":
        result = asdict(engine.start(args.run_id, fixture[args.case]))
    elif args.command == "advance":
        try:
            result = asdict(engine.advance(args.run_id))
        except ApprovalRequired:
            result = {"status": "awaiting_approval", "message": "Use 'decide' to continue"}
    elif args.command == "decide":
        result = asdict(engine.decide(args.run_id, args.actor, args.approve == "yes"))
    elif args.command == "show":
        result = asdict(engine.get(args.run_id))
    else:
        result = engine.events(args.run_id)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
