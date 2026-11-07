import argparse
import json
from pathlib import Path

import psycopg

from ..pipeline.config import DATABASE_CONFIG, PROCESSED_DIR
from .context import build_context
from .evaluate import evaluate_run
from .workflow import approve_run, generate_run


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate, review and evaluate pipeline candidates")
    sub = parser.add_subparsers(dest="command", required=True)
    propose = sub.add_parser("propose")
    propose.add_argument("agent", choices=("data_quality", "gold_design"))
    propose.add_argument("--instruction", required=True)
    propose.add_argument("--revise", type=Path)
    propose.add_argument("--feedback", default="")
    approve = sub.add_parser("approve")
    approve.add_argument("run", type=Path)
    approve.add_argument("--reviewer", required=True)
    evaluate = sub.add_parser("evaluate")
    evaluate.add_argument("run", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "propose":
            previous = None
            if args.revise:
                if not args.feedback.strip():
                    raise ValueError("A revision requires feedback")
                previous = json.loads((args.revise / "proposal.json").read_text())
                parent = json.loads((args.revise / "context.json").read_text())
                if parent["agent"] != args.agent:
                    raise ValueError("Revision must use the original agent")
            with psycopg.connect(**DATABASE_CONFIG) as connection:
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                context = build_context(connection, args.agent, args.instruction, args.feedback, previous)
            if args.revise:
                context["parent_run"] = args.revise.resolve().name
            directory = generate_run(args.agent, context, PROCESSED_DIR / "agent_runs")
            print(directory)
        elif args.command == "approve":
            approve_run(args.run, args.reviewer)
            print("Approved for isolated evaluation")
        else:
            print(json.dumps(evaluate_run(args.run), indent=2))
        return 0
    except Exception as error:
        reason = str(error) if type(error) is ValueError else "Check configuration, artifacts and trace files"
        print(f"Agent operation failed ({type(error).__name__}): {reason}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
