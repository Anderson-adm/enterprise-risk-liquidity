"""Command line interface: ``erl --help``."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from erl import config, log, pipeline
from erl.errors import ErlError
from erl.ingestion import bronze
from erl.sources import seed, synthetic

EXIT_CODES = {
    "configuration": 2,
    "infrastructure": 3,
    "source_unavailable": 3,
    "contract": 4,
    "quality_gate": 5,
    "publication": 6,
}


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def notify_failure(settings: config.Settings, step: str, run_id: str, ref: str, airflow_run: str) -> dict:
    import logging
    import os

    state = pipeline.load_state(settings, run_id)["steps"].get(step, {})
    message = {
        "alert": "erl pipeline step failed",
        "step": step,
        "run_id": run_id,
        "reference_date": ref,
        "airflow_run": airflow_run,
        "error_type": state.get("error_type", "unknown"),
        "error": state.get("error", "see task log")[:500],
    }
    log.event(logging.getLogger("erl.alert"), "step_failure_alert", logging.ERROR, **message)
    topic = os.environ.get("ERL_ALERTS_TOPIC_ARN")
    if settings.mode == "aws" and topic:
        import boto3

        boto3.client("sns", region_name=settings.aws_region).publish(
            TopicArn=topic,
            Subject=f"[erl] {step} failed for {ref}"[:100],
            Message=json.dumps(message, indent=2),
        )
        message["sns_topic"] = topic
    return message


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="erl", description="Enterprise Risk & Liquidity data platform")
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="(re)create synthetic source databases and inbox files")
    g.add_argument("--dates", nargs="*", default=[d.isoformat() for d in synthetic.DEFAULT_DATES])

    r = sub.add_parser("run", help="run the whole pipeline for one reference date")
    r.add_argument("--date", required=True)
    r.add_argument("--run-id")

    s = sub.add_parser("step", help="run a single pipeline step (used by Airflow)")
    s.add_argument("name", choices=list(pipeline.STEPS))
    s.add_argument("--date", required=True)
    s.add_argument("--run-id", required=True)

    sc = sub.add_parser("scenario", help="apply a controlled failure/correction scenario")
    sc.add_argument("name", choices=["correct-position", "remove-market-data", "degrade-market-data"])
    sc.add_argument("--date", required=True)

    sub.add_parser("new-run-id", help="print a new run id")

    n = sub.add_parser("notify", help="alert about a failed step (SNS in aws mode, log otherwise)")
    n.add_argument("--step", required=True)
    n.add_argument("--run-id", required=True)
    n.add_argument("--date", required=True)
    n.add_argument("--airflow-run", default="")
    sub.add_parser("publications", help="list the consumer publication log")

    args = parser.parse_args(argv)
    settings = config.load()
    log_file = settings.data_dir / "logs" / "erl.jsonl"
    log.configure(args.log_level, log_file)

    try:
        if args.command == "generate":
            _print(seed.generate_all(settings, tuple(date.fromisoformat(d) for d in args.dates)))
        elif args.command == "run":
            ref = pipeline.parse_reference_date(args.date)
            state = pipeline.run_all(ref, args.run_id, settings)
            _print(
                {
                    "run_id": state["run_id"],
                    "status": state["status"],
                    "steps": {k: v["status"] for k, v in state["steps"].items()},
                    "report": state["steps"]["report"].get("report"),
                }
            )
            return 0 if state["status"] == "SUCCEEDED" else EXIT_CODES.get(state.get("error_type"), 1)
        elif args.command == "step":
            ref = pipeline.parse_reference_date(args.date)
            _print(pipeline.run_step(args.name, pipeline.STEPS[args.name], settings, ref, args.run_id))
        elif args.command == "scenario":
            ref = date.fromisoformat(args.date)
            fn = {
                "correct-position": seed.correct_position,
                "remove-market-data": seed.remove_market_data,
                "degrade-market-data": seed.degrade_market_data,
            }[args.name]
            _print(fn(settings, ref))
        elif args.command == "new-run-id":
            print(bronze.new_run_id())
        elif args.command == "notify":
            _print(notify_failure(settings, args.step, args.run_id, args.date, args.airflow_run))
        elif args.command == "publications":
            from erl.publish import publisher

            engine = seed.engine_for(settings.consumer_url())
            _print(publisher.active_publications(engine))
    except ErlError as exc:
        print(f"ERROR [{exc.category}] {exc}", file=sys.stderr)
        return EXIT_CODES.get(exc.category, 1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
