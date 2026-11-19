"""The command line: load, apply, undo, dump.

    python3 graphundo.py [--db graph.db] load seed.jsonl
    python3 graphundo.py [--db graph.db] apply ops.jsonl
    python3 graphundo.py [--db graph.db] undo <op_id>      (reads CRASH_AFTER=<n>)
    python3 graphundo.py [--db graph.db] dump

The database path defaults to $GRAPHUNDO_DB, then graph.db in the current directory.
Exit codes: 0 done, 1 error (nothing written), 3 stopped by CRASH_AFTER.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import TextIO

from .applier import Applier
from .common import InputError
from .dumper import dump
from .loader import load
from .store import Store
from .undo import UndoError, format_report, undo


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="graphundo", description="Undo for subtree deletes in a shared graph.")
    parser.add_argument(
        "--db",
        default=os.environ.get("GRAPHUNDO_DB", "graph.db"),
        help="SQLite file (default: $GRAPHUNDO_DB or graph.db)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("load", help="create the database from a seed file").add_argument("seed")
    commands.add_parser("apply", help="apply operations in order").add_argument("ops")
    commands.add_parser("undo", help="undo one subtree delete and print the report").add_argument("op_id")
    commands.add_parser("dump", help="print the live graph as sorted JSONL")
    return parser


def _crash_after() -> int | None:
    raw = os.environ.get("CRASH_AFTER", "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        raise InputError(f"CRASH_AFTER must be a whole number, got {raw!r}") from None
    if value < 0:
        raise InputError(f"CRASH_AFTER must not be negative, got {value}")
    return value


def _write(stream: TextIO, text: str) -> None:
    stream.write(text)
    stream.flush()


async def _run(args: argparse.Namespace) -> int:
    if args.command == "load":
        counts = await load(args.db, args.seed)
        await asyncio.to_thread(_write, sys.stdout, json.dumps(counts) + "\n")
        return 0

    if not await asyncio.to_thread(os.path.isfile, args.db):
        raise InputError(f"no database at {args.db}; run load first")
    crash_after = _crash_after() if args.command == "undo" else None  # only undo reads CRASH_AFTER
    store = await Store.open(args.db, crash_after=crash_after)
    try:
        if args.command == "apply":
            output = json.dumps(await Applier(store).apply_file(args.ops), indent=2) + "\n"
        elif args.command == "undo":
            output = format_report(await undo(store, args.op_id))
            await asyncio.to_thread(_write, sys.stderr, f"graphundo: undo {args.op_id}: {store.writes} writes\n")
        else:
            output = await dump(store)
        await asyncio.to_thread(_write, sys.stdout, output)
    finally:
        await store.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return asyncio.run(_run(args))
    except (InputError, UndoError) as exc:
        print(f"graphundo: error: {exc}", file=sys.stderr)
        return 1
    except BrokenPipeError:
        return 1
