#!/usr/bin/env python3
"""Run the scenario and save every dump and report the checker reads.

    python3 run_scenario.py --fixture fixture --small fixture_small --out run

Main scenario (run/main):
    load, apply, dump > before.jsonl, undo > report.json, dump > after.jsonl     timed for check 8
    undo again > report2.json, dump > after2.jsonl                              check 6
    copies of the database taken right after apply, each undone with CRASH_AFTER set (the first write,
    a few in the middle, the last, past the end, and two and three crashes in a row), a dump after each
    crash, then undo without it and a final dump                                check 7
    on another copy, the same ops file applied again, then an op_id re-used for a different operation

Redo on the main scenario (run/main/redo), after the timed part:
    redo > report.json, dump > after.jsonl, redo again                          straight after undo
    undo again (round 2), then a few edits by people and the ingest on what undo put back, then redo
    copies of the database taken right after undo, each redone with CRASH_AFTER set, as for undo
    a redo after a crashed undo, and an undo after a crashed redo: both must be refused

Small fixture (run/small): one ops file per story of the design, each from a fresh load. ops_a, ops_b and
ops_c have two deletes and are undone in both orders; ops_d, ops_e and ops_f have one.

Redo stories (run/redo): one folder per story, each a short script of apply, undo and redo steps from a
fresh load, with the output of every step and a dump after it.

The tool only ever runs as a subprocess, the way a person would run it. Runs that do not depend on each
other (the crashed copies, the small-fixture runs) go in parallel.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

from graph_undo.store import CRASH_EXIT_CODE

ROOT = Path(__file__).resolve().parent
TOOL = [sys.executable, str(ROOT / "graphundo.py")]


class StepFailed(Exception):
    pass


async def tool(
    db: Path,
    *args: str | Path,
    stdout: Path | None = None,
    crash_after: int | None = None,
    expect: tuple[int, ...] = (0,),
) -> tuple[int, str]:
    """Run one graphundo command. Returns the exit code and stderr."""
    env = {k: v for k, v in os.environ.items() if k != "CRASH_AFTER"}
    if crash_after is not None:
        env["CRASH_AFTER"] = str(crash_after)
    proc = await asyncio.create_subprocess_exec(
        *TOOL, "--db", str(db), *map(str, args),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env,
    )
    out, err = await proc.communicate()
    if stdout is not None:
        await asyncio.to_thread(stdout.write_bytes, out)
    message = err.decode()
    if proc.returncode not in expect:
        raise StepFailed(f"graphundo {' '.join(map(str, args))} exited {proc.returncode}: {message.strip()}")
    return proc.returncode or 0, message


def writes_of(stderr: str) -> int:
    """undo and redo say on stderr how many writes they made."""
    match = re.search(r": (\d+) writes", stderr)
    if match is None:
        raise StepFailed(f"the command did not say how many writes it made: {stderr!r}")
    return int(match.group(1))


async def write_json(path: Path, value: object) -> None:
    await asyncio.to_thread(path.write_text, json.dumps(value, indent=2) + "\n", "utf-8")


# ---- main scenario ----------------------------------------------------------------------------------


async def main_scenario(fixture: Path, out: Path, limit: asyncio.Semaphore) -> dict[str, object]:
    await asyncio.to_thread(out.mkdir, parents=True, exist_ok=True)
    db = out / "graph.db"
    ops_text = await asyncio.to_thread((fixture / "ops.jsonl").read_text, "utf-8")
    delete_op = next(
        rec["op_id"] for rec in map(json.loads, filter(str.strip, ops_text.splitlines()))
        if rec.get("kind") == "delete_subtree"
    )

    steps: dict[str, float] = {}

    async def timed(name: str, *args: str | Path, stdout: Path | None = None) -> str:
        start = time.perf_counter()
        _, err = await tool(db, *args, stdout=stdout)
        steps[name] = round(time.perf_counter() - start, 3)
        return err

    await timed("load", "load", fixture / "seed.jsonl", stdout=out / "load.json")
    await timed("apply", "apply", fixture / "ops.jsonl", stdout=out / "apply.json")
    await asyncio.to_thread(shutil.copyfile, db, out / "after_apply.db")  # for the crashed runs; not timed
    await timed("dump_before", "dump", stdout=out / "before.jsonl")
    writes = writes_of(await timed("undo", "undo", delete_op, stdout=out / "report.json"))
    await timed("dump_after", "dump", stdout=out / "after.jsonl")
    scenario_seconds = round(sum(steps.values()), 3)
    print(f"main: load+apply+dump+undo+dump in {scenario_seconds:.2f}s, undo made {writes} writes")

    _, err = await tool(db, "undo", delete_op, stdout=out / "report2.json")
    second_writes = writes_of(err)
    await tool(db, "dump", stdout=out / "after2.jsonl")

    plans = {f"at_{n:05d}": [n] for n in sorted({1, 2, 3, writes // 4, writes // 2, 3 * writes // 4, writes - 1, writes})}
    plans[f"at_{writes + 1:05d}_past_end"] = [writes + 1]
    plans["twice"] = [writes // 3, writes // 3]
    plans["three_times"] = [writes // 5, writes // 5, writes // 5]

    async def crashed(name: str, points: list[int]) -> None:
        async with limit:
            d = out / "crash" / name
            await asyncio.to_thread(d.mkdir, parents=True)
            copy = d / "graph.db"
            await asyncio.to_thread(shutil.copyfile, out / "after_apply.db", copy)
            codes = []
            for i, n in enumerate(points):
                code, _ = await tool(
                    copy, "undo", delete_op, stdout=d / f"crashed_{i}.out", crash_after=n,
                    expect=(0, CRASH_EXIT_CODE),
                )
                codes.append(code)
                await tool(copy, "dump", stdout=d / f"mid_{i}.jsonl")
            await tool(copy, "undo", delete_op, stdout=d / "report.json")
            await tool(copy, "dump", stdout=d / "final.jsonl")
            await write_json(d / "meta.json", {"crash_after": points, "exit_codes": codes})

    async def apply_again() -> None:
        """apply on the same file again changes nothing; an op_id re-used for another operation is refused."""
        async with limit:
            d = out / "apply_again"
            await asyncio.to_thread(d.mkdir, parents=True)
            copy = d / "graph.db"
            await asyncio.to_thread(shutil.copyfile, out / "after_apply.db", copy)
            same, _ = await tool(copy, "apply", fixture / "ops.jsonl", stdout=d / "same.json")
            await tool(copy, "dump", stdout=d / "after_same.jsonl")
            first = json.loads(ops_text.splitlines()[0])
            reused_file = d / "reused_op_id.jsonl"  # the first op_id again, for a different operation
            await asyncio.to_thread(reused_file.write_text, json.dumps({**first, "actor": "ingest"}) + "\n", "utf-8")
            reused, err = await tool(copy, "apply", reused_file, expect=(0, 1))
            await tool(copy, "dump", stdout=d / "after_reused.jsonl")
            await write_json(d / "meta.json", {"same_exit": same, "reused_exit": reused, "reused_stderr": err.strip()})

    await asyncio.gather(*(crashed(name, points) for name, points in plans.items()), apply_again())
    print(f"main: {len(plans)} crashed runs and the apply-again runs done")
    await main_redo(out, db, delete_op, writes, limit)
    meta = {"delete_op": delete_op, "undo_writes": writes, "second_undo_writes": second_writes, "steps": steps}
    await write_json(out / "meta.json", meta)
    return {"scenario_seconds": scenario_seconds, "steps": steps}


# ---- redo on the main scenario ------------------------------------------------------------------------


def between_ops(report: dict, before_text: str, after_text: str) -> list[dict]:
    """A few changes to what undo put back, made between the second undo and the redo that follows.

    People rewrite the description of three restored nodes and of one node undo filled, and add an arrow to
    a restored node. The ingest reports a new type for another restored node. Chosen by position in the
    report, so the same fixture always gives the same operations.
    """
    restored = [n for n in report["nodes"] if n["outcome"] == "restored"]
    filled = [n for n in report["nodes"] if n["outcome"] == "merged" and "description" in n.get("filled", [])]
    live_before = [json.loads(line) for line in before_text.splitlines() if '"qualified_name"' in line]
    arrows = {(e["from_id"], e["to_id"]) for e in map(json.loads, after_text.splitlines()) if "from_id" in e}
    ops: list[dict] = []

    def human(kind: str, **fields: object) -> None:
        ops.append({"op_id": f"b{len(ops) + 1:04d}", "actor": "human", "kind": kind, **fields})

    for k in (len(restored) // 3, len(restored) // 2, len(restored) - 1):
        human("edit_node", id=restored[k]["id"], set={"description": f"Rewritten after the undo ({k})"})
    if filled:
        human("edit_node", id=filled[0]["merged_into"], set={"description": "Rewritten on the merged node"})
    target = restored[len(restored) // 5]["id"]
    source = next(n["id"] for n in live_before if (n["id"], target) not in arrows)
    human("add_edge", from_id=source, to_id=target, type="references")
    name = restored[len(restored) // 7]["qualified_name"]
    ops.append({"op_id": f"b{len(ops) + 1:04d}", "actor": "ingest", "kind": "ingest_node",
                "qualified_name": name, "type": "retyped", "parent": None})
    return ops


async def main_redo(out: Path, db: Path, delete_op: str, undo_writes: int, limit: asyncio.Semaphore) -> None:
    d = out / "redo"
    await asyncio.to_thread(d.mkdir, parents=True)
    await asyncio.to_thread(shutil.copyfile, db, out / "after_undo.db")

    _, err = await tool(db, "redo", delete_op, stdout=d / "report.json")
    writes = writes_of(err)
    await tool(db, "dump", stdout=d / "after.jsonl")
    _, err = await tool(db, "redo", delete_op, stdout=d / "report2.json")
    second_writes = writes_of(err)
    await tool(db, "dump", stdout=d / "after2.jsonl")

    # Round 2: undo again, a few changes by others, then redo.
    await tool(db, "undo", delete_op, stdout=d / "undo2_report.json")
    await tool(db, "dump", stdout=d / "undo2_after.jsonl")
    report, before_text, after_text = await asyncio.gather(
        *(asyncio.to_thread(path.read_text, "utf-8") for path in (out / "report.json", out / "before.jsonl", out / "after.jsonl"))
    )
    between = between_ops(json.loads(report), before_text, after_text)
    await asyncio.to_thread(
        (d / "between.jsonl").write_text, "".join(json.dumps(op) + "\n" for op in between), "utf-8"
    )
    await tool(db, "apply", d / "between.jsonl", stdout=d / "between.json")
    await tool(db, "dump", stdout=d / "between_after.jsonl")
    await tool(db, "redo", delete_op, stdout=d / "partial_report.json")
    await tool(db, "dump", stdout=d / "partial_after.jsonl")
    print(f"main: redo made {writes} writes; round 2 with {len(between)} changes in between done")

    plans = {f"at_{n:05d}": [n] for n in sorted({1, 2, 3, writes // 4, writes // 2, 3 * writes // 4, writes - 1, writes})}
    plans[f"at_{writes + 1:05d}_past_end"] = [writes + 1]
    plans["twice"] = [writes // 3, writes // 3]

    async def crashed(name: str, points: list[int]) -> None:
        async with limit:
            c = d / "crash" / name
            await asyncio.to_thread(c.mkdir, parents=True)
            copy = c / "graph.db"
            await asyncio.to_thread(shutil.copyfile, out / "after_undo.db", copy)
            codes = []
            for i, n in enumerate(points):
                code, _ = await tool(
                    copy, "redo", delete_op, stdout=c / f"crashed_{i}.out", crash_after=n, expect=(0, CRASH_EXIT_CODE)
                )
                codes.append(code)
                await tool(copy, "dump", stdout=c / f"mid_{i}.jsonl")
            await tool(copy, "redo", delete_op, stdout=c / "report.json")
            await tool(copy, "dump", stdout=c / "final.jsonl")
            await write_json(c / "meta.json", {"crash_after": points, "exit_codes": codes})

    async def refused(name: str, source: Path, first: str, then: str, crash_after: int) -> None:
        """One command stopped by a crash, then the other command: it must be refused and change nothing."""
        async with limit:
            c = d / "refused" / name
            await asyncio.to_thread(c.mkdir, parents=True)
            copy = c / "graph.db"
            await asyncio.to_thread(shutil.copyfile, source, copy)
            crash_code, _ = await tool(copy, first, delete_op, crash_after=crash_after, expect=(0, CRASH_EXIT_CODE))
            await tool(copy, "dump", stdout=c / "mid.jsonl")
            code, err = await tool(copy, then, delete_op, stdout=c / "refused.out", expect=(0, 1))
            await tool(copy, "dump", stdout=c / "after_refused.jsonl")
            await tool(copy, first, delete_op)  # the crashed command is finished by running it again
            await tool(copy, then, delete_op)  # and then the other one is accepted
            await tool(copy, "dump", stdout=c / "final.jsonl")
            await write_json(c / "meta.json", {"first": first, "then": then, "crash_exit": crash_code, "exit": code, "stderr": err.strip()})

    await asyncio.gather(
        *(crashed(name, points) for name, points in plans.items()),
        refused("redo_after_crashed_undo", out / "after_apply.db", "undo", "redo", undo_writes // 2),
        refused("undo_after_crashed_redo", out / "after_undo.db", "redo", "undo", writes // 2),
    )
    await write_json(d / "meta.json", {"redo_writes": writes, "second_redo_writes": second_writes})
    print(f"main: {len(plans)} crashed redo runs and 2 refused commands done")


# ---- small fixture ----------------------------------------------------------------------------------


async def small_run(small: Path, out: Path, ops_file: str, order: list[str], limit: asyncio.Semaphore) -> None:
    async with limit:
        d = out / f"{ops_file.split('.')[0]}_{'_'.join(order)}"
        await asyncio.to_thread(d.mkdir, parents=True)
        db = d / "graph.db"
        await tool(db, "load", small / "seed.jsonl")
        await tool(db, "dump", stdout=d / "load.jsonl")
        await tool(db, "apply", small / ops_file, stdout=d / "apply.out")
        await tool(db, "dump", stdout=d / "applied.jsonl")
        writes: dict[str, int] = {}
        for k, op_id in enumerate(order, start=1):
            await tool(db, "dump", stdout=d / f"{k}_before.jsonl")
            _, err = await tool(db, "undo", op_id, stdout=d / f"{k}_report.json")
            writes[str(k)] = writes_of(err)
            await tool(db, "dump", stdout=d / f"{k}_after.jsonl")
            _, err = await tool(db, "undo", op_id, stdout=d / f"{k}_report2.json")
            writes[f"{k}_again"] = writes_of(err)
            await tool(db, "dump", stdout=d / f"{k}_after2.jsonl")
        if len(order) > 1:
            # The first delete's report once more: rows it was waiting on may have come back since.
            _, err = await tool(db, "undo", order[0], stdout=d / "again_report.json")
            writes["again"] = writes_of(err)
            await tool(db, "dump", stdout=d / "again_after.jsonl")
        await write_json(d / "manifest.json", {"ops": ops_file, "order": order, "writes": writes})


# ---- redo stories -------------------------------------------------------------------------------------

# name -> (ops file, steps). A step is (command, argument) or (command, argument, CRASH_AFTER).
REDO_STORIES: dict[str, tuple[str, list[tuple]]] = {
    "straight": ("ops_d.jsonl", [("undo", "d1"), ("redo", "d1"), ("redo", "d1"), ("undo", "d1"), ("redo", "d1")]),
    "edited_restored_node": ("ops_g.jsonl", [("undo", "d1"), ("apply", "later_edit_total.jsonl"), ("redo", "d1")]),
    "rewrote_filled_field": ("ops_d.jsonl", [("undo", "d1"), ("apply", "later_edit_orders.jsonl"), ("redo", "d1")]),
    "two_undos_redo_d2": ("ops_a.jsonl", [("undo", "d1"), ("undo", "d2"), ("redo", "d2"), ("undo", "d1")]),
    "two_undos_redo_d1": ("ops_a.jsonl", [("undo", "d1"), ("undo", "d2"), ("redo", "d1")]),
    "parent_redo_d1": ("ops_b.jsonl", [("undo", "d1"), ("undo", "d2"), ("redo", "d1")]),
    "parent_redo_d2": ("ops_b.jsonl", [("undo", "d1"), ("undo", "d2"), ("redo", "d2"), ("undo", "d1")]),
    "rows_were_waiting": ("ops_b.jsonl", [("undo", "d1"), ("redo", "d1"), ("undo", "d2")]),
    "deleted_again": ("ops_g.jsonl", [("undo", "d1"), ("apply", "later_delete_total.jsonl"), ("redo", "d1"),
                                      ("undo", "d1"), ("undo", "d3")]),
    "merge_target_deleted": ("ops_h.jsonl", [("undo", "d1"), ("apply", "later_delete_orders.jsonl"), ("redo", "d1"),
                                             ("undo", "d3")]),
    "two_undos_one_node_blocked": ("ops_i.jsonl", [("undo", "d2"), ("undo", "d1"), ("redo", "d2")]),
    "two_undos_one_node_clean": ("ops_i.jsonl", [("undo", "d2"), ("undo", "d1"), ("redo", "d1"), ("redo", "d2")]),
    "refused_while_unfinished": ("ops_d.jsonl", [("undo", "d1", 3), ("redo", "d1"), ("undo", "d1"),
                                                 ("redo", "d1", 3), ("undo", "d1"), ("redo", "d1")]),
}


async def redo_story(small: Path, out: Path, name: str, ops_file: str, steps: list[tuple], limit: asyncio.Semaphore) -> None:
    async with limit:
        d = out / name
        await asyncio.to_thread(d.mkdir, parents=True)
        db = d / "graph.db"
        await tool(db, "load", small / "seed.jsonl")
        await tool(db, "dump", stdout=d / "load.jsonl")
        await tool(db, "apply", small / ops_file, stdout=d / "apply.out")
        await tool(db, "dump", stdout=d / "0_dump.jsonl")
        done = []
        for k, step in enumerate(steps, start=1):
            command, arg = step[0], step[1]
            crash_after = step[2] if len(step) > 2 else None
            target = small / arg if command == "apply" else arg
            code, err = await tool(
                db, command, target, stdout=d / f"{k}_out.json", crash_after=crash_after, expect=(0, 1, CRASH_EXIT_CODE)
            )
            writes = writes_of(err) if code == 0 and command != "apply" else None
            await tool(db, "dump", stdout=d / f"{k}_dump.jsonl")
            done.append({"command": command, "arg": arg, "crash_after": crash_after, "exit": code, "writes": writes,
                         "stderr": err.strip()})
        await write_json(d / "manifest.json", {"ops": ops_file, "steps": done})


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the scenario and save what the checker reads.")
    parser.add_argument("--fixture", required=True, type=Path, help="directory with seed.jsonl and ops.jsonl")
    parser.add_argument("--small", type=Path, help="directory with the small fixture")
    parser.add_argument("--out", required=True, type=Path, help="directory to write the run into")
    args = parser.parse_args(argv)

    start = time.perf_counter()
    await asyncio.to_thread(shutil.rmtree, args.out, True)
    limit = asyncio.Semaphore(max(2, min(8, os.cpu_count() or 2)))
    try:
        timing = await main_scenario(args.fixture, args.out / "main", limit)
        if args.small is not None:
            # Files with two deletes run in both undo orders; the one-delete stories run once.
            runs = [
                (ops_file, order)
                for ops_file in ("ops_a.jsonl", "ops_b.jsonl", "ops_c.jsonl")
                for order in (["d1", "d2"], ["d2", "d1"])
            ] + [(ops_file, ["d1"]) for ops_file in ("ops_d.jsonl", "ops_e.jsonl", "ops_f.jsonl")]
            await asyncio.gather(*(small_run(args.small, args.out / "small", f, o, limit) for f, o in runs))
            print(f"small: {len(runs)} runs done")
            await asyncio.gather(
                *(redo_story(args.small, args.out / "redo", name, f, steps, limit) for name, (f, steps) in REDO_STORIES.items())
            )
            print(f"redo: {len(REDO_STORIES)} stories done")
    except StepFailed as exc:
        print(f"run_scenario: {exc}", file=sys.stderr)
        return 1
    timing["harness_seconds"] = round(time.perf_counter() - start, 3)
    await write_json(args.out / "main" / "timing.json", timing)
    print(f"harness took {timing['harness_seconds']:.1f}s; now run checker.py")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
