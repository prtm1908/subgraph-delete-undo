#!/usr/bin/env python3
"""Break the undo and the redo on purpose, one way at a time, and show which checks catch each break.

    python3 mutation_check.py

Each mutation is a small edit to a copy of this repository in a temporary directory. For every copy the
script generates both fixtures, runs the scenario and runs the checker. A checker that passes a broken
undo or redo would prove nothing, so every mutation below must make at least one check fail. Exit status 0
when every mutation is caught. Takes a few minutes.
"""

from __future__ import annotations

import asyncio
import re
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# name -> [(file, text to find, replacement)]
MUTATIONS: dict[str, list[tuple[str, str, str]]] = {
    "merge overwrites what a person wrote": [
        ("graph_undo/undo.py",
         "old_is_newer = old_w is not None and (live_w is None or old_w[0] > live_w[0])",
         "old_is_newer = old_w is not None"),
    ],
    "tags not recomputed after a step": [
        ("graph_undo/undo.py",
         "            # Recompute what this step can have changed: the node and everything now below it.\n"
         "            await refresh_subtree(store, target)\n",
         "            pass\n"),
    ],
    "arrows a person moved away come back": [
        ("graph_undo/undo.py",
         '"AND seq > (SELECT seq FROM ops WHERE op_id = ?) LIMIT 1"',
         '"AND seq > (SELECT seq FROM ops WHERE op_id = ?) AND 0 LIMIT 1"'),
    ],
    "no transaction per step": [
        ("graph_undo/store.py", '        await self._call(self._conn.execute, "BEGIN IMMEDIATE")\n', "        pass\n"),
        ("graph_undo/store.py", '            await self._call(self._conn.execute, "COMMIT")\n', "            pass\n"),
        ("graph_undo/store.py",
         '            await self._call(self._conn.execute, "ROLLBACK")\n            raise\n        try:',
         "            raise\n        try:"),
    ],
    "report leaves out one arrow": [
        ("graph_undo/undo.py",
         "    for from_id, to_id, edge_type, outcome, reason, waiting_on, now_from, now_to in edge_rows:",
         "    for from_id, to_id, edge_type, outcome, reason, waiting_on, now_from, now_to in edge_rows[:-1]:"),
    ],
    "restored even when the name is taken": [
        ("graph_undo/undo.py",
         'holder = await store.fetchval("SELECT id FROM nodes WHERE qualified_name = ?", (row.qualified_name,))',
         "holder = None"),
        ("graph_undo/schema.py", "qualified_name TEXT NOT NULL UNIQUE,", "qualified_name TEXT NOT NULL,"),
    ],
    "waiting rows never retried": [
        ("graph_undo/undo.py", "        await self.retry_waiting()\n", "        pass\n"),
    ],
    "report shows the wrong replaced value": [
        ("graph_undo/undo.py",
         "        replaced = {\n            field: live_values[field]\n",
         "        replaced = {\n            field: old_values[field]\n"),
    ],
    "skipped rows decided again on every run": [
        ("graph_undo/undo.py",
         "    if not entry.finished:\n        await UndoRun(store, op_id, entry.seq).run()\n",
         "    if True:\n        await UndoRun(store, op_id, entry.seq).run()\n"),
        ("graph_undo/undo.py",
         "    if retrying_for is None:\n        return outcome is None",
         "    if retrying_for is None:\n        return outcome in (None, SKIPPED)"),
        ("graph_undo/undo.py",
         'where, params = "outcome IS NULL", (delete_op,)',
         "where, params = \"(outcome IS NULL OR outcome = 'skipped')\", (delete_op,)"),
    ],
    # ---- redo ----
    "redo removes a node a person wrote on": [
        ("graph_undo/redo.py", '            return "written_since_undo"\n', "            pass\n"),
    ],
    "redo puts back a field a person rewrote": [
        ("graph_undo/redo.py",
         "if field not in copied or current.get(field) != list(copied[field]):",
         "if field not in copied:"),
    ],
    "redo leaves skipped rows waiting": [
        ("graph_undo/redo.py",
         'await self._close_node(self.op_id, node_id, UNCHANGED, "not_restored_by_undo", reset=True)',
         'await self._close_node(self.op_id, node_id, UNCHANGED, "not_restored_by_undo", reset=False)'),
    ],
    "redo skips rows an earlier round marked": [
        ("graph_undo/redo.py", 'PENDING = "(redone_by IS NULL OR redone_by != ?2)"', 'PENDING = "(redone_by IS NULL AND ?2 = ?2)"'),
        ("graph_undo/redo.py", "if row is None or row[7] == self.redo_seq:", "if row is None or row[7] is not None:"),
        ("graph_undo/redo.py", "if row is None or row[5] == self.redo_seq:", "if row is None or row[5] is not None:"),
    ],
    "redo accepted while the undo is unfinished": [
        ("graph_undo/rounds.py", "    if not newest.finished:\n", "    if False:\n"),
    ],
    "tags not recomputed after redo puts tags back": [
        ("graph_undo/redo.py",
         '            if "tags" in put_back:\n                await refresh_subtree(store, target)\n',
         "            pass\n"),
    ],
    "redo removes a node another undo merged into": [
        ("graph_undo/redo.py", '            return "still_in_use"\n        return None', "            pass\n        return None"),
    ],
    "redo keeps a node with nothing attached": [
        ("graph_undo/redo.py",
         "            return True  # not an arrow this redo takes out\n",
         "            return True  # not an arrow this redo takes out\n        return True\n"),
    ],
}

STEPS = [
    ["gen_fixture.py", "--seed", "7", "--out", "fixture"],
    ["gen_fixture.py", "--small", "--out", "fixture_small"],
    ["run_scenario.py", "--fixture", "fixture", "--small", "fixture_small", "--out", "run"],
]
CHECK = ["checker.py", "fixture", "--small", "fixture_small", "--run", "run"]


async def run(cwd: Path, args: list[str]) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, *args, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode()


def _prepare(target: Path, edits: list[tuple[str, str, str]]) -> None:
    shutil.copytree(
        ROOT, target,
        ignore=shutil.ignore_patterns(".git", "run", "fixture", "fixture_small", "__pycache__", "*.db", "*.db-*"),
    )
    for rel, old, new in edits:
        path = target / rel
        text = path.read_text(encoding="utf-8")
        if old not in text:
            raise SystemExit(f"mutation_check: {rel} no longer contains {old[:70]!r}; update MUTATIONS")
        path.write_text(text.replace(old, new, 1), encoding="utf-8")


async def mutate(name: str, edits: list[tuple[str, str, str]], work: Path, limit: asyncio.Semaphore) -> tuple[str, list[str]]:
    async with limit:
        target = work / re.sub(r"\W+", "_", name)
        await asyncio.to_thread(_prepare, target, edits)
        for step in STEPS:
            code, out = await run(target, step)
            if code != 0:
                return name, [f"({step[0]} failed: {out.strip().splitlines()[-1] if out.strip() else code})"]
        code, out = await run(target, CHECK)
        caught = sorted({re.split(r"\s{2,}", line)[1] for line in out.splitlines() if line.startswith("FAIL")})
        if code not in (0, 1):  # the checker itself stopped with an error: that is a fault in the checker
            raise SystemExit(f"mutation_check: checker.py crashed on {name!r}: {out.strip().splitlines()[-1]}")
        return name, caught


async def main() -> int:
    work = Path(await asyncio.to_thread(tempfile.mkdtemp, prefix="graphundo-mutants-"))
    try:
        limit = asyncio.Semaphore(3)
        results = await asyncio.gather(*(mutate(n, e, work, limit) for n, e in MUTATIONS.items()))
    finally:
        await asyncio.to_thread(shutil.rmtree, work, True)
    missed = 0
    for name, caught in results:
        print(f"{'CAUGHT' if caught else 'MISSED'}  {name}")
        for check in caught:
            print(f"          by: {check}")
        missed += not caught
    print(f"\n{len(results) - missed} of {len(results)} mutations caught")
    return 0 if missed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
