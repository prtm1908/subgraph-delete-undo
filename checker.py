#!/usr/bin/env python3
"""Check a scenario run against the task's eight invariants, from the dumps and reports alone.

    python3 checker.py fixture before.jsonl after.jsonl report.json      by hand: checks 0-5, planted cases
    python3 checker.py fixture --small fixture_small --run run           after run_scenario.py: everything

A separate program: it imports nothing from the tool and never runs it. It replays the seed and the
operations in plain dictionaries (replay.py) to know what each delete removed and who wrote what, and when,
and holds the dumps and reports that run_scenario.py saved against that.

    0  apply matches the replay          the dump before undo equals the replayed graph
    1  no human change lost              every value, arrow and delete a person made after the delete is
                                         as it was just before undo
    2  free name + live parent: restored  every such removed node is back with its old id and authored fields
    3  names unique                       no two live nodes share a qualified_name
    4  report accounts for every row     every removed node and arrow exactly once, and each outcome agrees
                                         with the dump (restored: there; merged: target there; skipped: absent)
    5  effective_tags correct            recomputed from own tags and contains arrows in the dump
    6  idempotent                        a second undo gives the same dump and the same report, byte for byte,
                                         and writes nothing
    7  resumable                         crashed undos finish to the same dump and report; a dump between the
                                         runs has no arrow to a missing node and correct tags
    8  under 60 seconds                  the scenario (load to the last dump) and the whole run, timed by
                                         run_scenario.py

Checks 6, 7 and 8 need the runs that run_scenario.py makes, so they are only done with --run.

Plus the planted cases of the main fixture (planted.json), applying the operations file again, and the
nine runs of the small fixture: one per story of the design, each held against what the story shows.
Exit status 0 when every check passes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from replay import AUTHORED, Edge, Replay, ingest_node_id, walk_down

CRASH_EXIT_CODE = 3  # the exit status README.md gives for a run stopped by CRASH_AFTER
TIME_LIMIT_SECONDS = 60.0


# ---- dumps -------------------------------------------------------------------------------------------


@dataclass
class Graph:
    nodes: dict[str, dict[str, Any]]
    edges: set[Edge]

    @classmethod
    def parse(cls, text: str) -> Graph:
        nodes: dict[str, dict[str, Any]] = {}
        edges: set[Edge] = set()
        for line in text.splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if "qualified_name" in rec:
                nodes[rec["id"]] = {k: v for k, v in rec.items() if k != "id"}
            else:
                edges.add((rec["from_id"], rec["to_id"], rec["type"]))
        return cls(nodes, edges)

    @classmethod
    def of(cls, replay: Replay) -> Graph:
        """The replayed graph, with effective_tags worked out the same way as for a dump."""
        graph = cls({node: dict(fields) for node, fields in replay.nodes.items()}, replay.edges())
        for node, tags in graph.computed_tags().items():
            graph.nodes[node]["effective_tags"] = tags
        return graph

    def parent_of(self) -> dict[str, str]:
        return {t: f for f, t, kind in self.edges if kind == "contains"}

    def dangling(self) -> list[Edge]:
        return sorted(e for e in self.edges if e[0] not in self.nodes or e[1] not in self.nodes)

    def duplicate_names(self) -> list[str]:
        counts = Counter(n["qualified_name"] for n in self.nodes.values())
        return sorted(name for name, k in counts.items() if k > 1)

    def computed_tags(self) -> dict[str, list[str]]:
        parent = self.parent_of()
        memo: dict[str, set[str]] = {}
        for start in self.nodes:
            chain: list[str] = []
            node: str | None = start
            seen: set[str] = set()
            while node is not None and node not in memo and node in self.nodes:
                if node in seen:
                    raise ValueError(f"contains loop through {node}")
                seen.add(node)
                chain.append(node)
                node = parent.get(node)
            inherited = memo.get(node, set()) if node is not None else set()
            for nid in reversed(chain):
                inherited = inherited | set(self.nodes[nid]["tags"])
                memo[nid] = inherited
        return {nid: sorted(tags) for nid, tags in memo.items()}

    def tag_problems(self) -> list[str]:
        try:
            computed = self.computed_tags()
        except ValueError as exc:
            return [str(exc)]
        return [
            f"{nid}: effective_tags {node['effective_tags']} != {computed[nid]}"
            for nid, node in sorted(self.nodes.items())
            if node["effective_tags"] != computed[nid]
        ]

    def comparable(self) -> tuple[dict[str, dict[str, Any]], set[Edge]]:
        fields = ("qualified_name", "type", *AUTHORED, "effective_tags")
        return {nid: {k: n[k] for k in fields} for nid, n in self.nodes.items()}, self.edges


# ---- results ---------------------------------------------------------------------------------------------


class Results:
    def __init__(self) -> None:
        self.rows: list[tuple[bool, str, str, str]] = []

    def add(self, check: str, scope: str, problems: list[str], note: str = "") -> None:
        ok = not problems
        detail = note if ok else f"{len(problems)} problem(s): " + "; ".join(problems[:3])
        self.rows.append((ok, check, scope, detail))

    def print(self) -> bool:
        width = max(len(r[1]) for r in self.rows)
        scope_width = max(len(r[2]) for r in self.rows)
        for ok, check, scope, detail in self.rows:
            print(f"{'PASS' if ok else 'FAIL'}  {check:<{width}}  {scope:<{scope_width}}  {detail}")
        failed = sum(1 for r in self.rows if not r[0])
        print(f"\n{len(self.rows) - failed} passed, {failed} failed")
        return failed == 0


def first_difference(a: str, b: str) -> str:
    """Where two texts first differ, for a readable failure."""
    for i, (x, y) in enumerate(zip(a.splitlines(), b.splitlines()), start=1):
        if x != y:
            return f"line {i} differs: {x[:120]} | {y[:120]}"
    return f"lengths differ: {len(a.splitlines())} vs {len(b.splitlines())} lines"


def idempotency_problems(after: str, after2: str, report: str, report2: str, writes: int) -> list[str]:
    problems = []
    if after2 != after:
        problems.append("the second undo changed the dump: " + first_difference(after, after2))
    if report2 != report:
        problems.append("the second undo printed a different report: " + first_difference(report, report2))
    if writes != 0:
        problems.append(f"the second undo made {writes} writes")
    return problems


# ---- the checks for one undo --------------------------------------------------------------------------


def check_undo(
    res: Results, scope: str, replay: Replay, delete_op: str, before: Graph, after: Graph, report: dict[str, Any]
) -> None:
    deleted = replay.deletes[delete_op]
    seq = deleted["seq"]

    # 1. No change made by a human after the delete is lost or altered.
    problems: list[str] = []
    values = arrows = removals = 0
    for (nid, field), (when, actor) in sorted(replay.writes.items()):
        if actor == "human" and when > seq and nid in before.nodes:
            values += 1
            if nid not in after.nodes:
                problems.append(f"{nid} (written by a person) is gone")
            elif after.nodes[nid][field] != before.nodes[nid][field]:
                problems.append(f"{nid}.{field}: {before.nodes[nid][field]!r} -> {after.nodes[nid][field]!r}")
    for edge, events in sorted(replay.edge_events.items()):
        human = [e for e in events if e[1] == "human" and e[0] > seq]
        if not human:
            continue
        arrows += 1
        last = max(human)
        if last[2] == "add" and edge in before.edges and edge not in after.edges:
            problems.append(f"arrow {edge} a person added is gone")
        if last[2] == "remove" and edge not in before.edges and edge in after.edges:
            problems.append(f"arrow {edge} a person took away is back")
    for nid, events in sorted(replay.node_removals.items()):
        if any(actor == "human" and when > seq for when, actor in events):
            removals += 1
            if nid not in before.nodes and nid in after.nodes:
                problems.append(f"node {nid} a person deleted after {delete_op} is back")
    res.add(
        "1 no human change lost",
        scope,
        problems,
        f"{values} values, {arrows} arrows, {removals} deleted nodes a person touched after {delete_op}",
    )

    # 2. A removed node whose name is free and whose parent is live comes back with its authored fields.
    removed = deleted["nodes"]
    live_names = {n["qualified_name"] for n in before.nodes.values()}
    children: dict[str, list[str]] = defaultdict(list)
    roots = []
    for nid, snap in removed.items():
        if snap["parent"] in removed:
            children[snap["parent"]].append(nid)
        else:
            roots.append(nid)
    expected: set[str] = set()
    for nid in walk_down(roots, children):
        snap = removed[nid]
        if snap["qualified_name"] in live_names:
            continue
        parent = snap["parent"]
        if parent is None:
            parent_live = True
        elif parent in removed:  # restored, or merged into the live node that holds its name
            parent_live = parent in expected or removed[parent]["qualified_name"] in live_names
        else:
            parent_live = parent in before.nodes
        if parent_live:
            expected.add(nid)
    problems = []
    for nid in sorted(expected):
        snap, got = removed[nid], after.nodes.get(nid)
        if got is None:
            problems.append(f"{nid} ({snap['qualified_name']}) not restored")
            continue
        for field in ("qualified_name", *AUTHORED):
            if got[field] != snap[field]:
                problems.append(f"{nid}.{field}: {got[field]!r} != {snap[field]!r}")
    res.add("2 free name + live parent => restored", scope, problems, f"{len(expected)} of {len(removed)} removed nodes")

    # 3. No two live nodes share a name.
    res.add(
        "3 names unique",
        scope,
        [f"{name} is used twice" for name in after.duplicate_names()],
        f"{len(after.nodes)} live nodes",
    )

    # 4. The report accounts for every removed node and arrow exactly once, and agrees with the dump.
    problems = []
    nodes = report.get("nodes", [])
    arrows_rep = report.get("edges", [])
    node_ids = Counter(e["id"] for e in nodes)
    edge_keys = Counter((e["from"], e["to"], e["type"]) for e in arrows_rep)
    problems += [f"node {k} reported {n} times" for k, n in node_ids.items() if n != 1]
    problems += [f"arrow {k} reported {n} times" for k, n in edge_keys.items() if n != 1]
    problems += [f"node {k} missing from report" for k in sorted(set(removed) - set(node_ids))]
    problems += [f"node {k} in report but not removed" for k in sorted(set(node_ids) - set(removed))]
    problems += [f"arrow {k} missing from report" for k in sorted(deleted["edges"] - set(edge_keys))]
    problems += [f"arrow {k} in report but not removed" for k in sorted(set(edge_keys) - deleted["edges"])]
    for e in nodes:
        outcome = e.get("outcome")
        if outcome not in ("restored", "merged", "skipped"):
            problems.append(f"node {e['id']}: outcome {outcome!r}")
        elif outcome != "restored" and not e.get("reason"):
            problems.append(f"node {e['id']}: {outcome} without a reason")
        elif outcome == "restored" and e["id"] not in after.nodes:
            problems.append(f"node {e['id']}: restored but not live")
        elif outcome == "merged":
            target = after.nodes.get(e.get("merged_into"))
            if target is None or target["qualified_name"] != e["qualified_name"] or e["id"] in after.nodes:
                problems.append(f"node {e['id']}: merged, but the target is not live under the same name")
        elif outcome == "skipped" and e["id"] in after.nodes:
            problems.append(f"node {e['id']}: skipped but live")
    problems += merge_problems(nodes, removed, before, after)
    for e in arrows_rep:
        edge = (e["from"], e["to"], e["type"])
        outcome = e.get("outcome")
        if outcome not in ("restored", "merged", "skipped"):
            problems.append(f"arrow {edge}: outcome {outcome!r}")
        elif outcome != "restored" and not e.get("reason"):
            problems.append(f"arrow {edge}: {outcome} without a reason")
        elif outcome == "restored" and edge not in after.edges:
            problems.append(f"arrow {edge}: restored but not live")
        elif outcome == "merged" and (not e.get("now") or (e["now"][0], e["now"][1], e["type"]) not in after.edges):
            problems.append(f"arrow {edge}: merged, but {e.get('now')} is not live")
        elif outcome == "skipped" and edge in after.edges:
            problems.append(f"arrow {edge}: skipped but live")
    res.add("4 report accounts for every row once", scope, problems, f"{len(nodes)} nodes, {len(arrows_rep)} arrows")

    # 5. effective_tags correct for every live node (and the graph is whole).
    problems = after.tag_problems() + [f"arrow {e} to a missing node" for e in after.dangling()]
    res.add("5 effective_tags correct", scope, problems, f"{len(after.nodes)} nodes recomputed")


def merge_problems(
    nodes: list[dict[str, Any]], removed: dict[str, dict[str, Any]], before: Graph, after: Graph
) -> list[str]:
    """A merged row's fields, held against the node it was merged into, just before and just after undo.

    Kept fields are unchanged. A filled field that changed now holds the journal value, and when the report
    says what it replaced, that is what the node held before. Rows decided by an earlier undo (their target
    was not live before this one) are left alone.
    """
    problems = []
    for e in nodes:
        if e.get("outcome") != "merged":
            continue
        was, now = before.nodes.get(e.get("merged_into")), after.nodes.get(e.get("merged_into"))
        if was is None or now is None:
            continue
        filled, replaced = e.get("filled", []), e.get("replaced", {})
        for field in e.get("kept_live", []):
            if now[field] != was[field]:
                problems.append(f"node {e['id']}: kept {field}, but it changed from {was[field]!r} to {now[field]!r}")
        for field in filled:
            if now[field] != was[field] and now[field] != removed[e["id"]][field]:
                problems.append(f"node {e['id']}: filled {field} is {now[field]!r}, not the journal value")
        for field, value in replaced.items():
            if field not in filled:
                problems.append(f"node {e['id']}: replaced {field} was not filled")
            elif now[field] != was[field] and value != was[field]:
                problems.append(f"node {e['id']}: replaced {field} is {value!r}, but the node held {was[field]!r}")
    return problems


# ---- the main scenario ----------------------------------------------------------------------------------


async def read_text(path: Path) -> str:
    return await asyncio.to_thread(path.read_text, encoding="utf-8")


async def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in (await read_text(path)).splitlines() if line.strip()]


async def read_json(path: Path) -> Any:
    return json.loads(await read_text(path))


async def check_main(res: Results, fixture: Path, before_path: Path, after_path: Path, report_path: Path, run: Path | None) -> None:
    """Checks 0-5 and the planted cases from one before dump, after dump and report; 6-8 from the run folder."""
    scope = "main"
    seed, ops, planted, before_text, after_text, report_text = await asyncio.gather(
        read_jsonl(fixture / "seed.jsonl"),
        read_jsonl(fixture / "ops.jsonl"),
        read_json(fixture / "planted.json"),
        read_text(before_path),
        read_text(after_path),
        read_text(report_path),
    )
    replay = Replay(seed)
    replay.apply_all(ops)
    delete_op = next(op["op_id"] for op in ops if op.get("kind") == "delete_subtree")
    before, after = Graph.parse(before_text), Graph.parse(after_text)
    report = json.loads(report_text)

    # 0. The dump before undo is what the operations say.
    problems = [] if report.get("op_id") == delete_op else [f"report is for {report.get('op_id')}, the delete is {delete_op}"]
    want_nodes, want_edges = Graph.of(replay).comparable()
    got_nodes, got_edges = before.comparable()
    problems += [f"node {k} differs" for k in sorted(set(want_nodes) ^ set(got_nodes))][:3]
    problems += [f"node {k} fields differ" for k in sorted(set(want_nodes) & set(got_nodes)) if want_nodes[k] != got_nodes[k]][:3]
    problems += [f"arrow {e} differs" for e in sorted(want_edges ^ got_edges)][:3]
    rejected = sorted(k for k, v in replay.status.items() if v != "applied")
    res.add("0 apply matches the replay", scope, problems, f"{len(ops)} ops, rejected: {', '.join(rejected) or 'none'}")

    check_undo(res, scope, replay, delete_op, before, after, report)
    if run is not None:
        await check_runs(res, run, before_text, after_text, report_text)
    check_planted(res, planted, replay, after, report)


async def check_runs(res: Results, run: Path, before_text: str, after_text: str, report_text: str) -> None:
    """Checks 6, 7 and 8, and applying the ops again, from the extra runs run_scenario.py made."""
    scope = "main"
    again, same_summary, after_same, after_reused = await asyncio.gather(
        read_json(run / "apply_again" / "meta.json"),
        read_json(run / "apply_again" / "same.json"),
        read_text(run / "apply_again" / "after_same.jsonl"),
        read_text(run / "apply_again" / "after_reused.jsonl"),
    )
    problems = []
    if again["same_exit"] != 0 or same_summary["applied"] or same_summary["rejected"]:
        problems.append(f"applying the same file again did something: {same_summary}")
    if after_same != before_text:
        problems.append("applying the same file again changed the graph")
    if again["reused_exit"] != 1 or "different operation" not in again["reused_stderr"]:
        problems.append(f"an op_id re-used for another operation was not refused: {again['reused_stderr']!r}")
    if after_reused != before_text:
        problems.append("the refused file changed the graph")
    res.add("apply again", scope, problems, "same file: nothing changes; op_id re-used for another operation: refused")
    meta, timing, after2_text, report2_text = await asyncio.gather(
        read_json(run / "meta.json"),
        read_json(run / "timing.json"),
        read_text(run / "after2.jsonl"),
        read_text(run / "report2.json"),
    )

    # 6. Idempotent.
    res.add(
        "6 idempotent",
        scope,
        idempotency_problems(after_text, after2_text, report_text, report2_text, meta["second_undo_writes"]),
        "second undo: same dump, same report, no writes",
    )

    # 7. Resumable.
    problems = []
    crash_dirs = sorted(p for p in (run / "crash").iterdir() if p.is_dir())
    total = meta["undo_writes"]
    plans: list[str] = []
    for d in crash_dirs:
        cmeta = await read_json(d / "meta.json")
        plans.append("+".join(str(n) for n in cmeta["crash_after"]))
        final, final_report = await asyncio.gather(read_text(d / "final.jsonl"), read_text(d / "report.json"))
        for i, (n, code) in enumerate(zip(cmeta["crash_after"], cmeta["exit_codes"])):
            if i == 0:
                want_code = CRASH_EXIT_CODE if n <= total else 0
                if code != want_code:
                    problems.append(f"{d.name}: CRASH_AFTER={n} exited {code}, expected {want_code}")
            elif code not in (0, CRASH_EXIT_CODE):
                problems.append(f"{d.name}: run {i + 1} exited {code}")
            mid = Graph.parse(await read_text(d / f"mid_{i}.jsonl"))
            problems += [f"{d.name}: mid dump {i}: arrow {e} to a missing node" for e in mid.dangling()]
            problems += [f"{d.name}: mid dump {i}: {p}" for p in mid.tag_problems()[:2]]
            problems += [f"{d.name}: mid dump {i}: {name} used twice" for name in mid.duplicate_names()]
        if final != after_text:
            problems.append(f"{d.name}: final dump differs: " + first_difference(after_text, final))
        if final_report != report_text:
            problems.append(f"{d.name}: final report differs: " + first_difference(report_text, final_report))
    res.add(
        "7 resumable", scope, problems, f"{len(crash_dirs)} runs of a {total}-write undo, CRASH_AFTER = {', '.join(plans)}"
    )

    # 8. Under 60 seconds: the scenario itself, and the whole run with every extra undo.
    seconds, whole = timing["scenario_seconds"], timing["harness_seconds"]
    problems = [f"the scenario took {seconds:.1f}s"] if seconds >= TIME_LIMIT_SECONDS else []
    problems += [f"the whole run took {whole:.1f}s"] if whole >= TIME_LIMIT_SECONDS else []
    res.add("8 under 60 seconds", scope, problems, f"scenario {seconds:.2f}s, whole run {whole:.1f}s")


def check_planted(res: Results, planted: dict[str, Any], replay: Replay, after: Graph, report: dict[str, Any]) -> None:
    """The rare cases the generator planted, against the rules in DECISIONS.md."""
    cases = planted["cases"]
    snapshot = replay.deletes[planted["delete_op"]]["nodes"]
    node_rows = {e["id"]: e for e in report["nodes"]}
    edge_rows = {(e["from"], e["to"], e["type"]): e for e in report["edges"]}
    parent = after.parent_of()

    def case(name: str, problems: list[str], note: str) -> None:
        res.add(f"planted: {name}", "main", problems, note)

    def expect(cond: bool, message: str, problems: list[str]) -> None:
        if not cond:
            problems.append(message)

    c = cases["untouched_recreated"]
    row, node, p = node_rows[c["old_id"]], after.nodes.get(c["new_id"], {}), []
    expect(row["outcome"] == "merged" and row.get("merged_into") == c["new_id"], f"row {row}", p)
    expect(set(AUTHORED) <= set(row.get("filled", [])), f"filled {row.get('filled')}", p)
    for f in AUTHORED:
        expect(node.get(f) == snapshot[c["old_id"]][f], f"{f} {node.get(f)!r}", p)
    case("recreated, nobody edited", p, "merged; description, owner, tags filled from the journal")

    c = cases["person_changed_field"]
    row, node, p = node_rows[c["old_id"]], after.nodes.get(c["new_id"], {}), []
    expect(row["outcome"] == "merged" and c["field"] in row.get("kept_live", []), f"row {row}", p)
    expect(node.get(c["field"]) == c["value"], f"{c['field']} is {node.get(c['field'])!r}", p)
    expect(row.get("not_restored", {}).get(c["field"]) == snapshot[c["old_id"]][c["field"]], "old value not listed", p)
    case("recreated, a person changed a field", p, f"{c['field']} stays {c['value']!r}, old value listed")

    c = cases["person_emptied_field"]
    row, node, p = node_rows[c["old_id"]], after.nodes.get(c["new_id"], {}), []
    expect(row["outcome"] == "merged" and c["field"] in row.get("kept_live", []), f"row {row}", p)
    expect(node.get(c["field"]) is None, f"{c['field']} is {node.get(c['field'])!r}", p)
    case("recreated, a person emptied a field", p, f"{c['field']} stays empty")

    c = cases["recreated_moved"]
    row, p = edge_rows[(c["old_parent"], c["old_id"], "contains")], []
    expect(row["outcome"] == "skipped" and row.get("reason") == "child_has_other_parent", f"row {row}", p)
    expect(parent.get(c["new_id"]) == c["new_parent"], "moved node is not under the person's choice", p)
    case("recreated node moved by a person", p, "old parent arrow skipped: child_has_other_parent")

    c = cases["restored_under_recreated_parent"]
    row, p = node_rows[c["id"]], []
    expect(row["outcome"] == "restored", f"row {row}", p)
    expect(parent.get(c["id"]) == c["parent_new"], f"parent is {parent.get(c['id'])}", p)
    case("not recreated, parent recreated", p, "restored with its old id under the new parent")

    c = cases["recreated_child_of_free_parent"]
    p = []
    expect(node_rows[c["parent_id"]]["outcome"] == "restored", "old parent not restored", p)
    expect(node_rows[c["old_id"]]["outcome"] == "merged", "child not merged", p)
    row = edge_rows[(c["parent_id"], c["old_id"], "contains")]
    expect(row["outcome"] == "merged" and row.get("now") == [c["parent_id"], c["new_id"]], f"row {row}", p)
    expect(parent.get(c["new_id"]) == c["parent_id"], "recreated child is not under its old parent", p)
    case("recreated child, parent not recreated", p, "gets its old parent back")

    c = cases["repointed_onto_recreated"]
    row, p = edge_rows[(c["from"], c["old_to"], "references")], []
    expect(row["outcome"] == "merged" and row.get("reason") == "already_present", f"row {row}", p)
    case("arrow re-pointed onto a recreated node", p, "merged: already_present")

    c = cases["repointed_away"]
    row, p = edge_rows[(c["from"], c["old_to"], "references")], []
    expect(row["outcome"] == "skipped" and row.get("reason") == "removed_by_person", f"row {row}", p)
    expect((c["from"], c["new_to"], "references") not in after.edges, "the arrow came back", p)
    case("arrow re-pointed away from a recreated node", p, "skipped: removed_by_person")

    c = cases["type_differs"]
    row, node, p = node_rows[c["old_id"]], after.nodes.get(c["new_id"], {}), []
    expect(node.get("type") == c["new_type"], f"type is {node.get('type')}", p)
    expect(row.get("not_restored", {}).get("type") == c["old_type"], "old type not listed", p)
    case("recreated with a different type", p, f"type stays {c['new_type']}, report lists {c['old_type']}")

    c = cases["tag_above_root"]
    node, p = after.nodes.get(c["restored_id"], {}), []
    expect(c["tag"] in node.get("effective_tags", []), f"effective_tags {node.get('effective_tags')}", p)
    expect(c["tag"] in replay.nodes[c["node"]]["tags"], "the tag is not on the node above the root", p)
    case("tag added above the deleted root", p, f"restored node carries {c['tag']!r}")

    c = cases["swapped_parent_child"]
    p = []
    row = edge_rows[(c["table_old"], c["column_old"], "contains")]
    expect(row["outcome"] == "skipped" and row.get("reason") == "would_create_cycle", f"row {row}", p)
    row = edge_rows[(c["schema_old"], c["table_old"], "contains")]
    expect(row["outcome"] == "skipped" and row.get("reason") == "child_has_other_parent", f"row {row}", p)
    case("ingest swapped a parent and child", p, "skipped: would_create_cycle")


# ---- the small fixture ------------------------------------------------------------------------------------

# The design's stories that undo one delete, as the report and the end state each one shows. Ids are the
# design's short names: n1 to n6 are the six seed nodes; the ingest makes n7, n8, n9 and n10 (archive).
STORY_SEED = {"n1": "sales", "n2": "sales.orders", "n3": "sales.orders.email", "n4": "sales.orders.total",
              "n5": "sales.refunds", "n6": "reports.daily"}
STORY_INGESTED = {"n7": ("i1", "sales.orders"), "n8": ("i2", "sales.orders.email"),
                  "n9": ("i3", "sales.orders.currency"), "n10": ("i0", "archive")}


def _node(short: str, name: str, outcome: str, **more: Any) -> dict[str, Any]:
    return {"id": short, "qualified_name": name, "outcome": outcome, **more}


def _merged(short: str, name: str, into: str, filled: list[str], kept: list[str], not_restored: dict[str, Any]) -> dict[str, Any]:
    return _node(short, name, "merged", reason="name_in_use", merged_into=into, filled=filled, kept_live=kept,
                 not_restored=not_restored)


def _arrow(f: str, t: str, kind: str, outcome: str, reason: str | None = None, now: list[str] | None = None) -> dict[str, Any]:
    entry: dict[str, Any] = {"from": f, "to": t, "type": kind, "outcome": outcome}
    if reason:
        entry["reason"] = reason
    if now:
        entry["now"] = now
    return entry


STORIES: dict[str, dict[str, Any]] = {
    "ops_d": {
        "name": "the usual case",  # this report is the design's example report, word for word
        "report": {"op_id": "d1", "nodes": [
            _merged("n2", "sales.orders", "n7", ["description", "tags"], ["owner"], {"owner": "asha"}),
            _merged("n3", "sales.orders.email", "n8", [], ["description", "owner", "tags"],
                    {"description": "Customer email", "owner": "dana", "tags": ["contact"]}),
            _node("n4", "sales.orders.total", "restored"),
        ], "edges": [
            _arrow("n1", "n2", "contains", "merged", "already_present", ["n1", "n7"]),
            _arrow("n2", "n3", "contains", "merged", "already_present", ["n7", "n8"]),
            _arrow("n2", "n4", "contains", "merged", "endpoint_merged", ["n7", "n4"]),
            _arrow("n6", "n2", "references", "merged", "endpoint_merged", ["n6", "n7"]),
            _arrow("n6", "n4", "references", "restored"),
        ]},
        "nodes": {
            "sales": {"id": "n1", "tags": ["finance", "gdpr"]},
            "sales.refunds": {"id": "n5", "description": "Approved refunds only", "effective_tags": ["finance", "gdpr"]},
            "sales.orders": {"id": "n7", "parent": "n1", "owner": "ben", "description": "One row per order",
                             "tags": ["pii"], "effective_tags": ["finance", "gdpr", "pii"]},
            "sales.orders.email": {"id": "n8", "parent": "n7", "owner": None, "description": "Hashed customer email",
                                   "tags": ["hashed"], "effective_tags": ["finance", "gdpr", "hashed", "pii"]},
            "sales.orders.total": {"id": "n4", "parent": "n7", "owner": "asha", "description": "Order total, EUR",
                                   "tags": [], "effective_tags": ["finance", "gdpr", "pii"]},
            "sales.orders.currency": {"id": "n9", "parent": "n7", "effective_tags": ["finance", "gdpr", "pii"]},
        },
        "arrows": [("n6", "n7", "references", True), ("n6", "n4", "references", True)],
    },
    "ops_e": {
        "name": "a person moved an arrow away",
        "report": {"op_id": "d1", "nodes": [
            _merged("n2", "sales.orders", "n7", ["description", "owner", "tags"], [], {}),
            _node("n3", "sales.orders.email", "restored"),
            _node("n4", "sales.orders.total", "restored"),
        ], "edges": [
            _arrow("n1", "n2", "contains", "merged", "already_present", ["n1", "n7"]),
            _arrow("n2", "n3", "contains", "merged", "endpoint_merged", ["n7", "n3"]),
            _arrow("n2", "n4", "contains", "merged", "endpoint_merged", ["n7", "n4"]),
            _arrow("n6", "n2", "references", "skipped", "removed_by_person", ["n6", "n7"]),
            _arrow("n6", "n4", "references", "restored"),
        ]},
        "nodes": {
            "sales.orders": {"id": "n7", "parent": "n1", "owner": "asha", "description": "One row per order",
                             "tags": ["pii"], "effective_tags": ["finance", "pii"]},
            "sales.orders.email": {"id": "n3", "parent": "n7", "effective_tags": ["contact", "finance", "pii"]},
            "sales.orders.total": {"id": "n4", "parent": "n7", "effective_tags": ["finance", "pii"]},
        },
        "arrows": [("n6", "n5", "references", True), ("n6", "n7", "references", False), ("n6", "n4", "references", True)],
    },
    "ops_f": {
        "name": "the recreated node was moved",
        "report": {"op_id": "d1", "nodes": [
            _merged("n2", "sales.orders", "n7", ["description", "owner", "tags"], [], {}),
            _node("n3", "sales.orders.email", "restored"),
            _node("n4", "sales.orders.total", "restored"),
        ], "edges": [
            _arrow("n1", "n2", "contains", "skipped", "child_has_other_parent", ["n1", "n7"]),
            _arrow("n2", "n3", "contains", "merged", "endpoint_merged", ["n7", "n3"]),
            _arrow("n2", "n4", "contains", "merged", "endpoint_merged", ["n7", "n4"]),
            _arrow("n6", "n2", "references", "merged", "endpoint_merged", ["n6", "n7"]),
            _arrow("n6", "n4", "references", "restored"),
        ]},
        "nodes": {
            "archive": {"id": "n10", "tags": ["old"]},
            "sales.orders": {"id": "n7", "parent": "n10", "owner": "asha", "description": "One row per order",
                             "tags": ["pii"], "effective_tags": ["old", "pii"]},
            "sales.orders.email": {"id": "n3", "parent": "n7", "effective_tags": ["contact", "old", "pii"]},
            "sales.orders.total": {"id": "n4", "parent": "n7", "effective_tags": ["old", "pii"]},
        },
        "arrows": [("n1", "n7", "contains", False), ("n6", "n7", "references", True)],
    },
}


def check_story(
    res: Results, label: str, story: dict[str, Any], report: dict[str, Any], final: Graph, seed_ids: dict[str, str]
) -> None:
    """The report and the end state of one run, held against what the design's story shows."""
    ids = {short: seed_ids[name] for short, name in STORY_SEED.items()}
    ids.update({short: ingest_node_id(op_id, name) for short, (op_id, name) in STORY_INGESTED.items()})
    short = {full: s for s, full in ids.items()}
    text = json.dumps({k: v for k, v in report.items() if k != "summary"})
    for full, s in short.items():
        text = text.replace(f'"{full}"', f'"{s}"')
    got = json.loads(text)
    problems = []
    if got != story["report"]:
        problems.append("report differs from the story: " + first_difference(
            json.dumps(story["report"], indent=1, sort_keys=True), json.dumps(got, indent=1, sort_keys=True)
        ))
    parent = final.parent_of()
    by_name = {n["qualified_name"]: (nid, n) for nid, n in final.nodes.items()}
    for name, want in story["nodes"].items():
        if name not in by_name:
            problems.append(f"{name} is not live")
            continue
        nid, node = by_name[name]
        for key, value in want.items():
            got_value = short.get(nid) if key == "id" else short.get(parent.get(nid, "")) if key == "parent" else node[key]
            if got_value != value:
                problems.append(f"{name}.{key} is {got_value!r}, the story says {value!r}")
    for f, t, kind, present in story["arrows"]:
        if ((ids[f], ids[t], kind) in final.edges) != present:
            problems.append(f"arrow {f} -> {t} ({kind}) should {'' if present else 'not '}be there")
    res.add(f"story: {story['name']}", label, problems, "report and end state as the design's story shows")


async def check_small(res: Results, small: Path, run: Path) -> None:
    seed = await read_jsonl(small / "seed.jsonl")
    finals: dict[str, Graph] = {}
    names = {rec["qualified_name"]: rec["id"] for rec in seed if rec["kind"] == "node"}
    for d in sorted(p for p in run.iterdir() if p.is_dir()):
        manifest = await read_json(d / "manifest.json")
        replay = Replay(seed)
        replay.apply_all(await read_jsonl(small / manifest["ops"]))
        order: list[str] = manifest["order"]
        label = d.name
        paths = sorted(p for p in d.iterdir() if p.suffix in (".jsonl", ".json") and p.name != "manifest.json")
        files = dict(zip((p.name for p in paths), await asyncio.gather(*(read_text(p) for p in paths))))
        same = Graph.parse(files["applied.jsonl"]).comparable() == Graph.of(replay).comparable()
        res.add("0 apply matches the replay", label, [] if same else ["applied dump differs from the replay"], manifest["ops"])
        for k, op_id in enumerate(order, start=1):
            before, after = Graph.parse(files[f"{k}_before.jsonl"]), Graph.parse(files[f"{k}_after.jsonl"])
            check_undo(res, f"{label} undo {op_id}", replay, op_id, before, after, json.loads(files[f"{k}_report.json"]))
            problems = idempotency_problems(
                files[f"{k}_after.jsonl"], files[f"{k}_after2.jsonl"],
                files[f"{k}_report.json"], files[f"{k}_report2.json"], manifest["writes"][f"{k}_again"],
            )
            res.add("6 idempotent", f"{label} undo {op_id}", problems, "same dump, same report, no writes")
        last = files[f"{len(order)}_after.jsonl"]
        if len(order) > 1:
            # The first delete's report, printed again after the second undo: no change, no writes.
            problems = [] if files["again_after.jsonl"] == last else ["re-printing the report changed the graph"]
            if manifest["writes"]["again"] != 0:
                problems.append(f"re-printing the report made {manifest['writes']['again']} writes")
            res.add("6 idempotent", f"{label} undo {order[0]} again", problems, "same dump, no writes")
        final = finals[label] = Graph.parse(last)
        stem = manifest["ops"].split(".")[0]
        if stem in STORIES:
            check_story(res, label, STORIES[stem], json.loads(files["1_report.json"]), final, names)

        if stem in ("ops_a", "ops_b"):
            res.add(
                "both undone = as loaded",
                label,
                [] if last == files["load.jsonl"] else [first_difference(files["load.jsonl"], last)],
                "dump equals the dump right after load, byte for byte",
            )
            if order[0] == "d1":
                first = json.loads(files["1_report.json"])
                rows = first["nodes"] + first["edges"]
                skipped = [r for r in rows if r["outcome"] == "skipped"]
                p = [] if skipped else ["nothing was blocked by d2"]
                p += [f"{r} does not wait on d2" for r in skipped if r.get("waiting_on") != "d2"]
                p += [f"{r} is {r['outcome']}" for r in rows if r["outcome"] not in ("skipped", "restored")]
                res.add("blocked rows wait on d2", label, p, f"{len(skipped)} rows skipped, waiting_on d2")
                later = json.loads(files["again_report.json"])
                again = later["nodes"] + later["edges"]
                p = [f"{r} not restored after undo d2" for r in again if r["outcome"] != "restored"]
                res.add("retried after undo d2", label, p, f"report of d1 now shows {len(again)} rows restored")
        if stem == "ops_c":
            p = []
            holders = [nid for nid, n in final.nodes.items() if n["qualified_name"] == "sales.orders"]
            if len(holders) != 1:
                p.append(f"{len(holders)} live sales.orders")
            else:
                node = final.nodes[holders[0]]
                seed_orders = next(r for r in seed if r.get("qualified_name") == "sales.orders")
                for field, value in (
                    ("owner", "ben"),
                    ("type", "view"),
                    ("description", seed_orders["description"]),
                    ("tags", seed_orders["tags"]),
                ):
                    if node[field] != value:
                        p.append(f"{field} is {node[field]!r}, expected {value!r}")
                if final.parent_of().get(names["sales.orders.total"]) != holders[0]:
                    p.append("sales.orders.total is not under it")
            res.add("one sales.orders: ben, view, old description, pii", label, p)

    a, b = finals.get("ops_c_d1_d2"), finals.get("ops_c_d2_d1")
    if a is not None and b is not None:
        old_id, new_id = names["sales.orders"], ingest_node_id("i1", "sales.orders")
        swap = {new_id: old_id, old_id: new_id}
        swapped = Graph(
            {swap.get(k, k): v for k, v in b.nodes.items()},
            {(swap.get(f, f), swap.get(t, t), kind) for f, t, kind in b.edges},
        )
        res.add(
            "orders differ only in the surviving id",
            "ops_c both orders",
            [] if swapped.comparable() == a.comparable() else ["the two final graphs differ beyond the id that came back first"],
            "d1 first keeps the old id, d2 first keeps the new one",
        )


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check a scenario run against the eight invariants.")
    parser.add_argument("fixture", type=Path, help="directory with seed.jsonl, ops.jsonl, planted.json")
    parser.add_argument(
        "files", nargs="*", type=Path, metavar="FILE", help="by hand: the dump before undo, the dump after, the report"
    )
    parser.add_argument("--run", type=Path, help="output directory of run_scenario.py (all checks)")
    parser.add_argument("--small", type=Path, help="with --run: directory with the small fixture")
    args = parser.parse_args(argv)
    if bool(args.files) == bool(args.run) or (args.files and len(args.files) != 3):
        parser.error("give BEFORE AFTER REPORT (checks 0-5), or --run (everything)")
    res = Results()
    if args.run is None:
        await check_main(res, args.fixture, *args.files, run=None)
    else:
        main_run = args.run / "main"
        await check_main(
            res, args.fixture, main_run / "before.jsonl", main_run / "after.jsonl", main_run / "report.json", main_run
        )
        if args.small is not None:
            await check_small(res, args.small, args.run / "small")
    ok = res.print()
    if args.run is None:
        print("checks 6, 7 and 8 need the runs run_scenario.py makes: python3 checker.py FIXTURE --run RUN")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
