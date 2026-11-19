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

Redo (the extension), with --run. For every redo, from the dump just before it, the dump just after it and
its report:

    R1  no human change lost             redo adds nothing; every value a person wrote after the undo is as
                                         it was; a field only changes where the report says it was put back
    R2  report accounts for every row    every node and arrow the delete removed exactly once; each outcome
                                         agrees with the dumps; nothing left the graph without being reported
    R3  graph whole                      names unique, no arrow to a missing node, effective_tags correct
    R4  result as worked out here        the graph after redo is the one this checker works out on its own
                                         from the dumps around the undo and the changes made since
                                         (runs where one delete was undone)

and for the main scenario: redo straight after undo gives the dump from before the undo, byte for byte; a
second redo changes nothing; undo after redo gives the first undo's dump and report again; crashed redos
finish to the same dump and report; a redo after a crashed undo, and an undo after a crashed redo, are
refused. Then the fourteen redo stories of the small fixture, each held against what its story shows.
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
    res: Results, scope: str, replay: Replay, delete_op: str, before: Graph, after: Graph, report: dict[str, Any],
    deleted_again: frozenset[str] = frozenset(),
) -> None:
    """deleted_again: nodes of this delete that a later delete removed again and that are still gone. That
    can only happen when this is not the first undo of the delete. They belong to the later delete: this
    undo must not bring them back (check 1), so they are not expected back either (check 2)."""
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
    problems += [f"node {nid}, deleted again by a later delete, is back" for nid in sorted(deleted_again) if nid in after.nodes]
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
        if snap["qualified_name"] in live_names or nid in deleted_again:
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


# ---- the checks for one redo ----------------------------------------------------------------------------

FIELDS = ("type", *AUTHORED)
NODE_OUTCOMES = ("removed", "kept", "reverted", "unchanged")
EDGE_OUTCOMES = ("removed", "kept", "unchanged")


def human_fields_of(ops: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """(node, field) for every field a person set in these operations."""
    return {
        (op["id"], field)
        for op in ops
        if op.get("kind") == "edit_node" and op.get("actor") == "human"
        for field in op.get("set", {})
    }


def predict_redo(u_before: Graph, u_after: Graph, before: Graph, human: set[tuple[str, str]]) -> Graph:
    """The graph a redo must leave, worked out from dumps alone. For runs where one delete was undone.

    u_before and u_after are the dumps around the undo, so their difference is what undo put back.
    before is the dump just before the redo. A node undo put back leaves again unless a person wrote on it
    since, an arrow that undo did not add touches it, or a child that undo put back stays. An arrow undo
    added leaves unless it touches such a node that stays. A field undo changed goes back unless it was
    changed again since.
    """
    put_nodes = set(u_after.nodes) - set(u_before.nodes)
    put_edges = u_after.edges - u_before.edges
    written = {node for node, _ in human}
    touching: dict[str, list[Edge]] = defaultdict(list)
    for edge in before.edges:
        touching[edge[0]].append(edge)
        touching[edge[1]].append(edge)
    parent = before.parent_of()

    def depth(node: str) -> int:
        d = 0
        while node in parent:
            node, d = parent[node], d + 1
        return d

    stays: dict[str, bool] = {}
    for node in sorted(put_nodes & set(before.nodes), key=depth, reverse=True):  # children first
        stays[node] = (
            node in written
            or any(edge not in put_edges for edge in touching[node])
            or any(stays.get(t, False) for f, t, kind in touching[node] if kind == "contains" and f == node)
        )
    gone = {node for node, stay in stays.items() if not stay}
    nodes: dict[str, dict[str, Any]] = {}
    for nid, node in before.nodes.items():
        if nid in gone:
            continue
        fields = dict(node)
        was, filled = u_before.nodes.get(nid), u_after.nodes.get(nid)
        if was is not None and filled is not None:
            for field in FIELDS:
                if was[field] != filled[field] and node[field] == filled[field] and (nid, field) not in human:
                    fields[field] = was[field]
        nodes[nid] = fields
    edges = set()
    for edge in before.edges:
        if edge[0] in gone or edge[1] in gone:
            continue
        if edge in put_edges and not (stays.get(edge[0], False) or stays.get(edge[1], False)):
            continue
        edges.add(edge)
    return Graph(nodes, edges)


def check_redo(
    res: Results, scope: str, removed: dict[str, Any], removed_edges: set[Edge], before: Graph, after: Graph,
    report: dict[str, Any], human: set[tuple[str, str]], around_undo: tuple[Graph, Graph] | None,
) -> None:
    # R1. Nothing a person did after the undo is lost or altered.
    problems = [f"node {n} appeared" for n in sorted(set(after.nodes) - set(before.nodes))]
    problems += [f"arrow {e} appeared" for e in sorted(after.edges - before.edges)]
    for nid, field in sorted(human):
        if nid in before.nodes and nid not in after.nodes:
            problems.append(f"{nid} (written by a person after the undo) is gone")
        elif nid in before.nodes and after.nodes[nid][field] != before.nodes[nid][field]:
            problems.append(f"{nid}.{field}: {before.nodes[nid][field]!r} -> {after.nodes[nid][field]!r}")
    put_back = {
        (e.get("target"), field)
        for e in report.get("nodes", []) + report.get("other_nodes", [])
        if e.get("outcome") == "reverted"
        for field in e.get("put_back", [])
    }
    for nid in sorted(set(after.nodes) & set(before.nodes)):
        for field in ("qualified_name", *FIELDS):
            if after.nodes[nid][field] != before.nodes[nid][field] and (nid, field) not in put_back:
                problems.append(f"{nid}.{field} changed, and the report does not say it was put back")
    res.add("R1 no human change lost", scope, problems, f"{len(human)} values a person wrote after the undo")

    # R2. The report accounts for every removed node and arrow once, and agrees with the dumps.
    problems = []
    nodes, arrows = report.get("nodes", []), report.get("edges", [])
    node_ids = Counter(e["id"] for e in nodes)
    edge_keys = Counter((e["from"], e["to"], e["type"]) for e in arrows)
    problems += [f"node {k} reported {n} times" for k, n in node_ids.items() if n != 1]
    problems += [f"arrow {k} reported {n} times" for k, n in edge_keys.items() if n != 1]
    problems += [f"node {k} missing from report" for k in sorted(set(removed) - set(node_ids))]
    problems += [f"node {k} in report but not removed" for k in sorted(set(node_ids) - set(removed))]
    problems += [f"arrow {k} missing from report" for k in sorted(removed_edges - set(edge_keys))]
    problems += [f"arrow {k} in report but not removed" for k in sorted(set(edge_keys) - removed_edges)]
    said_nodes: set[str] = set()
    said_edges: set[Edge] = set()
    for e in nodes + report.get("other_nodes", []):
        outcome, nid = e.get("outcome"), e["id"]
        if outcome not in NODE_OUTCOMES:
            problems.append(f"node {nid}: outcome {outcome!r}")
        elif outcome in ("kept", "unchanged") and not e.get("reason"):
            problems.append(f"node {nid}: {outcome} without a reason")
        elif outcome == "removed":
            said_nodes.add(nid)
            if nid not in before.nodes or nid in after.nodes:
                problems.append(f"node {nid}: removed, but it was not live before or is still live")
        elif outcome == "kept" and nid not in after.nodes:
            problems.append(f"node {nid}: kept but not live")
        elif outcome == "unchanged" and (nid in after.nodes) != (nid in before.nodes):
            problems.append(f"node {nid}: unchanged, but it came or went")
        elif outcome == "reverted":
            was, now = before.nodes.get(e.get("target")), after.nodes.get(e.get("target"))
            if was is None or now is None or not e.get("put_back"):
                problems.append(f"node {nid}: reverted, but the target is not live or nothing was put back")
            else:
                problems += [f"node {nid}: kept {f}, but it changed" for f in e.get("kept", {}) if now[f] != was[f]]
    for e in arrows + report.get("other_edges", []):
        edge = (e["from"], e["to"], e["type"])
        outcome = e.get("outcome")
        if outcome not in EDGE_OUTCOMES:
            problems.append(f"arrow {edge}: outcome {outcome!r}")
        elif outcome in ("kept", "unchanged") and not e.get("reason"):
            problems.append(f"arrow {edge}: {outcome} without a reason")
        elif outcome == "removed":
            live = (*e["was"], e["type"]) if e.get("was") else edge
            said_edges.add(live)
            if live not in before.edges or live in after.edges:
                problems.append(f"arrow {edge}: removed, but {live} was not live before or is still live")
        elif outcome == "kept":
            live = (*e["now"], e["type"]) if e.get("now") else edge
            if live not in after.edges:
                problems.append(f"arrow {edge}: kept, but {live} is not live")
    problems += [f"node {n} left the graph without being reported" for n in sorted(set(before.nodes) - set(after.nodes) - said_nodes)]
    problems += [f"arrow {e} left the graph without being reported" for e in sorted(before.edges - after.edges - said_edges)]
    res.add("R2 report accounts for every row once", scope, problems, f"{len(nodes)} nodes, {len(arrows)} arrows")

    # R3. The graph stays whole.
    problems = [f"{name} is used twice" for name in after.duplicate_names()]
    problems += after.tag_problems() + [f"arrow {e} to a missing node" for e in after.dangling()]
    res.add("R3 graph whole", scope, problems, f"{len(after.nodes)} live nodes, tags recomputed")

    # R4. The result is the one worked out here, independently, from the dumps.
    if around_undo is not None:
        want = predict_redo(*around_undo, before, human)
        problems = [f"node {n} should have left" for n in sorted(set(after.nodes) - set(want.nodes))][:3]
        problems += [f"node {n} should have stayed" for n in sorted(set(want.nodes) - set(after.nodes))][:3]
        problems += [
            f"{n}.{f} is {after.nodes[n][f]!r}, expected {want.nodes[n][f]!r}"
            for n in sorted(set(want.nodes) & set(after.nodes)) for f in FIELDS if after.nodes[n][f] != want.nodes[n][f]
        ][:3]
        problems += [f"arrow {e} should have left" for e in sorted(after.edges - want.edges)][:3]
        problems += [f"arrow {e} should have stayed" for e in sorted(want.edges - after.edges)][:3]
        left = len(before.nodes) - len(after.nodes)
        res.add("R4 result as worked out here", scope, problems, f"{left} nodes out again, {len(before.edges) - len(after.edges)} arrows")


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
    if run is not None:
        await check_main_redo(res, run, replay, delete_op, before_text, after_text, report_text)


async def crash_problems(crash: Path, total: int, want_dump: str, want_report: str) -> tuple[list[str], list[str]]:
    """Crashed runs of one command: each must finish to the same dump and report, with whole dumps between."""
    problems: list[str] = []
    plans: list[str] = []
    for d in sorted(p for p in crash.iterdir() if p.is_dir()):
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
        if final != want_dump:
            problems.append(f"{d.name}: final dump differs: " + first_difference(want_dump, final))
        if final_report != want_report:
            problems.append(f"{d.name}: final report differs: " + first_difference(want_report, final_report))
    return problems, plans


async def check_main_redo(
    res: Results, run: Path, replay: Replay, delete_op: str, before_text: str, after_text: str, report_text: str
) -> None:
    """Redo on the main scenario: straight after undo, twice, round 2 with changes in between, crashes."""
    d = run / "redo"
    names = ("report.json", "after.jsonl", "report2.json", "after2.jsonl", "undo2_report.json", "undo2_after.jsonl",
             "between_after.jsonl", "partial_report.json", "partial_after.jsonl")
    files = dict(zip(names, await asyncio.gather(*(read_text(d / name) for name in names))))
    meta, between, between_summary = await asyncio.gather(
        read_json(d / "meta.json"), read_jsonl(d / "between.jsonl"), read_json(d / "between.json")
    )
    deleted = replay.deletes[delete_op]
    removed, removed_edges = deleted["nodes"], deleted["edges"]
    before, after = Graph.parse(before_text), Graph.parse(after_text)
    redone = Graph.parse(files["after.jsonl"])

    scope = "main redo"
    check_redo(res, scope, removed, removed_edges, after, redone, json.loads(files["report.json"]), set(), (before, after))
    res.add(
        "redo straight after undo = before the undo", scope,
        [] if files["after.jsonl"] == before_text else [first_difference(before_text, files["after.jsonl"])],
        "dump equals the dump taken before undo, byte for byte",
    )
    problems = idempotency_problems(
        files["after.jsonl"], files["after2.jsonl"], files["report.json"], files["report2.json"], meta["second_redo_writes"]
    )
    res.add("redo idempotent", scope, [p.replace("undo", "redo") for p in problems], "second redo: same dump, same report, no writes")

    scope = "main undo after redo"
    problems = [] if files["undo2_after.jsonl"] == after_text else ["dump differs: " + first_difference(after_text, files["undo2_after.jsonl"])]
    problems += [] if files["undo2_report.json"] == report_text else ["report differs: " + first_difference(report_text, files["undo2_report.json"])]
    res.add("undo after redo = the first undo", scope, problems, "same dump and same report, byte for byte")
    again = Graph.parse(files["undo2_after.jsonl"])
    check_undo(res, scope, replay, delete_op, redone, again, json.loads(files["undo2_report.json"]))

    scope = "main redo, changes since undo"
    problems = [] if between_summary["applied"] == len(between) else [f"only {between_summary['applied']} of {len(between)} applied"]
    changed = Graph.parse(files["between_after.jsonl"])
    partial = Graph.parse(files["partial_after.jsonl"])
    partial_report = json.loads(files["partial_report.json"])
    kept = [e for e in partial_report["nodes"] if e["outcome"] == "kept"]
    reasons = Counter(e.get("reason") for e in kept)
    problems += [] if reasons["written_since_undo"] and reasons["still_in_use"] else [f"expected both reasons to keep a node, got {dict(reasons)}"]
    res.add("changes between undo and redo applied", scope, problems,
            f"{len(between)} operations; redo kept {len(kept)} nodes: {dict(reasons)}")
    check_redo(res, scope, removed, removed_edges, changed, partial, partial_report, human_fields_of(between), (redone, again))

    scope = "main redo"
    problems, plans = await crash_problems(d / "crash", meta["redo_writes"], files["after.jsonl"], files["report.json"])
    res.add("redo resumable", scope, problems,
            f"{len(plans)} runs of a {meta['redo_writes']}-write redo, CRASH_AFTER = {', '.join(plans)}")

    problems = []
    for name, want_final in (("redo_after_crashed_undo", before_text), ("undo_after_crashed_redo", after_text)):
        r = d / "refused" / name
        rmeta, mid, after_refused, final = await asyncio.gather(
            read_json(r / "meta.json"), read_text(r / "mid.jsonl"), read_text(r / "after_refused.jsonl"), read_text(r / "final.jsonl")
        )
        if rmeta["crash_exit"] != CRASH_EXIT_CODE:
            problems.append(f"{name}: the first command did not crash (exit {rmeta['crash_exit']})")
        if rmeta["exit"] != 1 or "not finished" not in rmeta["stderr"]:
            problems.append(f"{name}: not refused: exit {rmeta['exit']}, {rmeta['stderr']!r}")
        if after_refused != mid:
            problems.append(f"{name}: the refused command changed the graph")
        if final != want_final:
            problems.append(f"{name}: after finishing both commands: " + first_difference(want_final, final))
    res.add("unfinished command blocks the other", scope, problems,
            "redo after a crashed undo, undo after a crashed redo: refused, nothing changes")


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
    total = meta["undo_writes"]
    problems, plans = await crash_problems(run / "crash", total, after_text, report_text)
    res.add("7 resumable", scope, problems, f"{len(plans)} runs of a {total}-write undo, CRASH_AFTER = {', '.join(plans)}")

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


# ---- the redo stories ---------------------------------------------------------------------------------------

# The design's example redo report, word for word (its first redo story).
REDO_REPORT = {"op_id": "d1", "redo_of": "undo:d1", "nodes": [
    {"id": "n2", "qualified_name": "sales.orders", "outcome": "reverted", "target": "n7",
     "put_back": ["description", "tags"], "kept": {}},
    {"id": "n3", "qualified_name": "sales.orders.email", "outcome": "unchanged", "reason": "nothing_filled"},
    {"id": "n4", "qualified_name": "sales.orders.total", "outcome": "removed"},
], "edges": [
    {"from": "n1", "to": "n2", "type": "contains", "outcome": "unchanged", "reason": "not_added_by_undo"},
    {"from": "n2", "to": "n3", "type": "contains", "outcome": "unchanged", "reason": "not_added_by_undo"},
    {"from": "n2", "to": "n4", "type": "contains", "outcome": "removed", "was": ["n7", "n4"]},
    {"from": "n6", "to": "n2", "type": "references", "outcome": "removed", "was": ["n6", "n7"]},
    {"from": "n6", "to": "n4", "type": "references", "outcome": "removed"},
]}

KEPT_END = ("kept", "end_kept")
WAITS = {"waiting_on": "d2"}

# What each story must show. Steps are numbered from 1; dump 0 is the one right after apply.
#   same      (k, j): the dump after step k equals the dump after step j ("load": the dump right after load)
#   same_out  (k, j): step k printed the same report as step j
#   zero      steps that must write nothing
#   exits     the exit status of every step
#   refused   steps that must be refused because the other command is unfinished
#   report    the whole report of a step, with the design's short ids
#   rows      rows of a step's report: "n2" a node, "n1>n2" an arrow, "d1:n2" a row of another delete;
#             each (outcome, reason) or (outcome, reason, fields the entry must have)
#   all       every row of the delete has this outcome, and no row of another delete is listed
#   only      the graph after a step is the seed with only these deletes applied
#   fields    values a live node must have after a step
#   single    one delete is undone in this story, so the result of each redo is also worked out here (R4)
REDO_EXPECT: dict[str, dict[str, Any]] = {
    "straight": {
        "single": True, "same": [(2, 0), (3, 0), (4, 1), (5, 0)], "zero": [3], "same_out": [(3, 2), (4, 1)],
        "report": {2: REDO_REPORT},
    },
    "edited_restored_node": {
        "single": True,
        "rows": {3: {"n3": ("removed", None), "n4": ("kept", "written_since_undo"), "n2": ("kept", "still_in_use"),
                     "n2>n3": ("removed", None), "n1>n2": KEPT_END, "n2>n4": KEPT_END, "n6>n2": KEPT_END, "n6>n4": KEPT_END}},
        "fields": {3: {"n4": {"description": "Order total with tax"}}},
    },
    "rewrote_filled_field": {
        "single": True,
        "rows": {3: {"n2": ("reverted", None, {"put_back": ["tags"], "kept": {"description": "written_since_undo"}}),
                     "n3": ("unchanged", "nothing_filled"), "n4": ("removed", None)}},
        "fields": {3: {"n7": {"description": "Orders, one row each", "tags": [], "owner": "ben",
                              "effective_tags": ["finance", "gdpr"]}}},
    },
    "two_undos_redo_d2": {
        "same": [(3, 1), (4, 3)], "zero": [4],
        "rows": {3: {"n6": ("removed", None), "d1:n6>n2": ("removed", None, WAITS), "d1:n6>n4": ("removed", None, WAITS)},
                 4: {"n6>n2": ("skipped", "endpoint_not_live", WAITS), "n6>n4": ("skipped", "endpoint_not_live", WAITS),
                     "n2": ("restored", None)}},
    },
    "two_undos_redo_d1": {"all": {3: "removed"}, "only": {3: ["d1"]}},
    "parent_redo_d1": {"all": {3: "removed"}, "only": {3: ["d1"]}},
    "parent_redo_d2": {
        "same": [(3, 0), (4, 3)], "zero": [4],
        "rows": {3: {"n1": ("removed", None), "n5": ("removed", None), "n1>n5": ("removed", None),
                     "d1:n2": ("removed", None, WAITS), "d1:n3": ("removed", None, WAITS), "d1:n4": ("removed", None, WAITS),
                     "d1:n1>n2": ("removed", None, WAITS), "d1:n6>n4": ("removed", None, WAITS)},
                 4: {"n2": ("skipped", "parent_not_live", WAITS), "n4": ("skipped", "parent_not_live", WAITS),
                     "n6>n2": ("skipped", "endpoint_not_live", WAITS)}},
    },
    "rows_were_waiting": {"same": [(2, 1)], "all": {2: "unchanged"}, "only": {3: ["d1"]}},
    "deleted_again": {
        # The later delete d3 owns total: a second undo of d1 leaves it out and waits; undo d3 brings it back.
        "single": True, "same": [(4, 2), (5, "load")],
        "rows": {3: {"n4": ("unchanged", "deleted_since", {"deleted_by": "d3"}), "n2": ("removed", None), "n3": ("removed", None),
                     "n6>n4": ("unchanged", "deleted_since", {"deleted_by": "d3"})},
                 4: {"n2": ("restored", None), "n3": ("restored", None), "n4": ("skipped", "deleted_again", {"waiting_on": "d3"}),
                     "n2>n4": ("skipped", "endpoint_not_live", {"waiting_on": "d3"})},
                 5: {"n4": ("restored", None), "n2>n4": ("restored", None), "n6>n4": ("restored", None)},
                 6: {"n4": ("restored", "already_live"), "n2>n4": ("merged", "already_present")}},
        "zero": [6],
    },
    "kept_then_deleted": {
        # Redo keeps total (a person edited it) and its parent. A later delete d3 then removes total. The row
        # of d1 still says restored, so a new undo of d1 has to look at it again: it waits on d3.
        "single": True, "same": [(6, 2), (7, 6)], "zero": [7],
        "rows": {3: {"n3": ("removed", None), "n4": ("kept", "written_since_undo"), "n2": ("kept", "still_in_use")},
                 5: {"n2": ("restored", None), "n3": ("restored", None), "n4": ("skipped", "deleted_again", {"waiting_on": "d3"}),
                     "n2>n4": ("skipped", "endpoint_not_live", {"waiting_on": "d3"}),
                     "n6>n4": ("skipped", "endpoint_not_live", {"waiting_on": "d3"})},
                 6: {"n4": ("restored", None)},
                 7: {"n4": ("restored", "already_live")}},
        "fields": {6: {"n4": {"description": "Order total with tax"}}},
    },
    "merge_target_deleted": {
        "single": True, "same": [(3, 2), (4, 1)],
        "rows": {3: {"n2": ("unchanged", "target_not_live", {"target": "n7", "deleted_by": "d3"}),
                     "n3": ("unchanged", "deleted_since"), "n4": ("unchanged", "deleted_since"),
                     "n1>n2": ("unchanged", "not_added_by_undo")}},
    },
    "two_undos_one_node_blocked": {"same": [(3, 2)], "rows": {3: {"n8": ("kept", "still_in_use"), "n2>n8": KEPT_END}}},
    "two_undos_one_node_clean": {
        "same": [(4, 0)],
        "rows": {3: {"n3": ("reverted", None, {"target": "n8", "put_back": ["description", "owner", "tags"]})},
                 4: {"n8": ("removed", None), "n2>n8": ("removed", None)}},
    },
    "refused_while_unfinished": {
        "single": True, "exits": [CRASH_EXIT_CODE, 1, 0, CRASH_EXIT_CODE, 1, 0], "refused": [2, 5],
        "same": [(2, 1), (5, 4), (6, 0)],
    },
}


async def check_redo_stories(res: Results, small: Path, run: Path) -> None:
    seed = await read_jsonl(small / "seed.jsonl")
    names = {rec["qualified_name"]: rec["id"] for rec in seed if rec["kind"] == "node"}
    ids = {short: names[name] for short, name in STORY_SEED.items()}
    ids.update({short: ingest_node_id(op_id, name) for short, (op_id, name) in STORY_INGESTED.items()})
    short = {full: s for s, full in ids.items()}

    def shorten(text: str) -> str:
        for full, s in short.items():
            text = text.replace(f'"{full}"', f'"{s}"')
        return text

    seen = sorted(p.name for p in run.iterdir() if p.is_dir())
    res.add("every redo story was run", "redo stories", [] if seen == sorted(REDO_EXPECT) else [f"ran {seen}"], f"{len(seen)} stories")
    for d in sorted(p for p in run.iterdir() if p.is_dir() and p.name in REDO_EXPECT):
        label, want = f"redo: {d.name}", REDO_EXPECT[d.name]
        manifest = await read_json(d / "manifest.json")
        steps: list[dict[str, Any]] = manifest["steps"]
        ops = await read_jsonl(small / manifest["ops"])
        replay = Replay(seed)
        replay.apply_all(ops)
        paths = sorted(p for p in d.iterdir() if p.suffix in (".jsonl", ".json") and p.name != "manifest.json")
        files = dict(zip((p.name for p in paths), await asyncio.gather(*(read_text(p) for p in paths))))
        dumps: dict[Any, str] = {k: files[f"{k}_dump.jsonl"] for k in range(len(steps) + 1)}
        dumps["load"] = files["load.jsonl"]
        later = {s["arg"]: await read_jsonl(small / s["arg"]) for s in steps if s["command"] == "apply"}

        # The checks every redo gets, from the dump before it, the dump after it and its report.
        human: set[tuple[str, str]] = set()  # written by a person since the last undo
        gone_later: set[str] = set()  # nodes removed by an operation applied after the first undo
        around: dict[str, tuple[int, int]] = {}  # delete -> the dumps around its last undo
        started: dict[str, int] = {}
        redone: set[str] = set()
        for k, step in enumerate(steps, start=1):
            command, arg, ok = step["command"], step["arg"], step["exit"] == 0
            if command == "apply":
                human |= human_fields_of(later[arg])
                gone_later |= set(Graph.parse(dumps[k - 1]).nodes) - set(Graph.parse(dumps[k]).nodes)
            elif command == "undo":
                started.setdefault(arg, k - 1)
                if ok:
                    around[arg] = (started.pop(arg), k)
                    human = set()
                    if arg in redone and arg in replay.deletes:
                        # An undo after a redo passes the checks of a first undo.
                        was = Graph.parse(dumps[k - 1])
                        check_undo(res, f"{label}, undo {arg} again", replay, arg, was, Graph.parse(dumps[k]),
                                   json.loads(files[f"{k}_out.json"]), frozenset(gone_later - set(was.nodes)))
            elif command == "redo" and ok and step["writes"]:
                deleted = replay.deletes[arg]
                span = around.get(arg)
                check_redo(
                    res, f"{label}, step {k}", deleted["nodes"], deleted["edges"], Graph.parse(dumps[k - 1]),
                    Graph.parse(dumps[k]), json.loads(files[f"{k}_out.json"]), human,
                    (Graph.parse(dumps[span[0]]), Graph.parse(dumps[span[1]])) if want.get("single") and span else None,
                )
                redone.add(arg)

        # What this story shows.
        problems: list[str] = []
        exits = want.get("exits", [0] * len(steps))
        problems += [
            f"step {k} ({s['command']} {s['arg']}): exit {s['exit']}, expected {e}: {s['stderr'].splitlines()[-1] if s['stderr'] else ''}"
            for k, (s, e) in enumerate(zip(steps, exits), start=1) if s["exit"] != e
        ]
        if problems:  # a step failed, so there is no report to hold against the story
            res.add("story", label, problems)
            continue
        for k in want.get("refused", []):
            if "not finished" not in steps[k - 1]["stderr"]:
                problems.append(f"step {k} was not refused as unfinished: {steps[k - 1]['stderr']!r}")
        for k, j in want.get("same", []):
            if dumps[k] != dumps[j]:
                problems.append(f"dump after step {k} differs from dump {j}: " + first_difference(dumps[j], dumps[k]))
        for k, j in want.get("same_out", []):
            if files[f"{k}_out.json"] != files[f"{j}_out.json"]:
                problems.append(f"step {k} printed a different report than step {j}")
        problems += [f"step {k} made {steps[k - 1]['writes']} writes" for k in want.get("zero", []) if steps[k - 1]["writes"] != 0]
        for k, expected in want.get("report", {}).items():
            got = json.loads(shorten(files[f"{k}_out.json"]))
            got.pop("summary", None)
            if got != expected:
                problems.append(f"step {k}: report differs from the design: " + first_difference(
                    json.dumps(expected, indent=1, sort_keys=True), json.dumps(got, indent=1, sort_keys=True)))
        for k, rows in want.get("rows", {}).items():
            report = json.loads(shorten(files[f"{k}_out.json"]))
            got_rows: dict[str, dict[str, Any]] = {}
            for key in ("nodes", "edges", "other_nodes", "other_edges"):
                for e in report.get(key, []):
                    name = e["id"] if "id" in e else f"{e['from']}>{e['to']}"
                    got_rows[f"{e['delete']}:{name}" if "delete" in e else name] = e
            for name, (outcome, reason, *more) in rows.items():
                e = got_rows.get(name)
                if e is None:
                    problems.append(f"step {k}: no row {name}")
                elif e["outcome"] != outcome or e.get("reason") != reason:
                    problems.append(f"step {k}: {name} is {e['outcome']} ({e.get('reason')}), the story says {outcome} ({reason})")
                else:
                    problems += [f"step {k}: {name}.{f} is {e.get(f)!r}, the story says {v!r}" for f, v in (more[0] if more else {}).items() if e.get(f) != v]
        for k, outcome in want.get("all", {}).items():
            report = json.loads(files[f"{k}_out.json"])
            problems += [f"step {k}: {e} is not {outcome}" for e in report["nodes"] + report["edges"] if e["outcome"] != outcome]
            if report.get("other_nodes") or report.get("other_edges"):
                problems.append(f"step {k}: rows of another delete are listed")
        for k, applied in want.get("only", {}).items():
            partial = Replay(seed)
            partial.apply_all([op for op in ops if op["op_id"] in applied])
            if Graph.parse(dumps[k]).comparable() != Graph.of(partial).comparable():
                problems.append(f"after step {k} the graph is not the seed with only {applied} applied")
        for k, nodes in want.get("fields", {}).items():
            graph = Graph.parse(dumps[k])
            for s, fields in nodes.items():
                node = graph.nodes.get(ids[s])
                problems += [f"after step {k}: {s} is not live"] if node is None else [
                    f"after step {k}: {s}.{f} is {node[f]!r}, the story says {v!r}" for f, v in fields.items() if node[f] != v
                ]
        res.add("story", label, problems, f"{len(steps)} steps as the design's story shows")


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
            await check_redo_stories(res, args.small, args.run / "redo")
    ok = res.print()
    if args.run is None:
        print("checks 6, 7 and 8 need the runs run_scenario.py makes: python3 checker.py FIXTURE --run RUN")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
