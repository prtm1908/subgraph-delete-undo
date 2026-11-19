"""redo <op_id>: take back what the undo of one subtree delete put back, on the graph as it is now.

Redo does not replay the delete. Restored nodes go out again, the arrows undo added are removed, and the
fields undo filled get the value they had before. It never removes or changes anything a person did after
the undo.

Which rows: every row of this delete, whichever undo command put it back (a row that waited and was
restored later by the undo of another delete is still a row of this delete). Plus the rows of other deletes
that an undo of this delete retried and put back.

Order of work, one row per step and per transaction:

1. Merged nodes: put back the fields undo filled, unless someone has written the field since.
2. Restored nodes, children before parents: removed again, or kept.
3. The remaining arrows undo added: removed, or kept when they touch a node that was kept.
4. The rows that are left (undo skipped them, or found the arrow already there): nothing to change.

Merges go first because a merge can sit on a node that step 2 takes out. Every step also resets the row it
handled. A row redo kept keeps its undo outcome, which is still true. Every other row of this delete is
opened again: no outcome and no waiting_on, as right after the delete, so a skipped row stops waiting and a
later undo decides every row afresh. A row of another delete goes back to skipped, waiting on this delete.

A restored node is removed again when all of these hold, and kept otherwise:
    no person has written one of its fields since it came back        else kept, written_since_undo
    no other undo that still stands restored it or merged into it     else kept, still_in_use
    every arrow on it is one undo added and this redo takes out,
    and no child it was restored with is still there                  else kept, still_in_use
A write by the ingest does not keep a node. The journal copy is refreshed from the live node as it leaves.

When a later delete got there first (a person deleted something undo put back), redo changes nothing:
a restored node that is gone is unchanged, deleted_since; a merge whose target is gone is unchanged,
target_not_live. Redo never edits the journal of another delete.

Each row gets a redo outcome in columns of its own, marked with the redo that wrote it. A resumed run skips
the rows that carry its own mark, and a finished redo writes nothing when it is run again.
"""

from __future__ import annotations

import json
from typing import Any

from .common import CONTAINS, WRITTEN_FIELDS, tags_json
from .graph import edge_live, node_live, record_write
from .rounds import REDO, check_delete, entry_for, finish
from .store import Store
from .tags import refresh_subtree

REMOVED, KEPT, REVERTED, UNCHANGED = "removed", "kept", "reverted", "unchanged"
Edge = tuple[str, str, str]

# A row this redo handles: a row of this delete, or a row of another delete that an undo of this delete
# put back. ?1 is the delete, ?2 the running number of this redo.
OURS = (
    "(delete_op = ?1 OR (outcome IN ('restored', 'merged') "
    "AND decided_by IN (SELECT seq FROM ops WHERE kind = 'undo' AND target = ?1)))"
)
PENDING = "(redone_by IS NULL OR redone_by != ?2)"


async def redo(store: Store, op_id: str) -> dict[str, Any]:
    """Redo one undone subtree delete and return the report."""
    await check_delete(store, op_id, REDO)
    entry = await entry_for(store, op_id, REDO)  # logged once; a repeated run finds it and writes nothing
    if not entry.finished:
        await RedoRun(store, op_id, entry.seq).run()
        await finish(store, entry)
    return await build_report(store, op_id, entry.seq, entry.round)


class RedoRun:
    def __init__(self, store: Store, op_id: str, redo_seq: int) -> None:
        self.store = store
        self.op_id = op_id
        self.redo_seq = redo_seq  # written next to every redo outcome this run decides
        self.key = (op_id, redo_seq)

    async def run(self) -> None:
        store = self.store
        # 1. Merges, newest first: rows of other deletes were merged last, so they are taken back first.
        merged = await store.fetchall(
            f"SELECT delete_op, id FROM removed_nodes WHERE {OURS} AND {PENDING} AND outcome = 'merged' "
            "ORDER BY (delete_op = ?1), ord DESC",
            self.key,
        )
        for delete_op, node_id in merged:
            await self.merged_step(delete_op, node_id)

        # 2. Restored nodes. Rows with nothing to remove first, then the deepest node first.
        restored = await store.fetchall(
            f"SELECT delete_op, id, ord, reason FROM removed_nodes WHERE {OURS} AND {PENDING} AND outcome = 'restored'",
            self.key,
        )
        depth = await self._depths()
        nothing_to_remove = lambda r: r[3] == "already_live" or r[1] not in depth  # noqa: E731
        restored.sort(key=lambda r: (not nothing_to_remove(r), -depth.get(r[1], 0), r[0] == self.op_id, -r[2]))
        for delete_op, node_id, _, _ in restored:
            await self.node_step(delete_op, node_id)

        # 3. Arrows that are still to decide.
        edges = await store.fetchall(
            f"SELECT delete_op, from_id, to_id, type FROM removed_edges WHERE {OURS} AND {PENDING} "
            "ORDER BY (delete_op = ?1), ord DESC",
            self.key,
        )
        for delete_op, from_id, to_id, edge_type in edges:
            await self.edge_step(delete_op, (from_id, to_id, edge_type))

        # 4. Nodes undo skipped: nothing to take out of the graph, but the row is opened again.
        rest = await store.fetchall(
            f"SELECT id FROM removed_nodes WHERE delete_op = ?1 AND {PENDING} ORDER BY ord DESC", self.key
        )
        for (node_id,) in rest:
            async with store.transaction():
                if await self._node_row(self.op_id, node_id) is not None:
                    await self._close_node(self.op_id, node_id, UNCHANGED, "not_restored_by_undo", reset=True)

    async def _depths(self) -> dict[str, int]:
        """How deep each live node sits under contains arrows. Children are handled before their parents."""
        parent = dict(
            (child, par) for par, child in await self.store.fetchall("SELECT from_id, to_id FROM edges WHERE type = 'contains'")
        )
        depth: dict[str, int] = {}
        for (node,) in await self.store.fetchall("SELECT id FROM nodes"):
            chain = []
            current: str | None = node
            while current is not None and current not in depth and current not in chain:
                chain.append(current)
                current = parent.get(current)
            base = depth.get(current, -1) if current is not None else -1
            for step in reversed(chain):
                base += 1
                depth[step] = base
        return depth

    # ---- steps -------------------------------------------------------------------------------------

    async def merged_step(self, delete_op: str, node_id: str) -> None:
        """One merged node: put back the fields undo filled on the live node, one field at a time."""
        store = self.store
        async with store.transaction():
            row = await self._node_row(delete_op, node_id)
            if row is None or row["outcome"] != "merged":
                return
            target = row["merged_into"]
            if not await node_live(store, target):
                detail = {"target": target, "deleted_by": await self._held_by(target)}
                await self._close_node(delete_op, node_id, UNCHANGED, "target_not_live", detail, reset=True)
                return
            prior = json.loads(row["prior"] or "{}")
            filled = json.loads(row["detail"] or "{}").get("filled", [])
            copied = json.loads(row["writes"])
            current = {
                field: [seq, actor]
                for field, seq, actor in await store.fetchall(
                    "SELECT field, seq, actor FROM field_writes WHERE node_id = ?", (target,)
                )
            }
            put_back: list[str] = []
            kept: dict[str, str] = {}
            for field in WRITTEN_FIELDS:
                if field not in prior:
                    continue
                # Still as undo left it: the field's write record is the one undo copied from the journal.
                if field not in copied or current.get(field) != list(copied[field]):
                    if field in filled:
                        kept[field] = "written_since_undo"
                    continue
                before = prior[field]
                if field in filled:
                    value = tags_json(before["value"]) if field == "tags" else before["value"]
                    await store.write(f"UPDATE nodes SET {field} = ? WHERE id = ?", (value, target))
                    put_back.append(field)
                if before["write"] is None:
                    await store.write("DELETE FROM field_writes WHERE node_id = ? AND field = ?", (target, field))
                else:
                    await record_write(store, target, field, before["write"][0], before["write"][1])
            if "tags" in put_back:
                await refresh_subtree(store, target)
            if put_back:
                detail = {"target": target, "put_back": put_back, "kept": kept}
                await self._close_node(delete_op, node_id, REVERTED, None, detail, reset=True)
            elif kept:
                detail = {"target": target, "kept": kept}
                await self._close_node(delete_op, node_id, UNCHANGED, "written_since_undo", detail, reset=True)
            else:
                await self._close_node(delete_op, node_id, UNCHANGED, "nothing_filled", reset=True)

    async def node_step(self, delete_op: str, node_id: str) -> None:
        """One restored node: removed again with its arrows, or kept."""
        store = self.store
        async with store.transaction():
            row = await self._node_row(delete_op, node_id)
            if row is None or row["outcome"] != "restored":
                return
            if row["reason"] == "already_live":
                # The undo of another delete brought this node back; this undo only found it there.
                await self._close_node(delete_op, node_id, UNCHANGED, "not_restored_by_undo", reset=True)
                return
            if not await node_live(store, node_id):
                await self._deleted_since(delete_op, node_id)
                return
            reason = await self._why_kept(delete_op, node_id, row["decided_by"])
            if reason is not None:
                await self._close_node(delete_op, node_id, KEPT, reason, reset=False)
                return

            arrows = await self._arrows_on(node_id)
            adders = [(arrow, await self._adders(arrow)) for arrow in arrows]
            if any(self._blocks(node_id, arrow, rows) for arrow, rows in adders):
                await self._close_node(delete_op, node_id, KEPT, "still_in_use", reset=False)
                return

            # Out again: its arrows first (the database refuses a node an arrow still points at), then the node.
            below = [to_id for from_id, to_id, edge_type in arrows if edge_type == CONTAINS and from_id == node_id]
            for arrow, rows in adders:
                await store.write("DELETE FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", arrow)
                for edge_delete, edge, outcome in rows:
                    detail = {"was": [arrow[0], arrow[1]]} if outcome == "merged" else None
                    await self._close_edge(edge_delete, edge, REMOVED, None, detail, reset=True)
            live = await store.fetchone("SELECT type, description, owner, tags FROM nodes WHERE id = ?", (node_id,))
            assert live is not None
            writes = {
                field: [seq, actor]
                for field, seq, actor in await store.fetchall(
                    "SELECT field, seq, actor FROM field_writes WHERE node_id = ?", (node_id,)
                )
            }
            # The journal copy is the node as it leaves: a type the ingest reported meanwhile is not lost.
            await store.write(
                "UPDATE removed_nodes SET type = ?, description = ?, owner = ?, tags = ?, writes = ? "
                "WHERE delete_op = ? AND id = ?",
                (*live, json.dumps(writes, sort_keys=True), delete_op, node_id),
            )
            await store.write("DELETE FROM nodes WHERE id = ?", (node_id,))
            for child in below:  # a node that stays (undo had re-aimed its parent arrow here) loses this parent
                await refresh_subtree(store, child)
            await self._close_node(delete_op, node_id, REMOVED, None, reset=True)

    async def edge_step(self, delete_op: str, edge: Edge) -> None:
        """One arrow that did not leave with a node."""
        store = self.store
        async with store.transaction():
            row = await store.fetchone(
                "SELECT outcome, reason, now_from, now_to, decided_by, redone_by FROM removed_edges "
                "WHERE delete_op = ? AND from_id = ? AND to_id = ? AND type = ?",
                (delete_op, *edge),
            )
            if row is None or row[5] == self.redo_seq:
                return
            outcome, reason, now_from, now_to, decided_by, _ = row
            arrow: Edge = (now_from or edge[0], now_to or edge[1], edge[2])
            if not (outcome == "restored" or (outcome == "merged" and reason == "endpoint_merged")):
                # Undo skipped it, or found it already there. A row of another delete keeps saying "already
                # there" while that is true.
                still_there = outcome == "merged" and await edge_live(store, *arrow)
                reset = delete_op == self.op_id or not still_there
                await self._close_edge(delete_op, edge, UNCHANGED, "not_added_by_undo", reset=reset)
                return
            if await self._moved_since(arrow, decided_by):
                await self._close_edge(delete_op, edge, UNCHANGED, "changed_since_undo", reset=True)
            elif not await edge_live(store, *arrow):
                holders = [await self._held_by(end) for end in arrow[:2] if not await node_live(store, end)]
                later = next((holder for holder in holders if holder is not None), None)
                detail = {"deleted_by": later} if later is not None else None
                await self._close_edge(delete_op, edge, UNCHANGED, "deleted_since", detail, reset=True)
            elif await store.fetchval(
                "SELECT 1 FROM removed_nodes WHERE id IN (?, ?) AND redo_outcome = 'kept' AND redone_by = ? LIMIT 1",
                (arrow[0], arrow[1], self.redo_seq),
            ):
                detail = {"now": [arrow[0], arrow[1]]} if outcome == "merged" else None
                await self._close_edge(delete_op, edge, KEPT, "end_kept", detail, reset=False)
            else:
                await store.write("DELETE FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", arrow)
                if arrow[2] == CONTAINS:
                    await refresh_subtree(store, arrow[1])
                detail = {"was": [arrow[0], arrow[1]]} if outcome == "merged" else None
                await self._close_edge(delete_op, edge, REMOVED, None, detail, reset=True)

    # ---- decisions ---------------------------------------------------------------------------------

    async def _why_kept(self, delete_op: str, node_id: str, undo_seq: int | None) -> str | None:
        """The reason a restored node stays, before its arrows are looked at. None when there is none."""
        store = self.store
        if await store.fetchval(
            "SELECT 1 FROM field_writes WHERE node_id = ? AND actor = 'human' AND seq > ? LIMIT 1",
            (node_id, undo_seq or 0),
        ):
            return "written_since_undo"
        # Another undo that still stands worked on this node after it came back: it merged an old node into
        # it, or it found the node live and counts it as restored for its own delete. The merges of this redo
        # were taken back in step 1. A row decided before the node came back says nothing about it now.
        if await store.fetchval(
            "SELECT 1 FROM removed_nodes WHERE merged_into = ?1 AND outcome = 'merged' AND decided_by > ?2 LIMIT 1",
            (node_id, undo_seq or 0),
        ) or await store.fetchval(
            "SELECT 1 FROM removed_nodes WHERE id = ?1 AND outcome = 'restored' AND delete_op != ?3 "
            "AND decided_by > ?2 LIMIT 1",
            (node_id, undo_seq or 0, delete_op),
        ):
            return "still_in_use"
        return None

    async def _arrows_on(self, node_id: str) -> list[Edge]:
        rows = await self.store.fetchall(
            "SELECT from_id, to_id, type FROM edges WHERE from_id = ?1 OR to_id = ?1 ORDER BY from_id, to_id, type",
            (node_id,),
        )
        return [(f, t, kind) for f, t, kind in rows]

    async def _adders(self, arrow: Edge) -> list[tuple[str, Edge, str]]:
        """The journal rows of this redo by which undo added this live arrow, and that nobody moved since.

        Empty when the arrow is someone else's: a person or the ingest added it, or the undo of another
        delete put it there as a row of its own.
        """
        rows = await self.store.fetchall(
            "SELECT delete_op, from_id, to_id, outcome, decided_by FROM removed_edges WHERE type = ?3 AND ("
            "  (outcome = 'restored' AND from_id = ?4 AND to_id = ?5) OR "
            "  (outcome = 'merged' AND reason = 'endpoint_merged' AND now_from = ?4 AND now_to = ?5)) "
            f"AND {OURS} AND {PENDING}",
            (*self.key, arrow[2], arrow[0], arrow[1]),
        )
        return [
            (delete_op, (from_id, to_id, arrow[2]), outcome)
            for delete_op, from_id, to_id, outcome, decided_by in rows
            if not await self._moved_since(arrow, decided_by)
        ]

    @staticmethod
    def _blocks(node_id: str, arrow: Edge, adders: list[tuple[str, Edge, str]]) -> bool:
        """True when this arrow keeps the node in place."""
        if not adders:
            return True  # not an arrow this redo takes out
        # A child that was restored together with its parent and is still there was kept: so is the parent.
        return arrow[2] == CONTAINS and arrow[0] == node_id and any(outcome == "restored" for _, _, outcome in adders)

    async def _moved_since(self, arrow: Edge, undo_seq: int | None) -> bool:
        """Someone re-pointed this arrow away after the undo put it there."""
        return bool(
            await self.store.fetchval(
                "SELECT 1 FROM edge_removals WHERE from_id = ? AND to_id = ? AND type = ? AND seq > ? LIMIT 1",
                (*arrow, undo_seq or 0),
            )
        )

    async def _held_by(self, node_id: str) -> str | None:
        """The delete whose journal holds a node that is not live: the newest one that has not put it back."""
        return await self.store.fetchval(
            "SELECT r.delete_op FROM removed_nodes r JOIN ops o ON o.op_id = r.delete_op "
            "WHERE r.id = ? AND (r.outcome IS NULL OR r.outcome = 'skipped') AND r.delete_op != ? "
            "ORDER BY o.seq DESC LIMIT 1",
            (node_id, self.op_id),
        )

    async def _deleted_since(self, delete_op: str, node_id: str) -> None:
        """A restored node that a later delete removed again: nothing to take out."""
        store = self.store
        later = await store.fetchone(
            "SELECT r.delete_op, r.type, r.description, r.owner, r.tags, r.writes FROM removed_nodes r "
            "JOIN ops o ON o.op_id = r.delete_op WHERE r.id = ? AND r.delete_op != ? "
            "AND (r.outcome IS NULL OR r.outcome = 'skipped') ORDER BY o.seq DESC LIMIT 1",
            (node_id, delete_op),
        )
        detail = None
        if later is not None:
            # Read the later journal, never write it. This journal now has the node as it last was.
            await store.write(
                "UPDATE removed_nodes SET type = ?, description = ?, owner = ?, tags = ?, writes = ? "
                "WHERE delete_op = ? AND id = ?",
                (*later[1:], delete_op, node_id),
            )
            detail = {"deleted_by": later[0]}
        await self._close_node(delete_op, node_id, UNCHANGED, "deleted_since", detail, reset=True)

    # ---- journal rows ------------------------------------------------------------------------------

    async def _node_row(self, delete_op: str, node_id: str) -> dict[str, Any] | None:
        """The row, or None when this redo has already handled it."""
        row = await self.store.fetchone(
            "SELECT outcome, reason, merged_into, writes, detail, prior, decided_by, redone_by FROM removed_nodes "
            "WHERE delete_op = ? AND id = ?",
            (delete_op, node_id),
        )
        if row is None or row[7] == self.redo_seq:
            return None
        return dict(zip(("outcome", "reason", "merged_into", "writes", "detail", "prior", "decided_by"), row))

    async def _close_node(
        self, delete_op: str, node_id: str, outcome: str, reason: str | None, detail: dict[str, Any] | None = None,
        *, reset: bool,
    ) -> None:
        """Write the redo outcome of a node row and, unless it was kept, what the row goes back to."""
        mark = (outcome, reason, None if detail is None else json.dumps(detail, ensure_ascii=False), self.redo_seq)
        where = (delete_op, node_id)
        sql = "UPDATE removed_nodes SET redo_outcome = ?, redo_reason = ?, redo_detail = ?, redone_by = ?"
        if not reset:
            await self.store.write(f"{sql} WHERE delete_op = ? AND id = ?", (*mark, *where))
        elif delete_op == self.op_id:  # open again, as right after the delete
            await self.store.write(
                f"{sql}, outcome = NULL, reason = NULL, merged_into = NULL, waiting_on = NULL, detail = NULL, "
                "decided_by = NULL, prior = NULL WHERE delete_op = ? AND id = ?",
                (*mark, *where),
            )
        else:  # a row of another delete: what it was before the undo of this delete retried it
            await self.store.write(
                f"{sql}, outcome = 'skipped', reason = 'parent_not_live', merged_into = NULL, waiting_on = ?, "
                "detail = NULL, prior = NULL, "
                "decided_by = (SELECT MAX(seq) FROM ops WHERE kind = 'undo' AND target = ?) "
                "WHERE delete_op = ? AND id = ?",
                (*mark, self.op_id, delete_op, *where),
            )

    async def _close_edge(
        self, delete_op: str, edge: Edge, outcome: str, reason: str | None, detail: dict[str, Any] | None = None,
        *, reset: bool,
    ) -> None:
        """Write the redo outcome of an arrow row and, unless it was kept, what the row goes back to."""
        mark = (outcome, reason, None if detail is None else json.dumps(detail, ensure_ascii=False), self.redo_seq)
        where = (delete_op, *edge)
        sql = "UPDATE removed_edges SET redo_outcome = ?, redo_reason = ?, redo_detail = ?, redone_by = ?"
        tail = "WHERE delete_op = ? AND from_id = ? AND to_id = ? AND type = ?"
        if not reset:
            await self.store.write(f"{sql} {tail}", (*mark, *where))
        elif delete_op == self.op_id:
            await self.store.write(
                f"{sql}, outcome = NULL, reason = NULL, waiting_on = NULL, now_from = NULL, now_to = NULL, "
                f"decided_by = NULL {tail}",
                (*mark, *where),
            )
        else:
            await self.store.write(
                f"{sql}, outcome = 'skipped', reason = 'endpoint_not_live', waiting_on = ?, now_from = NULL, "
                f"now_to = NULL, decided_by = (SELECT MAX(seq) FROM ops WHERE kind = 'undo' AND target = ?) {tail}",
                (*mark, self.op_id, delete_op, *where),
            )


# ---- report --------------------------------------------------------------------------------------


async def build_report(store: Store, op_id: str, redo_seq: int, round_: int) -> dict[str, Any]:
    """One entry for every node and arrow the delete removed, printed from the redo outcomes on its rows.

    Rows of other deletes that this redo sent back to waiting (or kept) are listed apart, each with the
    delete it belongs to.
    """
    undo_label = await store.fetchval(
        "SELECT op_id FROM ops WHERE kind = 'undo' AND target = ? AND round = ?", (op_id, round_)
    )
    node_rows = await store.fetchall(
        "SELECT delete_op, id, qualified_name, redo_outcome, redo_reason, redo_detail, redone_by FROM removed_nodes "
        "WHERE delete_op = ?1 OR redone_by = ?2 ORDER BY (delete_op != ?1), delete_op, ord",
        (op_id, redo_seq),
    )
    edge_rows = await store.fetchall(
        "SELECT delete_op, from_id, to_id, type, redo_outcome, redo_reason, redo_detail, redone_by, waiting_on "
        "FROM removed_edges WHERE delete_op = ?1 OR redone_by = ?2 ORDER BY (delete_op != ?1), delete_op, ord",
        (op_id, redo_seq),
    )
    waiting = {
        (delete_op, node_id)
        for delete_op, node_id in await store.fetchall(
            "SELECT delete_op, id FROM removed_nodes WHERE redone_by = ? AND outcome = 'skipped' AND waiting_on = ?",
            (redo_seq, op_id),
        )
    }

    def entry(head: dict[str, Any], delete_op: str, outcome: str, reason: str | None, detail: str | None,
              mine: bool, waits: bool) -> dict[str, Any]:
        item = dict(head) if delete_op == op_id else {"delete": delete_op, **head}
        item["outcome"] = outcome if mine else None  # None: this redo has not reached the row
        if mine and reason is not None:
            item["reason"] = reason
        if mine and detail is not None:
            item.update(json.loads(detail))
        if waits:
            item["waiting_on"] = op_id
        return item

    nodes: list[dict[str, Any]] = []
    other_nodes: list[dict[str, Any]] = []
    for delete_op, node_id, name, outcome, reason, detail, redone_by in node_rows:
        item = entry({"id": node_id, "qualified_name": name}, delete_op, outcome, reason, detail,
                     redone_by == redo_seq, (delete_op, node_id) in waiting)
        (nodes if delete_op == op_id else other_nodes).append(item)
    edges: list[dict[str, Any]] = []
    other_edges: list[dict[str, Any]] = []
    for delete_op, from_id, to_id, edge_type, outcome, reason, detail, redone_by, waiting_on in edge_rows:
        item = entry({"from": from_id, "to": to_id, "type": edge_type}, delete_op, outcome, reason, detail,
                     redone_by == redo_seq, delete_op != op_id and waiting_on == op_id)
        (edges if delete_op == op_id else other_edges).append(item)

    report: dict[str, Any] = {
        "op_id": op_id,
        "redo_of": undo_label,
        "summary": {"nodes": _count(nodes, (REMOVED, KEPT, REVERTED, UNCHANGED)),
                    "edges": _count(edges, (REMOVED, KEPT, UNCHANGED))},
        "nodes": nodes,
        "edges": edges,
    }
    if other_nodes or other_edges:
        report["other_nodes"] = other_nodes
        report["other_edges"] = other_edges
    return report


def _count(entries: list[dict[str, Any]], outcomes: tuple[str, ...]) -> dict[str, int]:
    counts = dict.fromkeys(outcomes, 0)
    for item in entries:
        if item["outcome"] in counts:
            counts[item["outcome"]] += 1
    return counts
