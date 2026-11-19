"""undo <op_id>: apply the inverse of one subtree delete to the graph as it is now.

Order of work:

1. The delete's removed nodes, parents first. For each: decide restored, merged or skipped; make the
   change; decide the arrow from its parent; refresh the tags of the node and everything below it.
2. The delete's remaining removed arrows (the references ones), each decided the same way.
3. Rows of earlier undos that are waiting on this delete, newest journal first, parents first.
4. Own rows that were skipped although their node or arrow is live after step 3: decided once more.
5. The report, printed from the journal rows.

The undo is logged as an operation of its own the first time it runs (by a person), and every journal row
records which undo decided it. A merge also records what the live node held before (the row's `prior`), and
the report lists any written value it replaced. That is what redo needs. An undo after a redo is a new
round with a new entry (rounds.py): it decides the rows redo opened again, against the graph as it is then.
A row redo kept still has its outcome, and that outcome can have stopped being true since: a kept node was
deleted again, a kept arrow was moved away. So before deciding, undo opens every row of this delete that
says something is in the graph when it is not, and decides those too. The first undo of a delete has no
such row.

Each row is one step and one transaction: its graph change, its tag refresh and its outcome in the journal
are saved together or not at all. A row that already has an outcome is never decided again, so a second run
writes nothing and a run that resumes after a crash carries on where the last committed step left off.
Each decision looks only at the live graph and the journal, so the resumed run decides as an uninterrupted
run would have.

Node decision
    name used by a live node      -> merged into that node (reason name_in_use)
    else parent live (or merged)  -> restored with its old id and all its fields
    else                          -> skipped, reason parent_not_live, waiting_on the delete holding the parent
    no parent at delete time      -> restored at the top level (when the name is free)
    its own id holds the name     -> restored, reason already_live: the undo of another delete that covers
                                     this node brought it back first, and there is nothing to add
    a newer delete removed it     -> skipped, reason deleted_again, waiting_on that delete: the node came back
    again and still stands           once (an earlier undo), was deleted again, and is that delete's now.
                                     This comes before the parent check and can only happen after a redo.

Merge, field by field (description, owner, tags, and type): the most recent write wins. The seed counts as
written at seq 0; a value put back by an undo keeps the seq of its original write. Everything in a journal
was written before its delete, and anything written on a live node after the delete is newer, so a value a
person wrote after the delete is never replaced. effective_tags is never filled: it is recomputed.

Arrow decision, after resolving each end to where it is now (same id, the node it was merged into, or not
live), first match wins:
    an end is not live                       -> skipped endpoint_not_live (waiting_on that end's delete)
    the arrow already exists                 -> merged already_present
    contains, and the child has a parent     -> skipped child_has_other_parent (the later placement wins)
    a person re-pointed it away after delete -> skipped removed_by_person
    contains, and it would close a loop      -> skipped would_create_cycle
    otherwise                                -> added: restored (same ends) or merged endpoint_merged
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .common import CONTAINS, WRITTEN_FIELDS, tags_json
from .graph import ancestor_or_self, edge_live, has_parent, insert_edge, node_live, record_write
from .rounds import UNDO, CommandError, check_delete, entry_for, finish
from .store import Store
from .tags import refresh_subtree

RESTORED, MERGED, SKIPPED = "restored", "merged", "skipped"
Edge = tuple[str, str, str]

NODE_ROW_SQL = """
SELECT delete_op, id, qualified_name, type, description, owner, tags, parent_id, writes, outcome, waiting_on
FROM removed_nodes WHERE delete_op = ? AND id = ?
"""


UndoError = CommandError  # undo was asked for something it cannot do; nothing has been written


@dataclass(frozen=True)
class Place:
    """Where a node id is now: a live id, or not live and the delete that has to be undone first."""

    live_id: str | None
    waiting_on: str | None = None


@dataclass(frozen=True)
class NodeRow:
    delete_op: str
    id: str
    qualified_name: str
    type: str
    description: str | None
    owner: str | None
    tags: str
    parent_id: str | None
    writes: str
    outcome: str | None
    waiting_on: str | None


def _pending(outcome: str | None, row_waiting_on: str | None, retrying_for: str | None) -> bool:
    """A row still to decide: no outcome yet, or (when retrying) skipped while waiting on this delete."""
    if retrying_for is None:
        return outcome is None
    return outcome == SKIPPED and row_waiting_on == retrying_for


async def undo(store: Store, op_id: str) -> dict[str, Any]:
    """Undo one subtree delete and return its report."""
    await check_delete(store, op_id, UNDO)
    entry = await entry_for(store, op_id, UNDO)  # logged once; a repeated run finds it and writes nothing
    if not entry.finished:
        await UndoRun(store, op_id, entry.seq).run()
        await finish(store, entry)
    return await build_report(store, op_id)


class UndoRun:
    def __init__(self, store: Store, op_id: str, undo_seq: int) -> None:
        self.store = store
        self.op_id = op_id
        self.undo_seq = undo_seq  # the running number of this undo, written next to every outcome it decides

    async def run(self) -> None:
        await self._reopen_stale()
        await self._decide_rows(self.op_id, retrying_for=None)
        await self.retry_waiting()
        await self._settle()

    async def _settle(self) -> None:
        """Own rows that were skipped, although their node or arrow is in the graph when this undo ends.

        A row of this delete waits for something another delete holds. Then a retried row of an older
        delete, which covers the same node, brings that node back in this same undo. The waiting row is
        decided once more, so the report does not say skipped for something that is live.
        """
        store = self.store
        nodes = await store.fetchall(
            "SELECT id FROM removed_nodes WHERE delete_op = ? AND outcome = 'skipped' "
            "AND id IN (SELECT id FROM nodes) ORDER BY ord",
            (self.op_id,),
        )
        for (node_id,) in nodes:
            async with store.transaction():
                row = await self._node_row(self.op_id, node_id)
                if row is not None and row.outcome == SKIPPED and await node_live(store, node_id):
                    await self._mark_node(row, RESTORED, "already_live")
        edges = await store.fetchall(
            "SELECT from_id, to_id, type FROM removed_edges WHERE delete_op = ? AND outcome = 'skipped' "
            "AND waiting_on IS NOT NULL ORDER BY ord",
            (self.op_id,),
        )
        for from_id, to_id, edge_type in edges:
            async with store.transaction():
                ends = (await self.where(from_id), await self.where(to_id))
                if all(end.live_id is not None for end in ends):  # nothing it waited for is missing any more
                    added = await self._decide_edge(self.op_id, (from_id, to_id, edge_type))
                    if added is not None and edge_type == CONTAINS:
                        await refresh_subtree(store, added[1])

    async def _reopen_stale(self) -> None:
        """Open the rows of this delete whose outcome is no longer true, so they are decided again.

        A row says "restored" (or, for an arrow, "added at these ends") and the node or arrow is not in the
        graph: redo kept it, and a later delete or a re-point took it away since. One row, one transaction.
        """
        store = self.store
        nodes = await store.fetchall(
            "SELECT id FROM removed_nodes WHERE delete_op = ? AND outcome = 'restored' "
            "AND id NOT IN (SELECT id FROM nodes) ORDER BY ord",
            (self.op_id,),
        )
        for (node_id,) in nodes:
            async with store.transaction():
                await store.write(
                    "UPDATE removed_nodes SET outcome = NULL, reason = NULL, merged_into = NULL, waiting_on = NULL, "
                    "detail = NULL, decided_by = NULL, prior = NULL WHERE delete_op = ? AND id = ? AND outcome = 'restored'",
                    (self.op_id, node_id),
                )
        edges = await store.fetchall(
            "SELECT r.from_id, r.to_id, r.type FROM removed_edges r WHERE r.delete_op = ? "
            "AND (r.outcome = 'restored' OR (r.outcome = 'merged' AND r.reason = 'endpoint_merged')) "
            "AND NOT EXISTS (SELECT 1 FROM edges e WHERE e.from_id = COALESCE(r.now_from, r.from_id) "
            "                AND e.to_id = COALESCE(r.now_to, r.to_id) AND e.type = r.type) ORDER BY r.ord",
            (self.op_id,),
        )
        for edge in edges:
            async with store.transaction():
                await store.write(
                    "UPDATE removed_edges SET outcome = NULL, reason = NULL, waiting_on = NULL, now_from = NULL, "
                    "now_to = NULL, decided_by = NULL WHERE delete_op = ? AND from_id = ? AND to_id = ? AND type = ?",
                    (self.op_id, *edge),
                )

    async def retry_waiting(self) -> None:
        """Rows of earlier undos that were skipped because this delete held a node they need.

        A row can only be blocked by its own journal or a newer one (everything it needs was live when its
        delete ran), so newer journals go first, and inside a journal nodes parents first, then arrows.
        """
        journals = await self.store.fetchall(
            """
            SELECT w.delete_op FROM (
                SELECT delete_op FROM removed_nodes WHERE outcome = 'skipped' AND waiting_on = ?1
                UNION
                SELECT delete_op FROM removed_edges WHERE outcome = 'skipped' AND waiting_on = ?1
            ) w JOIN ops o ON o.op_id = w.delete_op
            ORDER BY o.seq DESC
            """,
            (self.op_id,),
        )
        for (delete_op,) in journals:
            await self._decide_rows(delete_op, retrying_for=self.op_id)

    async def _decide_rows(self, delete_op: str, retrying_for: str | None) -> None:
        """The pending rows of one journal: nodes parents first, then arrows, one step each."""
        if retrying_for is None:
            where, params = "outcome IS NULL", (delete_op,)
        else:
            where, params = "outcome = 'skipped' AND waiting_on = ?", (delete_op, retrying_for)
        nodes = await self.store.fetchall(
            f"SELECT id FROM removed_nodes WHERE delete_op = ? AND {where} ORDER BY ord", params
        )
        for (node_id,) in nodes:
            await self.node_step(delete_op, node_id, retrying_for)
        edges = await self.store.fetchall(
            f"SELECT from_id, to_id, type FROM removed_edges WHERE delete_op = ? AND {where} ORDER BY ord", params
        )
        for from_id, to_id, edge_type in edges:
            await self.edge_step(delete_op, (from_id, to_id, edge_type), retrying_for)

    # ---- steps -------------------------------------------------------------------------------------

    async def node_step(self, delete_op: str, node_id: str, retrying_for: str | None) -> None:
        """One removed node and the arrow from its parent, in one transaction."""
        store = self.store
        async with store.transaction():
            row = await self._node_row(delete_op, node_id)
            if row is None or not _pending(row.outcome, row.waiting_on, retrying_for):
                return
            parent_arrow = (row.parent_id, row.id, CONTAINS) if row.parent_id is not None else None
            holder = await store.fetchval("SELECT id FROM nodes WHERE qualified_name = ?", (row.qualified_name,))
            if holder == row.id:
                # Another delete covers this node too, and its undo has already brought it back.
                await self._mark_node(row, RESTORED, "already_live")
                target = holder
            elif holder is not None:
                detail, prior = await self._merge_into(row, holder)
                await self._mark_node(row, MERGED, "name_in_use", merged_into=holder, detail=detail, prior=prior)
                target = holder
            else:
                later = await self._deleted_again_by(delete_op, row.id)
                if later is not None:
                    # Undoing this delete must not take back a later delete of the same node.
                    await self._mark_node(row, SKIPPED, "deleted_again", waiting_on=later)
                    if parent_arrow is not None:
                        await self._mark_edge(delete_op, parent_arrow, SKIPPED, "endpoint_not_live", waiting_on=later)
                    return
                if parent_arrow is not None:
                    parent = await self.where(parent_arrow[0])
                    if parent.live_id is None:
                        await self._mark_node(row, SKIPPED, "parent_not_live", waiting_on=parent.waiting_on)
                        await self._mark_edge(
                            delete_op, parent_arrow, SKIPPED, "endpoint_not_live", waiting_on=parent.waiting_on
                        )
                        return
                await store.write(
                    "INSERT INTO nodes (id, qualified_name, type, description, owner, tags, effective_tags) "
                    "VALUES (?, ?, ?, ?, ?, ?, '[]')",
                    (row.id, row.qualified_name, row.type, row.description, row.owner, row.tags),
                )
                await self._mark_node(row, RESTORED)
                target = row.id
            if parent_arrow is not None and await self._edge_pending(delete_op, parent_arrow, retrying_for):
                await self._decide_edge(delete_op, parent_arrow)
            # Recompute what this step can have changed: the node and everything now below it.
            await refresh_subtree(store, target)

    async def edge_step(self, delete_op: str, edge: Edge, retrying_for: str | None) -> None:
        """One removed arrow, in one transaction."""
        async with self.store.transaction():
            if not await self._edge_pending(delete_op, edge, retrying_for):
                return
            added = await self._decide_edge(delete_op, edge)
            if added is not None and edge[2] == CONTAINS:
                await refresh_subtree(self.store, added[1])

    # ---- decisions ---------------------------------------------------------------------------------

    async def where(self, node_id: str) -> Place:
        """The live node that stands for node_id now, or the delete that has to be undone first.

        Follows merges (a merged id lives on as the node it was folded into). A node only disappears through
        a delete, so a node that is not live is always held by some journal row.
        """
        current = node_id
        seen: set[str] = set()
        while current not in seen:
            seen.add(current)
            if await node_live(self.store, current):
                return Place(current)
            rows = await self.store.fetchall(
                "SELECT r.outcome, r.merged_into, r.waiting_on, r.delete_op "
                "FROM removed_nodes r JOIN ops o ON o.op_id = r.delete_op WHERE r.id = ? ORDER BY o.seq DESC",
                (current,),
            )
            merged = next((r for r in rows if r[0] == MERGED), None)
            if merged is not None:
                current = merged[1]
                continue
            holder = next((r for r in rows if r[0] is None or r[0] == SKIPPED), None)
            if holder is None:
                return Place(None)
            outcome, _, waiting_on, delete_op = holder
            # Held by a delete nobody has undone yet, or by a row that is itself waiting on another delete.
            return Place(None, delete_op if outcome is None else waiting_on)
        return Place(None)

    async def _deleted_again_by(self, delete_op: str, node_id: str) -> str | None:
        """The newer delete that removed this same node again and has not been undone, if there is one.

        The node was live in between, so an earlier undo of this delete had restored it. An undone newer
        delete does not count: its row is decided, or skipped while it waits for something else.
        """
        return await self.store.fetchval(
            "SELECT r.delete_op FROM removed_nodes r JOIN ops o ON o.op_id = r.delete_op "
            "WHERE r.id = ? AND r.outcome IS NULL AND o.seq > (SELECT seq FROM ops WHERE op_id = ?) "
            "ORDER BY o.seq DESC LIMIT 1",
            (node_id, delete_op),
        )

    async def _decide_edge(self, delete_op: str, edge: Edge) -> tuple[str, str] | None:
        """Decide one removed arrow and write the outcome. Returns the arrow added, if one was."""
        store = self.store
        from_id, to_id, edge_type = edge
        ends = (await self.where(from_id), await self.where(to_id))
        if any(end.live_id is None for end in ends):
            waiting_on = next((end.waiting_on for end in ends if end.live_id is None and end.waiting_on), None)
            await self._mark_edge(delete_op, edge, SKIPPED, "endpoint_not_live", waiting_on=waiting_on)
            return None
        now = (ends[0].live_id, ends[1].live_id)
        assert now[0] is not None and now[1] is not None
        if await edge_live(store, now[0], now[1], edge_type):
            await self._mark_edge(delete_op, edge, MERGED, "already_present", now=now)
            return None
        if edge_type == CONTAINS and await has_parent(store, now[1]):
            await self._mark_edge(delete_op, edge, SKIPPED, "child_has_other_parent", now=now)
            return None
        if await store.fetchval(
            "SELECT 1 FROM edge_removals WHERE from_id = ? AND to_id = ? AND type = ? AND actor = 'human' "
            "AND seq > (SELECT seq FROM ops WHERE op_id = ?) LIMIT 1",
            (*now, edge_type, delete_op),
        ):
            await self._mark_edge(delete_op, edge, SKIPPED, "removed_by_person", now=now)
            return None
        if edge_type == CONTAINS and await ancestor_or_self(store, now[1], now[0]):
            await self._mark_edge(delete_op, edge, SKIPPED, "would_create_cycle", now=now)
            return None
        await insert_edge(store, now[0], now[1], edge_type)
        if now == (from_id, to_id):
            await self._mark_edge(delete_op, edge, RESTORED)
        else:
            await self._mark_edge(delete_op, edge, MERGED, "endpoint_merged", now=now)
        return now

    async def _merge_into(self, row: NodeRow, holder: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Fold a removed node into the live node that holds its name.

        Returns the report detail, and `prior`: for every field whose value or write record changes, what the
        live node held before ({"value": ..., "write": [seq, actor] or null}), so a redo can put it back.
        """
        store = self.store
        live = await store.fetchone("SELECT type, description, owner, tags FROM nodes WHERE id = ?", (holder,))
        assert live is not None
        live_values = {"type": live[0], "description": live[1], "owner": live[2], "tags": json.loads(live[3])}
        old_values = {
            "type": row.type,
            "description": row.description,
            "owner": row.owner,
            "tags": json.loads(row.tags),
        }
        live_writes = {
            field: (seq, actor)
            for field, seq, actor in await store.fetchall(
                "SELECT field, seq, actor FROM field_writes WHERE node_id = ?", (holder,)
            )
        }
        old_writes = {field: (seq, actor) for field, (seq, actor) in json.loads(row.writes).items()}

        filled: list[str] = []
        kept_live: list[str] = []
        not_restored: dict[str, Any] = {}
        take_value: list[str] = []
        take_write: list[str] = []
        for field in WRITTEN_FIELDS:
            old_w, live_w = old_writes.get(field), live_writes.get(field)
            old_is_newer = old_w is not None and (live_w is None or old_w[0] > live_w[0])
            differs = old_values[field] != live_values[field]
            if field == "type" and not differs:
                # Same type either way: nothing to report. Keep the newest report time for later merges.
                if old_is_newer:
                    take_write.append(field)
                continue
            if old_is_newer:
                filled.append(field)
                take_value.append(field)
                take_write.append(field)
            else:
                kept_live.append(field)
                if old_w is not None and differs:
                    not_restored[field] = old_values[field]

        prior = {
            field: {"value": live_values[field], "write": list(live_writes[field]) if field in live_writes else None}
            for field in take_write
        }
        # A filled field that overwrote a value someone had written on the live node: show what was there.
        replaced = {
            field: live_values[field]
            for field in take_value
            if field in live_writes and live_values[field] != old_values[field]
        }
        if take_value:
            assignments = ", ".join(f"{field} = ?" for field in take_value)
            params = [tags_json(old_values[f]) if f == "tags" else old_values[f] for f in take_value]
            await store.write(f"UPDATE nodes SET {assignments} WHERE id = ?", (*params, holder))
        for field in take_write:
            seq, actor = old_writes[field]
            await record_write(store, holder, field, seq, actor)
        detail: dict[str, Any] = {"filled": filled, "kept_live": kept_live, "not_restored": not_restored}
        if replaced:
            detail["replaced"] = replaced
        return detail, prior

    # ---- journal rows ------------------------------------------------------------------------------

    async def _node_row(self, delete_op: str, node_id: str) -> NodeRow | None:
        row = await self.store.fetchone(NODE_ROW_SQL, (delete_op, node_id))
        return None if row is None else NodeRow(*row)

    async def _edge_pending(self, delete_op: str, edge: Edge, retrying_for: str | None) -> bool:
        row = await self.store.fetchone(
            "SELECT outcome, waiting_on FROM removed_edges WHERE delete_op = ? AND from_id = ? AND to_id = ? "
            "AND type = ?",
            (delete_op, *edge),
        )
        return row is not None and _pending(row[0], row[1], retrying_for)

    async def _mark_node(
        self,
        row: NodeRow,
        outcome: str,
        reason: str | None = None,
        *,
        merged_into: str | None = None,
        waiting_on: str | None = None,
        detail: dict[str, Any] | None = None,
        prior: dict[str, Any] | None = None,
    ) -> None:
        await self.store.write(
            "UPDATE removed_nodes SET outcome = ?, reason = ?, merged_into = ?, waiting_on = ?, detail = ?, "
            "decided_by = ?, prior = ? WHERE delete_op = ? AND id = ?",
            (
                outcome,
                reason,
                merged_into,
                waiting_on,
                None if detail is None else json.dumps(detail, ensure_ascii=False),
                self.undo_seq,
                None if prior is None else json.dumps(prior, ensure_ascii=False),
                row.delete_op,
                row.id,
            ),
        )

    async def _mark_edge(
        self,
        delete_op: str,
        edge: Edge,
        outcome: str,
        reason: str | None = None,
        *,
        now: tuple[str, str] | None = None,
        waiting_on: str | None = None,
    ) -> None:
        await self.store.write(
            "UPDATE removed_edges SET outcome = ?, reason = ?, waiting_on = ?, now_from = ?, now_to = ?, decided_by = ? "
            "WHERE delete_op = ? AND from_id = ? AND to_id = ? AND type = ?",
            (outcome, reason, waiting_on, *(now or (None, None)), self.undo_seq, delete_op, *edge),
        )


# ---- report --------------------------------------------------------------------------------------


async def build_report(store: Store, op_id: str) -> dict[str, Any]:
    """The report is a printout of the delete's journal rows: one entry per removed node and arrow."""
    node_rows = await store.fetchall(
        "SELECT id, qualified_name, outcome, reason, merged_into, waiting_on, detail "
        "FROM removed_nodes WHERE delete_op = ? ORDER BY ord",
        (op_id,),
    )
    edge_rows = await store.fetchall(
        "SELECT from_id, to_id, type, outcome, reason, waiting_on, now_from, now_to "
        "FROM removed_edges WHERE delete_op = ? ORDER BY ord",
        (op_id,),
    )
    nodes: list[dict[str, Any]] = []
    for node_id, name, outcome, reason, merged_into, waiting_on, detail in node_rows:
        entry: dict[str, Any] = {"id": node_id, "qualified_name": name, "outcome": outcome}
        if reason is not None:
            entry["reason"] = reason
        if merged_into is not None:
            entry["merged_into"] = merged_into
        if detail is not None:
            entry.update(json.loads(detail))
        if waiting_on is not None:
            entry["waiting_on"] = waiting_on
        nodes.append(entry)
    edges: list[dict[str, Any]] = []
    for from_id, to_id, edge_type, outcome, reason, waiting_on, now_from, now_to in edge_rows:
        entry = {"from": from_id, "to": to_id, "type": edge_type, "outcome": outcome}
        if reason is not None:
            entry["reason"] = reason
        if now_from is not None and outcome != RESTORED:
            entry["now"] = [now_from, now_to]
        if waiting_on is not None:
            entry["waiting_on"] = waiting_on
        edges.append(entry)
    summary = {"nodes": _count(nodes), "edges": _count(edges)}
    return {"op_id": op_id, "summary": summary, "nodes": nodes, "edges": edges}


def _count(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts = {RESTORED: 0, MERGED: 0, SKIPPED: 0}
    for entry in entries:
        counts[entry["outcome"]] += 1
    return counts


def format_report(report: dict[str, Any]) -> str:
    """JSON with one node or arrow per line, so the report reads like a table and diffs cleanly.

    Used for the report of undo and of redo: a list becomes a block of lines, anything else one line.
    """

    def part(key: str, value: Any) -> str:
        if not isinstance(value, list):
            return f'  "{key}": {json.dumps(value, ensure_ascii=False)}'
        if not value:
            return f'  "{key}": []'
        body = ",\n".join("    " + json.dumps(item, ensure_ascii=False) for item in value)
        return f'  "{key}": [\n{body}\n  ]'

    return "{\n" + ",\n".join(part(key, value) for key, value in report.items()) + "\n}\n"
