"""apply: run ops.jsonl in order, one transaction per operation.

Five kinds of operation, each carrying an actor (human or ingest):

    edit_node       human           set description, owner and/or tags of one live node
    add_edge        human, ingest   add a contains or references arrow between two live nodes
    repoint_edge    human, ingest   move one end of a live arrow (new_from_id or new_to_id); counts as one
                                    arrow taken away (kept in edge_removals) and one added
    ingest_node     ingest          by qualified_name: update the type of the live node that has the name,
                                    or create a node with an id built from op_id and name, under its parent
                                    when that parent is live; never sets description, owner or tags
    delete_subtree  human, ingest   remove a node, everything it contains and every arrow touching them;
                                    all of it moves into the journal under the delete's op_id

Who did an operation matters in two places only. Ingest never sets description, owner or tags, so an
edit_node from ingest is refused. People do not create nodes or set types, so an ingest_node from a person
is refused.

An operation that cannot be applied as written (it names a node or arrow that is not live, it would give a
node a second parent or make a node its own ancestor, a field has the wrong shape, ...) is stored as
rejected, with the reason, and changes nothing. A loop never gets into the graph.

An op_id names one operation. One already in the database is skipped when the line is that same operation,
so running apply again on the same file is harmless. If it was used for a different operation, apply stops
with an error before it changes anything.
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from .common import ACTORS, AUTHORED, CONTAINS, EDGE_TYPES, HUMAN, INGEST, InputError, read_jsonl, tags_json, valid_value
from .graph import (
    ancestor_or_self,
    edge_live,
    has_parent,
    insert_edge,
    log_op,
    node_live,
    record_write,
    subtree,
    walk_down,
)
from .store import Store
from .tags import refresh_all, refresh_subtree

# An ingest that creates a node builds the id from the op_id and the name, so the same files give the same
# ids on every run and the fixture generator can refer to ingested nodes in later operations.
INGEST_NAMESPACE = uuid.UUID("2f0c9a52-8a4e-4c55-9d1b-1b0f6d1e7a3c")


def ingest_node_id(op_id: str, qualified_name: str) -> str:
    return str(uuid.uuid5(INGEST_NAMESPACE, f"{op_id}/{qualified_name}"))


class Rejected(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _text(op: dict[str, Any], key: str) -> str:
    value = op.get(key)
    if not isinstance(value, str) or not value:
        raise Rejected("malformed")
    return value


def _edge_type(op: dict[str, Any]) -> str:
    value = op.get("type")
    if value not in EDGE_TYPES:
        raise Rejected("malformed")
    return value


class Applier:
    def __init__(self, store: Store) -> None:
        self.store = store
        self._handlers = {
            "edit_node": self._edit_node,
            "add_edge": self._add_edge,
            "repoint_edge": self._repoint_edge,
            "ingest_node": self._ingest_node,
            "delete_subtree": self._delete_subtree,
        }

    async def apply_file(self, path: str | Path) -> dict[str, Any]:
        records = await read_jsonl(path)
        seen: set[str] = set()
        for lineno, op in records:
            op_id = op.get("op_id")
            if not isinstance(op_id, str) or not op_id:
                raise InputError(f"{path}:{lineno}: op_id must be a non-empty string")
            if op_id in seen:
                raise InputError(f"{path}:{lineno}: op_id {op_id} appears twice")
            seen.add(op_id)
        for lineno, op in records:
            stored = await self.store.fetchval("SELECT payload FROM ops WHERE op_id = ?", (op["op_id"],))
            if stored is not None and json.loads(stored) != op:
                raise InputError(f"{path}:{lineno}: op_id {op['op_id']} was already applied as a different operation")
        summary: dict[str, Any] = {"applied": 0, "rejected": 0, "already_applied": 0, "rejections": {}}
        for _, op in records:
            status, reason = await self.apply_one(op)
            summary[status] += 1
            if status == "rejected":
                summary["rejections"][op["op_id"]] = reason
        return summary

    async def apply_one(self, op: dict[str, Any]) -> tuple[str, str | None]:
        store = self.store
        op_id = op["op_id"]
        async with store.transaction():
            if await store.fetchval("SELECT 1 FROM ops WHERE op_id = ?", (op_id,)):
                return "already_applied", None
            kind, actor = op.get("kind"), op.get("actor")
            seq = await log_op(store, op_id, str(kind), str(actor), op)
            await store.control("SAVEPOINT op_body")
            try:
                if actor not in ACTORS:
                    raise Rejected("unknown_actor")
                handler = self._handlers.get(kind) if isinstance(kind, str) else None
                if handler is None:
                    raise Rejected("unknown_kind")
                await handler(op, seq, actor)
            except Rejected as rejected:
                await store.control("ROLLBACK TO op_body")
                await store.control("RELEASE op_body")
                await store.write(
                    "UPDATE ops SET status = 'rejected', reason = ? WHERE seq = ?", (rejected.reason, seq)
                )
                return "rejected", rejected.reason
            await store.control("RELEASE op_body")
        return "applied", None

    async def _check_new_arrow(self, from_id: str, to_id: str, edge_type: str, *, child_moves: bool = False) -> None:
        """Refuse an arrow that exists already, or a contains arrow that would give its child a second
        parent (unless the child is moving away from its current one) or make a node its own ancestor."""
        if await edge_live(self.store, from_id, to_id, edge_type):
            raise Rejected("edge_exists")
        if edge_type == CONTAINS:
            if not child_moves and await has_parent(self.store, to_id):
                raise Rejected("child_has_parent")
            if await ancestor_or_self(self.store, to_id, from_id):
                raise Rejected("would_create_cycle")

    # ---- the five kinds ----------------------------------------------------------------------------

    async def _edit_node(self, op: dict[str, Any], seq: int, actor: str) -> None:
        if actor != HUMAN:
            raise Rejected("actor_not_allowed")
        node_id = _text(op, "id")
        changes = op.get("set")
        if not isinstance(changes, dict) or not changes:
            raise Rejected("malformed")
        if any(field not in AUTHORED or not valid_value(field, value) for field, value in changes.items()):
            raise Rejected("malformed")
        if not await node_live(self.store, node_id):
            raise Rejected("node_not_live")
        fields = sorted(changes)
        assignments = ", ".join(f"{field} = ?" for field in fields)
        params = [tags_json(changes[f]) if f == "tags" else changes[f] for f in fields]
        await self.store.write(f"UPDATE nodes SET {assignments} WHERE id = ?", (*params, node_id))
        for field in fields:
            await record_write(self.store, node_id, field, seq, actor)
        if "tags" in changes:
            await refresh_subtree(self.store, node_id)

    async def _add_edge(self, op: dict[str, Any], seq: int, actor: str) -> None:
        from_id, to_id, edge_type = _text(op, "from_id"), _text(op, "to_id"), _edge_type(op)
        if not (await node_live(self.store, from_id) and await node_live(self.store, to_id)):
            raise Rejected("endpoint_not_live")
        await self._check_new_arrow(from_id, to_id, edge_type)
        await insert_edge(self.store, from_id, to_id, edge_type)
        if edge_type == CONTAINS:
            await refresh_subtree(self.store, to_id)

    async def _repoint_edge(self, op: dict[str, Any], seq: int, actor: str) -> None:
        from_id, to_id, edge_type = _text(op, "from_id"), _text(op, "to_id"), _edge_type(op)
        has_new_from, has_new_to = op.get("new_from_id") is not None, op.get("new_to_id") is not None
        if has_new_from == has_new_to:
            raise Rejected("malformed")  # exactly one end moves
        moved = _text(op, "new_from_id" if has_new_from else "new_to_id")
        new_from, new_to = (moved, to_id) if has_new_from else (from_id, moved)
        if not await edge_live(self.store, from_id, to_id, edge_type):
            raise Rejected("edge_not_live")
        if not await node_live(self.store, moved):
            raise Rejected("endpoint_not_live")
        await self._check_new_arrow(new_from, new_to, edge_type, child_moves=has_new_from)
        await self.store.write(
            "DELETE FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", (from_id, to_id, edge_type)
        )
        await insert_edge(self.store, new_from, new_to, edge_type)
        await self.store.write(
            "INSERT INTO edge_removals (from_id, to_id, type, seq, actor, op_id) VALUES (?, ?, ?, ?, ?, ?)",
            (from_id, to_id, edge_type, seq, actor, op["op_id"]),
        )
        if edge_type == CONTAINS:
            await refresh_subtree(self.store, to_id)  # moved under a new parent, or lost its parent
            if new_to != to_id:
                await refresh_subtree(self.store, new_to)

    async def _ingest_node(self, op: dict[str, Any], seq: int, actor: str) -> None:
        if actor != INGEST:
            raise Rejected("actor_not_allowed")
        if any(field in op for field in AUTHORED):
            raise Rejected("ingest_sets_authored_field")
        name, node_type = _text(op, "qualified_name"), _text(op, "type")
        parent_name = op.get("parent")
        if parent_name is not None and not isinstance(parent_name, str):
            raise Rejected("malformed")
        holder = await self.store.fetchone("SELECT id, type FROM nodes WHERE qualified_name = ?", (name,))
        if holder is not None:
            holder_id, holder_type = holder
            if holder_type != node_type:
                await self.store.write("UPDATE nodes SET type = ? WHERE id = ?", (node_type, holder_id))
            await record_write(self.store, holder_id, "type", seq, actor)  # the most recent report of the type
            return
        node_id = ingest_node_id(op["op_id"], name)
        parent_id = None
        if parent_name:
            parent_id = await self.store.fetchval("SELECT id FROM nodes WHERE qualified_name = ?", (parent_name,))
        await self.store.write(
            "INSERT INTO nodes (id, qualified_name, type, description, owner, tags, effective_tags) "
            "VALUES (?, ?, ?, NULL, NULL, '[]', '[]')",
            (node_id, name, node_type),
        )
        await record_write(self.store, node_id, "type", seq, actor)
        if parent_id is not None:
            await insert_edge(self.store, parent_id, node_id, CONTAINS)
            await refresh_subtree(self.store, node_id)

    async def _delete_subtree(self, op: dict[str, Any], seq: int, actor: str) -> None:
        root = _text(op, "root_id")
        if not await node_live(self.store, root):
            raise Rejected("node_not_live")
        store = self.store
        rows, children = await subtree(store, root)
        by_id = {row.id: row for row in rows}
        order = [node for node, _ in walk_down([root], children)]  # parents first

        await store.control("CREATE TEMP TABLE IF NOT EXISTS doomed (id TEXT PRIMARY KEY)")
        await store.write("DELETE FROM temp.doomed")
        await store.write_many("INSERT INTO temp.doomed (id) VALUES (?)", [(node_id,) for node_id in order])
        touching = await store.fetchall(
            "SELECT from_id, to_id, type FROM edges "
            "WHERE from_id IN (SELECT id FROM temp.doomed) OR to_id IN (SELECT id FROM temp.doomed)"
        )
        writes: dict[str, dict[str, list[Any]]] = defaultdict(dict)
        for node_id, field, write_seq, write_actor in await store.fetchall(
            "SELECT node_id, field, seq, actor FROM field_writes WHERE node_id IN (SELECT id FROM temp.doomed)"
        ):
            writes[node_id][field] = [write_seq, write_actor]

        await store.write_many(
            "INSERT INTO removed_nodes (delete_op, ord, id, qualified_name, type, description, owner, tags, "
            "parent_id, writes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    op["op_id"],
                    ord_,
                    node_id,
                    by_id[node_id].qualified_name,
                    by_id[node_id].type,
                    by_id[node_id].description,
                    by_id[node_id].owner,
                    by_id[node_id].tags,
                    by_id[node_id].parent_id,
                    json.dumps(writes.get(node_id, {}), sort_keys=True),
                )
                for ord_, node_id in enumerate(order)
            ],
        )
        # Every contains arrow touching the subtree ends inside it: it is the arrow from a node's parent.
        rank = {node_id: i for i, node_id in enumerate(order)}
        parent_arrows = sorted((e for e in touching if e[2] == CONTAINS), key=lambda e: rank[e[1]])
        other_arrows = sorted(e for e in touching if e[2] != CONTAINS)
        await store.write_many(
            "INSERT INTO removed_edges (delete_op, ord, from_id, to_id, type) VALUES (?, ?, ?, ?, ?)",
            [(op["op_id"], i, *edge) for i, edge in enumerate(parent_arrows + other_arrows)],
        )
        # Arrows first, then nodes: the database refuses a node that an arrow still points at.
        await store.write(
            "DELETE FROM edges WHERE from_id IN (SELECT id FROM temp.doomed) OR to_id IN (SELECT id FROM temp.doomed)"
        )
        await store.write("DELETE FROM nodes WHERE id IN (SELECT id FROM temp.doomed)")
        await store.write("DELETE FROM temp.doomed")
        # Recompute effective_tags of the remaining nodes. In a tree no value changes (a node that inherited
        # from the subtree was inside it), so this writes nothing; it keeps the stored values honest anyway.
        await refresh_all(store)
