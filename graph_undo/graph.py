"""Reads and writes that apply, undo and the tag propagator share: the live graph and the operation log."""

from __future__ import annotations

import json
from collections import defaultdict, deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, NamedTuple

from .common import utc_now
from .store import Store

# A node and everything below it along contains arrows, each with its contains parent. UNION (not
# UNION ALL) drops a node it has already found, so the walk ends even if a contains loop ever existed.
SUBTREE_SQL = """
WITH RECURSIVE sub(id) AS (
    SELECT ?
    UNION
    SELECT e.to_id FROM edges e JOIN sub ON e.from_id = sub.id WHERE e.type = 'contains'
)
SELECT n.id, n.qualified_name, n.type, n.description, n.owner, n.tags, n.effective_tags,
       (SELECT p.from_id FROM edges p WHERE p.to_id = n.id AND p.type = 'contains')
FROM sub JOIN nodes n ON n.id = sub.id
"""

# Walk up from the second id along contains arrows; 1 when the first id is on the way (or is the second).
ANCESTOR_OR_SELF_SQL = """
WITH RECURSIVE up(id) AS (
    SELECT ?2
    UNION
    SELECT e.from_id FROM edges e JOIN up ON e.to_id = up.id WHERE e.type = 'contains'
)
SELECT 1 FROM up WHERE id = ?1 LIMIT 1
"""


class SubtreeRow(NamedTuple):
    id: str
    qualified_name: str
    type: str
    description: str | None
    owner: str | None
    tags: str
    effective_tags: str
    parent_id: str | None


def walk_down(roots: Iterable[str], children: Mapping[str, Sequence[str]]) -> Iterator[tuple[str, str | None]]:
    """Every node below the roots once, parents before children, as (node, parent). Siblings in id order."""
    queue: deque[tuple[str, str | None]] = deque((root, None) for root in roots)
    seen: set[str] = set()
    while queue:
        node, parent = queue.popleft()
        if node in seen:
            continue
        seen.add(node)
        yield node, parent
        queue.extend((child, node) for child in sorted(children.get(node, ())))


async def subtree(store: Store, root: str) -> tuple[list[SubtreeRow], dict[str, list[str]]]:
    """The rows of root and everything below it, and the children of each of them."""
    rows = [SubtreeRow(*row) for row in await store.fetchall(SUBTREE_SQL, (root,))]
    children: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        if row.id != root and row.parent_id is not None:
            children[row.parent_id].append(row.id)
    return rows, children


async def node_live(store: Store, node_id: str) -> bool:
    return bool(await store.fetchval("SELECT 1 FROM nodes WHERE id = ?", (node_id,)))


async def edge_live(store: Store, from_id: str, to_id: str, edge_type: str) -> bool:
    return bool(
        await store.fetchval(
            "SELECT 1 FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", (from_id, to_id, edge_type)
        )
    )


async def has_parent(store: Store, node_id: str) -> bool:
    return bool(await store.fetchval("SELECT 1 FROM edges WHERE to_id = ? AND type = 'contains'", (node_id,)))


async def ancestor_or_self(store: Store, ancestor: str, node_id: str) -> bool:
    """True when ancestor is node_id itself or above it along contains arrows."""
    return bool(await store.fetchval(ANCESTOR_OR_SELF_SQL, (ancestor, node_id)))


async def insert_edge(store: Store, from_id: str, to_id: str, edge_type: str) -> None:
    await store.write("INSERT INTO edges (from_id, to_id, type) VALUES (?, ?, ?)", (from_id, to_id, edge_type))


async def log_op(store: Store, op_id: str, kind: str, actor: str, payload: dict[str, Any]) -> int:
    """Add an operation to the log with the next running number, and return that number."""
    seq = await store.fetchval("SELECT COALESCE(MAX(seq), 0) + 1 FROM ops")
    await store.write(
        "INSERT INTO ops (seq, op_id, kind, actor, applied_at, status, payload) VALUES (?, ?, ?, ?, ?, 'applied', ?)",
        (seq, op_id, kind, actor, utc_now(), json.dumps(payload, ensure_ascii=False, sort_keys=True)),
    )
    return seq


async def record_write(store: Store, node_id: str, field: str, seq: int, actor: str) -> None:
    """Remember who wrote this field of this node last, and in which operation."""
    await store.write(
        "INSERT INTO field_writes (node_id, field, seq, actor) VALUES (?, ?, ?, ?) "
        "ON CONFLICT (node_id, field) DO UPDATE SET seq = excluded.seq, actor = excluded.actor",
        (node_id, field, seq, actor),
    )
