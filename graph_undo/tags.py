"""Tag propagator.

effective_tags of a node = its own tags + the effective tags of its contains parent. Only contains arrows
carry tags. Inherited tags are never copied into the journal: they are always worked out from what is live.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence

from .common import tags_json
from .graph import subtree, walk_down
from .store import Store

PARENT_TAGS_SQL = """
SELECT p.effective_tags FROM edges e JOIN nodes p ON p.id = e.from_id
WHERE e.to_id = ? AND e.type = 'contains'
"""

ROOTS_SQL = "SELECT id FROM nodes WHERE id NOT IN (SELECT to_id FROM edges WHERE type = 'contains') ORDER BY id"


def propagate(
    roots: Iterable[str],
    children: Mapping[str, Sequence[str]],
    own: Mapping[str, Sequence[str]],
    inherited: Mapping[str, Sequence[str]],
) -> dict[str, list[str]]:
    """Each node takes its own tags plus the worked-out tags of its parent; a root starts from inherited."""
    result: dict[str, list[str]] = {}
    for node, parent in walk_down(roots, children):
        above = result[parent] if parent is not None else inherited.get(node, ())
        result[node] = sorted(set(own[node]) | set(above))
    return result


async def refresh_subtree(store: Store, root_id: str) -> None:
    """Recompute effective_tags for root_id and everything below it. Writes only the rows that change."""
    rows, children = await subtree(store, root_id)
    if not rows:
        return
    parent_tags = await store.fetchval(PARENT_TAGS_SQL, (root_id,))
    fresh = propagate(
        [root_id],
        children,
        {row.id: json.loads(row.tags) for row in rows},
        {root_id: json.loads(parent_tags) if parent_tags else []},
    )
    for row in sorted(rows):
        text = tags_json(fresh[row.id])
        if text != row.effective_tags:
            await store.write("UPDATE nodes SET effective_tags = ? WHERE id = ?", (text, row.id))


async def refresh_all(store: Store) -> None:
    """Recompute effective_tags for every live node: the same walk, from every node without a parent."""
    for (root,) in await store.fetchall(ROOTS_SQL):
        await refresh_subtree(store, root)
