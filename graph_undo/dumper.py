"""dump: the live graph as sorted JSONL.

Node lines sorted by id, then arrow lines sorted by from_id, to_id and type. Keys always in the same order.
Two equal graphs always print the same bytes.
"""

from __future__ import annotations

import json

from .store import Store


async def dump(store: Store) -> str:
    nodes = await store.fetchall(
        "SELECT id, qualified_name, type, description, owner, tags, effective_tags FROM nodes ORDER BY id"
    )
    edges = await store.fetchall("SELECT from_id, to_id, type FROM edges ORDER BY from_id, to_id, type")
    lines = [
        json.dumps(
            {
                "id": node_id,
                "qualified_name": name,
                "type": node_type,
                "description": description,
                "owner": owner,
                "tags": json.loads(tags),
                "effective_tags": json.loads(effective),
            },
            ensure_ascii=False,
        )
        for node_id, name, node_type, description, owner, tags, effective in nodes
    ]
    lines.extend(
        json.dumps({"from_id": from_id, "to_id": to_id, "type": edge_type}, ensure_ascii=False)
        for from_id, to_id, edge_type in edges
    )
    return "".join(line + "\n" for line in lines)
