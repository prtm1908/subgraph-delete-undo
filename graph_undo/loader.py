"""load: start a new database file from seed.jsonl, replacing an old one.

The seed is checked before anything is written: ids are UUIDs, ids and names are unique, every arrow joins
two seed nodes, no node has two contains parents, and the contains arrows have no loop. The new file is
built next to the old one and moved into place at the end, so a failed load leaves the old file alone.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections import defaultdict
from typing import Any

from .common import CONTAINS, EDGE_TYPES, WRITTEN_FIELDS, InputError, read_jsonl, tags_json, valid_value
from .schema import SCHEMA
from .store import Store
from .tags import propagate

# The seed counts as written at the very start, for every field of every node.
SEED_SEQ, SEED_ACTOR = 0, "seed"
# SQLite's own files next to a database. A stale one next to the new file would be read as part of it.
SIDE_FILES = ("-journal", "-wal", "-shm")


async def load(db_path: str, seed_path: str) -> dict[str, int]:
    records = await read_jsonl(seed_path)
    nodes, edges = _check_seed(seed_path, records)
    effective = _effective_for_all(seed_path, nodes, edges)

    tmp_path = db_path + ".loading"
    await asyncio.to_thread(_remove, tmp_path, ("",) + SIDE_FILES)
    try:
        store = await Store.open(tmp_path)
        try:
            await store.script(SCHEMA)
            async with store.transaction():
                await store.write_many(
                    "INSERT INTO nodes (id, qualified_name, type, description, owner, tags, effective_tags) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            n["id"],
                            n["qualified_name"],
                            n["type"],
                            n["description"],
                            n["owner"],
                            tags_json(n["tags"]),
                            tags_json(effective[n["id"]]),
                        )
                        for n in nodes
                    ],
                )
                await store.write_many("INSERT INTO edges (from_id, to_id, type) VALUES (?, ?, ?)", edges)
                await store.write_many(
                    "INSERT INTO field_writes (node_id, field, seq, actor) VALUES (?, ?, ?, ?)",
                    [(n["id"], field, SEED_SEQ, SEED_ACTOR) for n in nodes for field in WRITTEN_FIELDS],
                )
        finally:
            await store.close()
        await asyncio.to_thread(_remove, db_path, SIDE_FILES)
        await asyncio.to_thread(os.replace, tmp_path, db_path)
    except BaseException:
        await asyncio.to_thread(_remove, tmp_path, ("",) + SIDE_FILES)
        raise
    return {"nodes": len(nodes), "edges": len(edges)}


def _remove(base: str, suffixes: tuple[str, ...]) -> None:
    for suffix in suffixes:
        try:
            os.remove(base + suffix)
        except FileNotFoundError:
            pass


def _check_seed(
    path: str, records: list[tuple[int, dict[str, Any]]]
) -> tuple[list[dict[str, Any]], list[tuple[str, str, str]]]:
    nodes: list[dict[str, Any]] = []
    raw_edges: list[tuple[int, tuple[str, str, str]]] = []
    ids: set[str] = set()
    names: set[str] = set()
    for lineno, rec in records:
        where = f"{path}:{lineno}"
        kind = rec.get("kind")
        if kind == "node":
            node_id = rec.get("id")
            if not isinstance(node_id, str) or not _is_uuid(node_id):
                raise InputError(f"{where}: id must be a UUID in canonical form")
            name, node_type = rec.get("qualified_name"), rec.get("type")
            if not isinstance(name, str) or not name:
                raise InputError(f"{where}: qualified_name must be a non-empty string")
            if not isinstance(node_type, str) or not node_type:
                raise InputError(f"{where}: type must be a non-empty string")
            fields = {"description": rec.get("description"), "owner": rec.get("owner"), "tags": rec.get("tags", [])}
            for field, value in fields.items():
                if not valid_value(field, value):
                    raise InputError(f"{where}: {field} must be " + ("a list of strings" if field == "tags" else "a string or null"))
            if node_id in ids:
                raise InputError(f"{where}: node id {node_id} appears twice")
            if name in names:
                raise InputError(f"{where}: qualified_name {name!r} appears twice")
            ids.add(node_id)
            names.add(name)
            nodes.append({"id": node_id, "qualified_name": name, "type": node_type, **fields})
        elif kind == "edge":
            from_id, to_id, edge_type = rec.get("from_id"), rec.get("to_id"), rec.get("type")
            if edge_type not in EDGE_TYPES:
                raise InputError(f"{where}: edge type must be one of {', '.join(EDGE_TYPES)}")
            if not isinstance(from_id, str) or not isinstance(to_id, str):
                raise InputError(f"{where}: from_id and to_id must be strings")
            raw_edges.append((lineno, (from_id, to_id, edge_type)))
        else:
            raise InputError(f"{where}: kind must be 'node' or 'edge'")

    edges: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    has_parent: set[str] = set()
    for lineno, edge in raw_edges:
        where = f"{path}:{lineno}"
        from_id, to_id, edge_type = edge
        if from_id not in ids or to_id not in ids:
            raise InputError(f"{where}: arrow joins a node that is not in the seed")
        if edge in seen:
            raise InputError(f"{where}: arrow appears twice")
        if edge_type == CONTAINS:
            if from_id == to_id:
                raise InputError(f"{where}: a node cannot contain itself")
            if to_id in has_parent:
                raise InputError(f"{where}: node {to_id} would have two contains parents")
            has_parent.add(to_id)
        seen.add(edge)
        edges.append(edge)
    return nodes, edges


def _effective_for_all(
    path: str, nodes: list[dict[str, Any]], edges: list[tuple[str, str, str]]
) -> dict[str, list[str]]:
    children: dict[str, list[str]] = defaultdict(list)
    for from_id, to_id, edge_type in edges:
        if edge_type == CONTAINS:
            children[from_id].append(to_id)
    has_parent = {to_id for children_ in children.values() for to_id in children_}
    roots = [n["id"] for n in nodes if n["id"] not in has_parent]
    effective = propagate(roots, children, {n["id"]: n["tags"] for n in nodes}, {})
    if len(effective) != len(nodes):
        raise InputError(f"{path}: the contains arrows form a loop")
    return effective


def _is_uuid(value: str) -> bool:
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False
