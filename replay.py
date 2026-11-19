"""The operations applied to plain dictionaries: the reference the checker holds the tool against.

checker.py replays the fixture here to know what each delete removed and who wrote what, and when.
gen_fixture.py applies every operation it writes here, so each one is valid when it runs and the planted
rejections really are rejected. Nothing here comes from the tool (graph_undo): the checker must share no
code with it, so the rules are written out a second time, independently.
"""

from __future__ import annotations

import uuid
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

# The repository's rule for the id of a node an ingest creates (README).
INGEST_NAMESPACE = uuid.UUID("2f0c9a52-8a4e-4c55-9d1b-1b0f6d1e7a3c")
AUTHORED = ("description", "owner", "tags")
EDGE_TYPES = ("contains", "references")

Edge = tuple[str, str, str]


def ingest_node_id(op_id: str, qualified_name: str) -> str:
    return str(uuid.uuid5(INGEST_NAMESPACE, f"{op_id}/{qualified_name}"))


def walk_down(roots: Iterable[str], children: Mapping[str, Sequence[str]]) -> list[str]:
    """Every node below the roots once, parents before children, siblings in id order."""
    order: list[str] = []
    seen: set[str] = set()
    queue = deque(roots)
    while queue:
        node = queue.popleft()
        if node in seen:
            continue
        seen.add(node)
        order.append(node)
        queue.extend(sorted(children.get(node, ())))
    return order


class Replay:
    """The graph after the seed and each operation, plus who wrote what and when."""

    def __init__(self, seed: Iterable[dict[str, Any]]) -> None:
        self.nodes: dict[str, dict[str, Any]] = {}
        self.by_name: dict[str, str] = {}
        self.parent: dict[str, str] = {}
        self.refs: set[tuple[str, str]] = set()
        self.writes: dict[tuple[str, str], tuple[int, str]] = {}  # (node, field) -> (seq, actor); seed = 0
        self.edge_events: dict[Edge, list[tuple[int, str, str]]] = defaultdict(list)  # (seq, actor, add/remove)
        self.node_removals: dict[str, list[tuple[int, str]]] = defaultdict(list)
        self.deletes: dict[str, dict[str, Any]] = {}  # op_id -> seq, removed node snapshots, removed arrows
        self.status: dict[str, str] = {}  # op_id -> "applied" or "rejected:<reason>"
        records = list(seed)
        for rec in records:
            if rec["kind"] == "node":
                node_id = rec["id"]
                self.nodes[node_id] = {
                    "qualified_name": rec["qualified_name"],
                    "type": rec["type"],
                    "description": rec.get("description"),
                    "owner": rec.get("owner"),
                    "tags": list(rec.get("tags", [])),
                }
                self.by_name[rec["qualified_name"]] = node_id
                for field in AUTHORED + ("type",):
                    self.writes[(node_id, field)] = (0, "seed")
        for rec in records:
            if rec["kind"] == "edge":
                if rec["type"] == "contains":
                    self.parent[rec["to_id"]] = rec["from_id"]
                else:
                    self.refs.add((rec["from_id"], rec["to_id"]))

    # ---- reading the graph ---------------------------------------------------------------------------

    def edges(self) -> set[Edge]:
        return {(p, c, "contains") for c, p in self.parent.items()} | {(f, t, "references") for f, t in self.refs}

    def has_edge(self, edge: Edge) -> bool:
        f, t, kind = edge
        return self.parent.get(t) == f if kind == "contains" else (f, t) in self.refs

    def children(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = defaultdict(list)
        for child, parent in self.parent.items():
            out[parent].append(child)
        return out

    def children_of(self, node: str) -> list[str]:
        return sorted(child for child, parent in self.parent.items() if parent == node)

    def subtree(self, root: str) -> list[str]:
        return walk_down([root], self.children())

    def ancestor_or_self(self, ancestor: str, node: str | None) -> bool:
        seen: set[str] = set()
        while node is not None and node not in seen:
            if node == ancestor:
                return True
            seen.add(node)
            node = self.parent.get(node)
        return False

    # ---- operations ------------------------------------------------------------------------------------

    def apply_all(self, ops: Iterable[dict[str, Any]]) -> None:
        for seq, op in enumerate(ops, start=1):
            self.apply(seq, op)

    def apply(self, seq: int, op: dict[str, Any]) -> str | None:
        """Apply one operation. Returns None, or the reason it is rejected (then nothing changes)."""
        actor, kind = op.get("actor"), op.get("kind")
        handler = {
            "edit_node": self._edit,
            "add_edge": self._add_edge,
            "repoint_edge": self._repoint,
            "ingest_node": self._ingest,
            "delete_subtree": self._delete,
        }.get(kind)
        if actor not in ("human", "ingest"):
            reason = "unknown_actor"
        elif handler is None:
            reason = "unknown_kind"
        else:
            reason = handler(seq, op, actor)
        self.status[op["op_id"]] = "applied" if reason is None else f"rejected:{reason}"
        return reason

    def _add(self, edge: Edge, seq: int, actor: str) -> None:
        f, t, kind = edge
        if kind == "contains":
            self.parent[t] = f
        else:
            self.refs.add((f, t))
        self.edge_events[edge].append((seq, actor, "add"))

    def _remove(self, edge: Edge, seq: int, actor: str) -> None:
        f, t, kind = edge
        if kind == "contains":
            del self.parent[t]
        else:
            self.refs.discard((f, t))
        self.edge_events[edge].append((seq, actor, "remove"))

    def _new_arrow_problem(self, edge: Edge, *, child_moves: bool = False) -> str | None:
        f, t, kind = edge
        if self.has_edge(edge):
            return "edge_exists"
        if kind == "contains":
            if not child_moves and t in self.parent:
                return "child_has_parent"
            if self.ancestor_or_self(t, f):
                return "would_create_cycle"
        return None

    @staticmethod
    def _text(op: dict[str, Any], key: str) -> str | None:
        value = op.get(key)
        return value if isinstance(value, str) and value else None

    def _edit(self, seq: int, op: dict[str, Any], actor: str) -> str | None:
        if actor != "human":
            return "actor_not_allowed"
        node, changes = self._text(op, "id"), op.get("set")
        if node is None or not isinstance(changes, dict) or not changes:
            return "malformed"
        for field, value in changes.items():
            if field not in AUTHORED:
                return "malformed"
            if field == "tags":
                if not isinstance(value, list) or not all(isinstance(t, str) for t in value):
                    return "malformed"
            elif value is not None and not isinstance(value, str):
                return "malformed"
        if node not in self.nodes:
            return "node_not_live"
        for field, value in changes.items():
            self.nodes[node][field] = list(value) if field == "tags" else value
            self.writes[(node, field)] = (seq, actor)
        return None

    def _add_edge(self, seq: int, op: dict[str, Any], actor: str) -> str | None:
        f, t, kind = self._text(op, "from_id"), self._text(op, "to_id"), op.get("type")
        if f is None or t is None or kind not in EDGE_TYPES:
            return "malformed"
        if f not in self.nodes or t not in self.nodes:
            return "endpoint_not_live"
        problem = self._new_arrow_problem((f, t, kind))
        if problem:
            return problem
        self._add((f, t, kind), seq, actor)
        return None

    def _repoint(self, seq: int, op: dict[str, Any], actor: str) -> str | None:
        f, t, kind = self._text(op, "from_id"), self._text(op, "to_id"), op.get("type")
        if f is None or t is None or kind not in EDGE_TYPES:
            return "malformed"
        new_from, new_to = op.get("new_from_id"), op.get("new_to_id")
        if (new_from is None) == (new_to is None):
            return "malformed"
        moved = self._text(op, "new_from_id" if new_from is not None else "new_to_id")
        if moved is None:
            return "malformed"
        new_edge = (moved, t, kind) if new_from is not None else (f, moved, kind)
        if not self.has_edge((f, t, kind)):
            return "edge_not_live"
        if moved not in self.nodes:
            return "endpoint_not_live"
        problem = self._new_arrow_problem(new_edge, child_moves=new_from is not None)
        if problem:
            return problem
        self._remove((f, t, kind), seq, actor)
        self._add(new_edge, seq, actor)
        return None

    def _ingest(self, seq: int, op: dict[str, Any], actor: str) -> str | None:
        if actor != "ingest":
            return "actor_not_allowed"
        if any(field in op for field in AUTHORED):
            return "ingest_sets_authored_field"
        name, node_type, parent_name = self._text(op, "qualified_name"), self._text(op, "type"), op.get("parent")
        if name is None or node_type is None or (parent_name is not None and not isinstance(parent_name, str)):
            return "malformed"
        if name in self.by_name:
            node = self.by_name[name]
            self.nodes[node]["type"] = node_type
            self.writes[(node, "type")] = (seq, actor)
            return None
        node = ingest_node_id(op["op_id"], name)
        self.nodes[node] = {"qualified_name": name, "type": node_type, "description": None, "owner": None, "tags": []}
        self.by_name[name] = node
        self.writes[(node, "type")] = (seq, actor)
        if parent_name and parent_name in self.by_name:
            self._add((self.by_name[parent_name], node, "contains"), seq, actor)
        return None

    def _delete(self, seq: int, op: dict[str, Any], actor: str) -> str | None:
        root = self._text(op, "root_id")
        if root is None:
            return "malformed"
        if root not in self.nodes:
            return "node_not_live"
        ids = self.subtree(root)
        doomed = set(ids)
        snapshot = {node: {**self.nodes[node], "parent": self.parent.get(node)} for node in ids}
        removed = {(self.parent[n], n, "contains") for n in ids if n in self.parent}
        removed |= {(f, t, "references") for f, t in self.refs if f in doomed or t in doomed}
        for edge in sorted(removed):
            self._remove(edge, seq, actor)
        for node in ids:
            del self.by_name[self.nodes.pop(node)["qualified_name"]]
            self.node_removals[node].append((seq, actor))
        self.deletes[op["op_id"]] = {"seq": seq, "nodes": snapshot, "edges": removed}
        return None
