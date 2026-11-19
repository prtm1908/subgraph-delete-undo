#!/usr/bin/env python3
"""Write the test data, deterministic from a seed number.

    python3 gen_fixture.py --seed 7 --out fixture          seed.jsonl, ops.jsonl, planted.json
    python3 gen_fixture.py --small --out fixture_small     the six-node graph and the ops files of the stories

Every id and every choice comes from one random generator started with the seed, so the same seed always
gives the same files. Before writing, the generator counts what the task asks for, prints the counts, and
stops with an error if one is missed.

The main fixture: a data catalog (two databases, eight schemas, ~110 tables, ~1,400 columns, 150
dashboards). The first operation deletes the warehouse.sales schema (500+ nodes). Then about 300 mixed
operations: people editing the neighbours of the deleted area, an ingest run that recreates 50+ of the
deleted names under new ids, people editing those recreated nodes, arrows re-pointed, and arrows added.
Random data alone might never reach a rare rule, so the generator also plants one case for each rule and
lists them in planted.json, which the checker reads.

Each operation is applied to the replay (replay.py) as it is written, so later choices see its effect, and
the generator stops if an operation it meant to be valid is rejected.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import uuid
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from replay import Replay, ingest_node_id

PEOPLE = ["asha", "ben", "dana", "eli", "farah", "gus", "hana", "ivan", "jo", "kai", "lena", "mo", "noor", "omar"]
# gdpr is kept out of the seed: a person adds it above the deleted root, and restored nodes must pick it up.
TAGS = ["finance", "pii", "contact", "internal", "public", "sensitive", "raw", "curated", "daily", "certified"]
TABLE_WORDS = [
    "orders", "refunds", "customers", "invoices", "payments", "shipments", "returns", "discounts",
    "carts", "sessions", "leads", "accounts", "contracts", "quotes", "subscriptions", "renewals",
    "regions", "territories", "targets", "commissions", "products", "prices", "promotions", "channels",
    "partners", "events", "tickets", "campaigns", "budgets", "employees", "payroll", "vendors",
    "assets", "ledgers", "forecasts", "inventory", "stores", "suppliers", "audits", "surveys",
]
COLUMN_WORDS = [
    "id", "created_at", "updated_at", "amount", "currency_code", "email", "phone", "full_name",
    "status", "region", "country", "city", "postal_code", "customer_id", "order_id", "product_id",
    "quantity", "unit_price", "discount", "tax", "total", "notes", "source", "channel",
    "segment", "score", "owner_id", "due_date", "paid_at", "method", "card_last4", "ip_address",
    "user_agent", "referrer", "campaign_id", "margin", "cost", "weight", "sku", "category",
]
PII_COLUMNS = {"email", "phone", "full_name", "postal_code", "card_last4", "ip_address", "user_agent"}
DASHBOARD_WORDS = ["revenue", "growth", "churn", "pipeline", "funnel", "margin", "retention", "ops", "health"]
# (database, schema, number of tables, (min, max) columns per table)
LAYOUT = [
    ("warehouse", "sales", 26, (19, 26)),
    ("warehouse", "marketing", 13, (8, 14)),
    ("warehouse", "finance", 13, (8, 14)),
    ("warehouse", "hr", 12, (8, 14)),
    ("warehouse", "ops", 12, (8, 14)),
    ("warehouse", "product", 12, (8, 14)),
    ("lake", "raw", 10, (6, 12)),
    ("lake", "staging", 10, (6, 12)),
]
DASHBOARDS = 150
DELETED_SCHEMA = "warehouse.sales"
OPS_AFTER_DELETE = 300


# ---- seed -----------------------------------------------------------------------------------------


def build_seed(rng: random.Random) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    contains: list[tuple[str, str]] = []
    refs: set[tuple[str, str]] = set()

    def new_id() -> str:
        return str(uuid.UUID(int=rng.getrandbits(128), version=4))

    def person(p: float) -> str | None:
        return rng.choice(PEOPLE) if rng.random() < p else None

    def some_tags(most: int) -> list[str]:
        return rng.sample(TAGS, rng.randint(0, most))

    def add_node(node_id: str, name: str, node_type: str, description: str | None, owner: str | None, tags: list[str]) -> str:
        nodes.append(
            {
                "kind": "node",
                "id": node_id,
                "qualified_name": name,
                "type": node_type,
                "description": description,
                "owner": owner,
                "tags": tags,
            }
        )
        return node_id

    databases = {
        "warehouse": add_node(new_id(), "warehouse", "database", "Central warehouse", "asha", ["internal"]),
        "lake": add_node(new_id(), "lake", "database", "Raw landing zone", "omar", ["raw"]),
    }
    tables: dict[str, list[str]] = {}
    columns: dict[str, list[str]] = {}
    for db_name, schema, n_tables, (cmin, cmax) in LAYOUT:
        schema_name = f"{db_name}.{schema}"
        schema_id = add_node(new_id(), schema_name, "schema", f"{schema.capitalize()} schema", person(0.7), some_tags(1))
        contains.append((databases[db_name], schema_id))
        tables[schema_name] = []
        for word in sorted(rng.sample(TABLE_WORDS, n_tables)):
            table_name = f"{schema_name}.{word}"
            table_id = add_node(
                new_id(), table_name, "table", f"{word.capitalize()} in {schema}", person(0.85), some_tags(2)
            )
            contains.append((schema_id, table_id))
            tables[schema_name].append(table_id)
            columns[table_id] = []
            for col in sorted(rng.sample(COLUMN_WORDS, rng.randint(cmin, cmax))):
                tags = ["pii"] if col in PII_COLUMNS else []
                tags += [t for t in some_tags(1) if t not in tags]
                description = f"{col.replace('_', ' ').capitalize()} of {word}" if rng.random() < 0.9 else None
                column_id = add_node(new_id(), f"{table_name}.{col}", "column", description, person(0.4), tags)
                contains.append((table_id, column_id))
                columns[table_id].append(column_id)

    reports = add_node(new_id(), "reports", "folder", "Dashboards", None, ["public"])
    sales_tables = tables[DELETED_SCHEMA]
    other_tables = [t for name, ts in tables.items() if name != DELETED_SCHEMA for t in ts]
    for i in range(DASHBOARDS):
        word = rng.choice(DASHBOARD_WORDS)
        dashboard = add_node(
            new_id(), f"reports.{word}_{i:03d}", "dashboard", f"{word.capitalize()} dashboard {i}", person(0.7),
            some_tags(1),
        )
        contains.append((reports, dashboard))
        targets: set[str] = set()
        want = rng.randint(5, 11)
        while len(targets) < want:
            table = rng.choice(sales_tables if rng.random() < 0.3 else other_tables)
            targets.add(rng.choice(columns[table]) if rng.random() < 0.35 else table)
        refs.update((dashboard, target) for target in targets)
    all_tables = sales_tables + other_tables
    for table in all_tables:  # lineage: a table is built from other tables
        for _ in range(rng.choice((0, 0, 1, 1, 2, 3))):
            source = rng.choice(all_tables)
            if source != table:
                refs.add((table, source))
    return (
        nodes
        + [{"kind": "edge", "from_id": p, "to_id": c, "type": "contains"} for p, c in sorted(contains)]
        + [{"kind": "edge", "from_id": f, "to_id": t, "type": "references"} for f, t in sorted(refs)]
    )


# ---- operations -----------------------------------------------------------------------------------


class Ops:
    """Writes operations, applying each one to the replay so the next one sees its effect."""

    def __init__(self, replay: Replay) -> None:
        self.r = replay
        self.lines: list[dict[str, Any]] = []
        self.human_edits: list[str] = []  # node ids a person edited, in order
        self.repointed = 0
        self.expected_rejections = 0
        self.ingest_created: dict[str, str] = {}  # name -> id, for every node an ingest created

    def _apply(self, actor: str, kind: str, *, rejected: bool = False, **fields: Any) -> str:
        op_id = f"op{len(self.lines) + 1:04d}"
        op = {"op_id": op_id, "actor": actor, "kind": kind, **fields}
        self.lines.append(op)
        reason = self.r.apply(len(self.lines), op)
        if rejected != (reason is not None):
            raise SystemExit(f"gen_fixture: {op_id} should {'not ' * (not rejected)}be rejected ({reason}): {op}")
        return op_id

    def delete(self, root: str) -> str:
        return self._apply("human", "delete_subtree", root_id=root)

    def edit(self, node_id: str, **changes: Any) -> None:
        self._apply("human", "edit_node", id=node_id, set=changes)
        self.human_edits.append(node_id)

    def ingest(self, name: str, node_type: str, parent_name: str | None) -> str:
        created = name not in self.r.by_name
        self._apply("ingest", "ingest_node", qualified_name=name, type=node_type, parent=parent_name)
        if created:
            self.ingest_created[name] = self.r.by_name[name]
        return self.r.by_name[name]

    def add_ref(self, actor: str, from_id: str, to_id: str) -> None:
        self._apply(actor, "add_edge", from_id=from_id, to_id=to_id, type="references")

    def repoint_ref(self, from_id: str, to_id: str, new_to: str) -> None:
        self._apply("human", "repoint_edge", from_id=from_id, to_id=to_id, type="references", new_to_id=new_to)
        self.repointed += 1

    def move(self, child: str, new_parent: str) -> None:
        self._apply(
            "human", "repoint_edge", from_id=self.r.parent[child], to_id=child, type="contains", new_from_id=new_parent
        )
        self.repointed += 1

    def rejected(self, actor: str, kind: str, **fields: Any) -> None:
        """An operation the tool must refuse; nothing changes."""
        self._apply(actor, kind, rejected=True, **fields)
        self.expected_rejections += 1


def build_ops(r: Replay, rng: random.Random) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    w = Ops(r)
    warehouse = r.by_name["warehouse"]
    sales = r.by_name[DELETED_SCHEMA]
    siblings = [c for c in r.children_of(warehouse) if c != sales]
    doomed = set(r.subtree(sales))
    old = {node_id: dict(r.nodes[node_id]) for node_id in doomed}
    referrers = sorted({f for f, t in r.refs if t in doomed and f not in doomed})
    dashboards = {n for n, v in r.nodes.items() if v["type"] == "dashboard"}
    sales_tables = r.children_of(sales)
    columns_of = {t: r.children_of(t) for t in sales_tables}
    table_referrers = {t: sorted(d for d in dashboards if (d, t) in r.refs) for t in sales_tables}
    live_refs_after = defaultdict(set)  # dashboard -> targets that survive the delete
    for f, t in r.refs:
        if f in dashboards and t not in doomed:
            live_refs_after[f].add(t)

    # ---- 1. the delete --------------------------------------------------------------------------
    delete_op = w.delete(sales)

    # ---- roles for the planted cases --------------------------------------------------------------
    tables = sales_tables[:]
    rng.shuffle(tables)

    def take(pred: Callable[[str], bool]) -> str:
        for t in tables:
            if pred(t):
                tables.remove(t)
                return t
        raise SystemExit("gen_fixture: no table fits a planted case; try another seed")

    t_g = take(lambda t: any(live_refs_after[d] for d in table_referrers[t]))
    d_g = next(d for d in table_referrers[t_g] if live_refs_after[d])
    t_h = take(lambda t: any(d != d_g for d in table_referrers[t]))
    d_h = next(d for d in table_referrers[t_h] if d != d_g)
    t_a = take(lambda t: bool(old[t]["description"] and old[t]["owner"] and old[t]["tags"]))
    t_b = take(lambda t: old[t]["owner"] not in (None, "ben"))
    t_c = take(lambda t: old[t]["owner"] is not None)
    t_d = take(lambda t: True)
    t_view = take(lambda t: True)
    t_e = take(lambda t: True)
    t_f = take(lambda t: True)
    t_x = take(lambda t: True)
    extra_recreated = [tables.pop() for _ in range(5)]
    recreated_tables = [t_a, t_b, t_c, t_d, t_view, t_e, t_g, t_h] + extra_recreated
    protected = {d_g, d_h}  # random operations never touch these, so the planted cases stay as planted

    def name(node_id: str) -> str:
        return old[node_id]["qualified_name"]

    # ---- 2. people work next to the deleted area (independent edits) ----------------------------
    w.edit(warehouse, tags=r.nodes[warehouse]["tags"] + ["gdpr"])  # a tag added above the deleted root
    w.edit(warehouse, description="Central warehouse, EU region")
    for sibling in siblings:
        w.edit(sibling, owner=rng.choice(PEOPLE))
    for referrer in rng.sample([x for x in referrers if x not in protected], 10):
        w.edit(referrer, description=f"{r.nodes[referrer]['description'] or 'Reworked'} (reworked after cleanup)")

    # ---- 3. the ingest run ----------------------------------------------------------------------
    new: dict[str, str] = {}  # old id -> id of the node the ingest created under the same name
    new[sales] = w.ingest(DELETED_SCHEMA, "schema", "warehouse")
    order = recreated_tables[:]
    rng.shuffle(order)
    e_col = None
    for t in order:
        new[t] = w.ingest(name(t), "view" if t == t_view else "table", DELETED_SCHEMA)
        cols = columns_of[t]
        picked = sorted(rng.sample(cols, rng.randint(3, 5)))
        for c in picked:
            new[c] = w.ingest(name(c), "column", name(t))
        if t == t_e:
            e_col = next(c for c in cols if c not in picked)  # stays deleted, its parent comes back
    f_col = columns_of[t_f][0]  # recreated while its table is not: lands at the top level
    new[f_col] = w.ingest(name(f_col), "column", name(t_f))
    x_col = columns_of[t_x][0]  # parent and child swapped by the ingest source
    new[x_col] = w.ingest(name(x_col), "column", name(t_x))
    new[t_x] = w.ingest(name(t_x), "table", name(x_col))
    for t in rng.sample([t for t in recreated_tables if t != t_a], 3):  # a column that never existed
        w.ingest(f"{name(t)}.currency", "column", name(t))
    for t in recreated_tables:  # ingest re-adds a dashboard's arrow to the recreated table
        if t in (t_g, t_h):
            continue
        for d in table_referrers[t]:
            if d not in protected and (d, new[t]) not in r.refs:
                w.add_ref("ingest", d, new[t])
                break
    w.add_ref("ingest", d_h, new[t_h])
    live_other_tables = sorted(
        n for n, v in r.nodes.items()
        if v["type"] == "table" and n not in new.values() and r.parent.get(n) in siblings
    )
    for i, t in enumerate(rng.sample(live_other_tables, 10)):  # ingest re-reports live tables
        w.ingest(r.nodes[t]["qualified_name"], "view" if i == 0 else "table", r.nodes[r.parent[t]]["qualified_name"])

    # ---- 4. people work on the recreated nodes and around them ----------------------------------
    recreated_cols = sorted(new[c] for c in new if c in old and old[c]["type"] == "column" and c not in (f_col, x_col))
    hashed_col, described_col = rng.sample([c for c in recreated_cols if r.parent.get(c) != new[t_a]], 2)
    sibling_for_d = rng.choice(siblings)
    planted: list[Callable[[], None]] = [
        lambda: w.edit(new[t_b], owner="ben"),
        lambda: w.edit(new[t_c], owner=None),  # emptied on purpose: the ingest left it empty, a person says so
        lambda: w.edit(hashed_col, description="Hashed before load", tags=["hashed"], owner=None),
        lambda: w.edit(described_col, description="Rebuilt by ingest, checked by hand"),
        lambda: w.edit(new[sales], owner="farah"),
        lambda: w.move(new[t_d], sibling_for_d),
        lambda: w.repoint_ref(d_g, sorted(live_refs_after[d_g] & {t for f, t in r.refs if f == d_g})[0], new[t_g]),
        lambda: w.repoint_ref(d_h, new[t_h], _free_target(r, rng, d_h, protected | set(new.values()))),
    ]
    # Random edits leave the planted nodes alone: the untouched node stays untouched, the owner a person set
    # (or emptied) stays as planted, and the tag added above the root is never taken off again.
    no_random_edit = {warehouse, new[t_a], new[t_b], new[t_c]}
    edit_pools = {
        "neighbour": [n for n in siblings + referrers if n not in protected],
        "recreated": [n for n in new.values() if n not in no_random_edit],
    }

    def random_edit() -> None:
        roll = rng.random()
        if roll < 0.35:
            pool = edit_pools["neighbour"]
        elif roll < 0.5:
            pool = edit_pools["recreated"]
        else:
            pool = list(r.nodes)
        node_id = rng.choice(sorted(n for n in pool if n in r.nodes and n not in no_random_edit))
        node = r.nodes[node_id]
        kind = rng.choice(("description", "owner", "tags", "both"))
        changes: dict[str, Any] = {}
        if kind in ("description", "both"):
            changes["description"] = f"{node['qualified_name']} — note {len(w.lines)}"
        if kind in ("owner", "both"):
            changes["owner"] = rng.choice(PEOPLE) if rng.random() < 0.9 else None
        if kind == "tags":
            tags = list(node["tags"])
            if tags and rng.random() < 0.4:
                tags.remove(rng.choice(tags))
            else:
                extra = rng.choice([t for t in TAGS if t not in tags] or TAGS)
                if extra not in tags:
                    tags.append(extra)
            changes["tags"] = tags
        w.edit(node_id, **changes)

    def random_repoint() -> None:
        candidates = sorted((f, t) for f, t in r.refs if f in dashboards and f not in protected and t not in protected)
        from_id, to_id = rng.choice(candidates)
        w.repoint_ref(from_id, to_id, _free_target(r, rng, from_id, protected))

    def random_add() -> None:
        from_id = rng.choice(sorted(d for d in dashboards if d in r.nodes and d not in protected))
        w.add_ref("human", from_id, _free_target(r, rng, from_id, protected))

    def random_move() -> None:
        movable = sorted(
            t for t in r.nodes
            if r.nodes[t]["type"] == "table" and r.parent.get(t) in siblings and t not in new.values()
        )
        child = rng.choice(movable)
        w.move(child, rng.choice([s for s in siblings if s != r.parent[child]]))

    rejections: list[Callable[[], None]] = [
        lambda: w.rejected("human", "edit_node", id=t_a, set={"owner": "zed"}),  # old id, not live
        lambda: w.rejected(  # warehouse under one of its own tables: a loop
            "human", "add_edge", from_id=rng.choice(r.children_of(siblings[0])), to_id=warehouse, type="contains"
        ),
        lambda: w.rejected(  # an arrow that is not there
            "human", "repoint_edge", from_id=d_g, to_id=t_g, type="references", new_to_id=warehouse
        ),
    ]
    late_ingest = [new[t] for t in recreated_tables[:6]] + [new[t_view]]
    fixed = len(planted) + len(rejections) + len(late_ingest)
    n_random = OPS_AFTER_DELETE - (len(w.lines) - 1) - fixed
    weights = [(random_edit, 0.62), (random_repoint, 0.18), (random_add, 0.12), (random_move, 0.08)]
    actions: list[Callable[[], None]] = planted + rejections
    for _ in range(n_random):
        roll, acc = rng.random(), 0.0
        for fn, weight in weights:
            acc += weight
            if roll < acc:
                actions.append(fn)
                break
        else:
            actions.append(random_edit)
    rng.shuffle(actions)
    for action in actions:
        action()

    # ---- 5. a late ingest pass re-reports some recreated tables -------------------------------------
    for node_id in late_ingest:
        parent = r.parent.get(node_id)
        w.ingest(r.nodes[node_id]["qualified_name"], r.nodes[node_id]["type"], r.nodes[parent]["qualified_name"] if parent else None)

    # ---- counts the task asks for ------------------------------------------------------------------
    deleted_names = {v["qualified_name"] for v in old.values()}
    recreated_names = sorted(n for n in w.ingest_created if n in deleted_names)
    recreated_ids = {w.ingest_created[n] for n in recreated_names}
    human_after = set(w.human_edits)
    counts = {
        "delete_op": delete_op,
        "deleted_nodes": len(r.deletes[delete_op]["nodes"]),
        "operations_after_delete": len(w.lines) - 1,
        "human_edits_to_parent": w.human_edits.count(warehouse),
        "human_edits_to_siblings": sum(w.human_edits.count(s) for s in siblings),
        "human_edits_to_referrers": sum(1 for n in w.human_edits if n in set(referrers)),
        "edges_repointed": w.repointed,
        "deleted_names_recreated_by_ingest": len(recreated_names),
        "recreated_nodes_edited_by_people": len(recreated_ids & human_after),
        "expected_rejections": w.expected_rejections,
    }
    minimums = {
        "delete removes at least 500 nodes": counts["deleted_nodes"] >= 500,
        "about 300 operations after the delete": 250 <= counts["operations_after_delete"] <= 350,
        "people edit the parent of the deleted area": counts["human_edits_to_parent"] >= 1,
        "people edit siblings of the deleted area": counts["human_edits_to_siblings"] >= 1,
        "people edit nodes that referenced the deleted area": counts["human_edits_to_referrers"] >= 1,
        "edges are re-pointed to other nodes": counts["edges_repointed"] >= 1,
        "ingest recreates at least 50 deleted names under new ids": counts["deleted_names_recreated_by_ingest"] >= 50,
        "people edit at least 5 recreated nodes": counts["recreated_nodes_edited_by_people"] >= 5,
        "nobody edits the untouched recreated node": new[t_a] not in human_after,
    }
    planted_cases = {
        "delete_op": delete_op,
        "cases": {
            "untouched_recreated": {"old_id": t_a, "new_id": new[t_a]},
            "person_changed_field": {"old_id": t_b, "new_id": new[t_b], "field": "owner", "value": "ben"},
            "person_emptied_field": {"old_id": t_c, "new_id": new[t_c], "field": "owner"},
            "recreated_moved": {"old_id": t_d, "new_id": new[t_d], "old_parent": sales, "new_parent": sibling_for_d},
            "restored_under_recreated_parent": {"id": e_col, "parent_old": t_e, "parent_new": new[t_e]},
            "recreated_child_of_free_parent": {"parent_id": t_f, "old_id": f_col, "new_id": new[f_col]},
            "repointed_onto_recreated": {"from": d_g, "old_to": t_g, "new_to": new[t_g]},
            "repointed_away": {"from": d_h, "old_to": t_h, "new_to": new[t_h]},
            "type_differs": {"old_id": t_view, "new_id": new[t_view], "old_type": "table", "new_type": "view"},
            "tag_above_root": {"node": warehouse, "tag": "gdpr", "restored_id": e_col},
            "swapped_parent_child": {
                "schema_old": sales,
                "table_old": t_x,
                "table_new": new[t_x],
                "column_old": x_col,
                "column_new": new[x_col],
            },
        },
    }
    return w.lines, counts, {"minimums": minimums, "planted": planted_cases}


def _free_target(r: Replay, rng: random.Random, from_id: str, avoid: set[str]) -> str:
    """A live table or column that from_id does not reference yet."""
    taken = {t for f, t in r.refs if f == from_id}
    pool = sorted(
        n for n, v in r.nodes.items()
        if v["type"] in ("table", "column") and n not in taken and n not in avoid and n != from_id
    )
    return rng.choice(pool)


def build_main(seed: int) -> tuple[dict[str, Any], dict[str, Any], dict[str, bool]]:
    rng = random.Random(seed)
    seed_lines = build_seed(rng)
    ops, counts, extra = build_ops(Replay(seed_lines), rng)
    counts = {
        "seed_nodes": sum(1 for line in seed_lines if line["kind"] == "node"),
        "seed_edges": sum(1 for line in seed_lines if line["kind"] == "edge"),
        **counts,
    }
    files = {"seed.jsonl": seed_lines, "ops.jsonl": ops, "planted.json": extra["planted"]}
    return files, counts, extra["minimums"]


# ---- the small fixture --------------------------------------------------------------------------


def small_id(short: str) -> str:
    return f"00000000-0000-4000-8000-{int(short[1:]):012d}"


def build_small() -> dict[str, Any]:
    nodes = [
        ("n1", "sales", "schema", "Sales schema", None, ["finance"]),
        ("n2", "sales.orders", "table", "One row per order", "asha", ["pii"]),
        ("n5", "sales.refunds", "table", "One row per refund", None, []),
        ("n3", "sales.orders.email", "column", "Customer email", "dana", ["contact"]),
        ("n4", "sales.orders.total", "column", "Order total, EUR", "asha", []),
        ("n6", "reports.daily", "dashboard", "Daily revenue", None, []),
    ]
    edges = [
        ("n1", "n2", "contains"),
        ("n1", "n5", "contains"),
        ("n2", "n3", "contains"),
        ("n2", "n4", "contains"),
        ("n6", "n2", "references"),
        ("n6", "n4", "references"),
    ]
    seed = [
        {"kind": "node", "id": small_id(s), "qualified_name": q, "type": t, "description": d, "owner": o, "tags": g}
        for s, q, t, d, o, g in nodes
    ] + [{"kind": "edge", "from_id": small_id(a), "to_id": small_id(b), "type": t} for a, b, t in edges]

    def op(op_id: str, actor: str, kind: str, **fields: Any) -> dict[str, Any]:
        return {"op_id": op_id, "actor": actor, "kind": kind, **fields}

    def delete(op_id: str, root_id: str) -> dict[str, Any]:
        return op(op_id, "human", "delete_subtree", root_id=root_id)

    def ingest(op_id: str, name: str, node_type: str, parent: str | None) -> dict[str, Any]:
        return op(op_id, "ingest", "ingest_node", qualified_name=name, type=node_type, parent=parent)

    # The nodes an ingest creates in the stories (n7, n8, n9 and archive in the design's drawings).
    orders = ingest_node_id("i1", "sales.orders")
    email = ingest_node_id("i2", "sales.orders.email")
    archive = ingest_node_id("i0", "archive")
    n1, n2, n3, n4, n5, n6 = (small_id(s) for s in ("n1", "n2", "n3", "n4", "n5", "n6"))
    return {
        "seed.jsonl": seed,
        # A neighbour is deleted later: d2 removes the dashboard that two arrows of d1 need.
        "ops_a.jsonl": [delete("d1", n2), delete("d2", n6)],
        # The parent is deleted later: d2 removes sales, the parent of the subtree d1 removed.
        "ops_b.jsonl": [delete("d1", n2), delete("d2", n1)],
        # The recreated node is deleted again: two old versions of sales.orders end up in two journals.
        "ops_c.jsonl": [
            delete("d1", n2),
            ingest("i1", "sales.orders", "view", "sales"),
            op("e1", "human", "edit_node", id=orders, set={"owner": "ben"}),
            delete("d2", orders),
        ],
        # The usual case: others keep working, the ingest recreates names, people edit what it made.
        "ops_d.jsonl": [
            delete("d1", n2),
            op("e1", "human", "edit_node", id=n1, set={"tags": ["finance", "gdpr"]}),
            op("e2", "human", "edit_node", id=n5, set={"description": "Approved refunds only"}),
            ingest("i1", "sales.orders", "table", "sales"),
            ingest("i2", "sales.orders.email", "column", "sales.orders"),
            ingest("i3", "sales.orders.currency", "column", "sales.orders"),
            op("e3", "human", "edit_node", id=orders, set={"owner": "ben"}),
            op("e4", "human", "edit_node", id=email, set={"description": "Hashed customer email", "tags": ["hashed"], "owner": None}),
        ],
        # A person moved an arrow away: the ingest re-adds the dashboard's arrow, a person re-points it.
        "ops_e.jsonl": [
            delete("d1", n2),
            ingest("i1", "sales.orders", "table", "sales"),
            op("a1", "ingest", "add_edge", from_id=n6, to_id=orders, type="references"),
            op("r1", "human", "repoint_edge", from_id=n6, to_id=orders, type="references", new_to_id=n5),
        ],
        # The recreated node was moved: a person moves it under archive. People do not create nodes, so the
        # ingest creates archive and a person gives it the tag old, before the delete as in the drawing.
        "ops_f.jsonl": [
            ingest("i0", "archive", "folder", None),
            op("e0", "human", "edit_node", id=archive, set={"tags": ["old"]}),
            delete("d1", n2),
            ingest("i1", "sales.orders", "table", "sales"),
            op("m1", "human", "repoint_edge", from_id=n1, to_id=orders, type="contains", new_from_id=archive),
        ],
        # ---- for the redo stories -----------------------------------------------------------------
        # The delete alone: undo restores everything, then a person edits or deletes something.
        "ops_g.jsonl": [delete("d1", n2)],
        # The delete and one recreated node: undo merges into it and restores the two columns under it.
        "ops_h.jsonl": [delete("d1", n2), ingest("i1", "sales.orders", "table", "sales")],
        # Two undos on one node: a column is deleted, recreated, and the recreated one is deleted again.
        "ops_i.jsonl": [delete("d1", n3), ingest("i2", "sales.orders.email", "column", "sales.orders"), delete("d2", email)],
        # Applied between an undo and a redo.
        "later_edit_total.jsonl": [op("x1", "human", "edit_node", id=n4, set={"description": "Order total with tax"})],
        "later_edit_orders.jsonl": [op("x1", "human", "edit_node", id=orders, set={"description": "Orders, one row each"})],
        "later_delete_total.jsonl": [delete("d3", n4)],
        "later_delete_orders.jsonl": [delete("d3", orders)],
    }


# ---- writing ------------------------------------------------------------------------------------


def _render(name: str, content: Any) -> str:
    if name.endswith(".jsonl"):
        return "".join(json.dumps(line, ensure_ascii=False) + "\n" for line in content)
    return json.dumps(content, indent=2, ensure_ascii=False) + "\n"


async def _write_all(out: Path, files: dict[str, Any]) -> None:
    await asyncio.to_thread(out.mkdir, parents=True, exist_ok=True)
    await asyncio.gather(
        *(asyncio.to_thread((out / name).write_text, _render(name, content), "utf-8") for name, content in files.items())
    )


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--seed", type=int, default=7, help="random seed (default 7)")
    parser.add_argument("--out", required=True, help="directory to write into")
    parser.add_argument("--small", action="store_true", help="write the six-node fixture instead")
    args = parser.parse_args(argv)
    out = Path(args.out)

    if args.small:
        files = build_small()
        await _write_all(out, files)
        print(json.dumps({"wrote": sorted(files)}, indent=2))
        return 0

    files, counts, minimums = await asyncio.to_thread(build_main, args.seed)
    print(json.dumps({"seed": args.seed, **counts}, indent=2))
    for rule, ok in minimums.items():
        print(f"  {'ok  ' if ok else 'MISS'} {rule}")
    if not all(minimums.values()):
        print("gen_fixture: a minimum was missed; nothing written", file=sys.stderr)
        return 1
    await _write_all(out, files)
    print(f"wrote {', '.join(sorted(files))} to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
