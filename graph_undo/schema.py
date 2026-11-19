"""The tables of the database file: the live graph, and the journal undo reads and marks off."""

SCHEMA = """
-- The live graph. dump prints these two tables and nothing else.
CREATE TABLE nodes (
    id             TEXT PRIMARY KEY,
    qualified_name TEXT NOT NULL UNIQUE,   -- the table holds live nodes only: a name is unique among live nodes
    type           TEXT NOT NULL,
    description    TEXT,
    owner          TEXT,
    tags           TEXT NOT NULL,          -- JSON list, set by people, in the order written
    effective_tags TEXT NOT NULL           -- JSON list, worked out, sorted
);

CREATE TABLE edges (
    from_id TEXT NOT NULL REFERENCES nodes(id),
    to_id   TEXT NOT NULL REFERENCES nodes(id),
    type    TEXT NOT NULL CHECK (type IN ('contains', 'references')),
    PRIMARY KEY (from_id, to_id, type)
);
-- A node has at most one contains parent: the contains arrows form a tree.
CREATE UNIQUE INDEX one_parent ON edges(to_id) WHERE type = 'contains';
CREATE INDEX edges_by_to ON edges(to_id, type);

-- Every operation, applied or rejected. seq is the running number: "after the delete" means a larger seq.
-- Each undo is logged here too, once, as op_id 'undo:<op_id of the delete>' with kind 'undo', by a person,
-- so a later operation (or a redo) can tell what came after it.
CREATE TABLE ops (
    seq        INTEGER PRIMARY KEY,
    op_id      TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL,
    actor      TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('applied', 'rejected')),
    reason     TEXT,
    payload    TEXT NOT NULL
);

-- Who wrote each field of each node last, and in which operation (seq 0 = the seed).
-- Tells "empty because nobody wrote it" from "a person cleared it on purpose".
-- Rows outlive their node, so a node restored with its old id keeps its write history.
CREATE TABLE field_writes (
    node_id TEXT NOT NULL,
    field   TEXT NOT NULL CHECK (field IN ('description', 'owner', 'tags', 'type')),
    seq     INTEGER NOT NULL,
    actor   TEXT NOT NULL,
    PRIMARY KEY (node_id, field)
);

-- Every arrow a person re-pointed away from its old end, with the operation's seq.
CREATE TABLE edge_removals (
    from_id TEXT NOT NULL,
    to_id   TEXT NOT NULL,
    type    TEXT NOT NULL,
    seq     INTEGER NOT NULL,
    actor   TEXT NOT NULL,
    op_id   TEXT NOT NULL,
    PRIMARY KEY (from_id, to_id, type, seq)
);

-- A copy of each node a delete removed, and what undo decided for it.
CREATE TABLE removed_nodes (
    delete_op      TEXT NOT NULL REFERENCES ops(op_id),
    ord            INTEGER NOT NULL,       -- parents first
    id             TEXT NOT NULL,
    qualified_name TEXT NOT NULL,
    type           TEXT NOT NULL,
    description    TEXT,
    owner          TEXT,
    tags           TEXT NOT NULL,
    parent_id      TEXT,                   -- the contains parent at the time of the delete
    writes         TEXT NOT NULL,          -- JSON {field: [seq, actor]}, copied from field_writes
    outcome        TEXT CHECK (outcome IN ('restored', 'merged', 'skipped')),
    reason         TEXT,
    merged_into    TEXT,
    waiting_on     TEXT,                   -- the delete that has to be undone before this row can come back
    detail         TEXT,                   -- JSON for merged rows: filled, kept_live, not_restored, replaced
    decided_by     TEXT,                   -- the undo that wrote the outcome ('undo:<op_id>'; a retry names a later one)
    prior          TEXT,                   -- JSON for merged rows: {field: {value, write}} the live node held before
                                           -- undo changed it, so a redo can put it back
    PRIMARY KEY (delete_op, id)
);
CREATE INDEX removed_nodes_by_id ON removed_nodes(id);
CREATE INDEX removed_nodes_waiting ON removed_nodes(waiting_on) WHERE outcome = 'skipped';

-- A copy of each arrow a delete removed, and what undo decided for it.
CREATE TABLE removed_edges (
    delete_op  TEXT NOT NULL REFERENCES ops(op_id),
    ord        INTEGER NOT NULL,           -- arrows from parents in the order of their child, then the rest
    from_id    TEXT NOT NULL,
    to_id      TEXT NOT NULL,
    type       TEXT NOT NULL,
    outcome    TEXT CHECK (outcome IN ('restored', 'merged', 'skipped')),
    reason     TEXT,
    waiting_on TEXT,
    now_from   TEXT,                       -- where the arrow points now, once its ends were worked out
    now_to     TEXT,
    decided_by TEXT,                       -- the undo that wrote the outcome
    PRIMARY KEY (delete_op, from_id, to_id, type)
);
CREATE INDEX removed_edges_waiting ON removed_edges(waiting_on) WHERE outcome = 'skipped';
"""
