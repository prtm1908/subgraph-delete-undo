# Undo in a shared graph

A command-line tool that keeps a graph of entities and relationships in one SQLite file, and can undo a
subtree delete after other people and an ingest job have kept editing the graph. Instead of rolling the
graph back, undo puts back what the delete removed on top of the graph as it is now. For every removed
node and arrow it reports one outcome: restored, merged or skipped. Redo, the one extension the task
names, is built too: it takes back what an undo put back. The rules are in [DECISIONS.md](DECISIONS.md),
and the full design, with step-by-step stories for undo and redo, is in
[docs/design.html](docs/design.html) (download it and open it in a browser).

Python 3.11 or newer. Nothing to install: everything comes from the standard library.

## Run it

```bash
python3 gen_fixture.py --seed 7 --out fixture
python3 gen_fixture.py --small --out fixture_small
python3 run_scenario.py --fixture fixture --small fixture_small --out run
python3 checker.py fixture --small fixture_small --run run
```

1. Writes `fixture/seed.jsonl` (1,717 nodes and 3,034 arrows), `fixture/ops.jsonl` (one delete of 620
   nodes, then 300 operations) and `fixture/planted.json`. Prints the counts the task asks for and stops
   with an error, writing nothing, if one is missed. Same seed, same files.
2. Writes the six-node fixture from the design's stories: `seed.jsonl`, one operations file per undo
   story (`ops_a.jsonl` to `ops_f.jsonl`), three more for the redo stories (`ops_g.jsonl` to
   `ops_i.jsonl`) and four `later_*.jsonl` files that are applied between an undo and a redo.
3. Runs the tool as a subprocess and saves every dump and report under `run/`. The main scenario is
   load, apply, dump, undo, dump (timed). Then it runs undo a second time, and runs 11 crashed undos on
   copies of the database taken right after apply. On another copy it applies the operations file
   again, then a file that re-uses an op_id for a different operation. Then redo on the main scenario:
   redo, redo again, undo again, a few changes by people and the ingest, redo once more, 10 crashed
   redos, and each command tried while the other is unfinished. Last, it runs each undo story's file
   from a fresh load, the two-delete ones in both undo orders, and the 13 redo stories. Takes about 17
   seconds.
4. Reads only files and never runs the tool. Prints one PASS or FAIL line per check, and exits with
   status 0 when every check passes.

Optional: `python3 mutation_check.py` breaks the undo in 9 different ways and the redo in 8, each in a
throwaway copy of the repository, and shows which checks catch each break. It takes a few minutes and
exits with status 0 when every break is caught.

## The scenario by hand

```bash
python3 graphundo.py load fixture/seed.jsonl
python3 graphundo.py apply fixture/ops.jsonl
python3 graphundo.py dump > before.jsonl
python3 graphundo.py undo op0001 > report.json
python3 graphundo.py dump > after.jsonl
python3 checker.py fixture before.jsonl after.jsonl report.json
```

The delete is the first operation, `op0001`. The database is `graph.db` in the current directory. Use
`--db PATH` or `GRAPHUNDO_DB` to put it somewhere else. On these three files the checker runs checks 0 to
5 and the planted cases. Checks 6, 7 and 8 need the extra runs that `run_scenario.py` makes.

A crash and a resume, on a database where the delete has not been undone yet:

```bash
CRASH_AFTER=500 python3 graphundo.py undo op0001    # stops after 500 writes, exit status 3
python3 graphundo.py dump                           # a smaller graph, still whole, tags correct
python3 graphundo.py undo op0001                    # carries on: same final dump and report
```

`CRASH_AFTER=n` kills the process right after its n-th write, the way a crash would. Steps that finished
before it stay saved. The step it was in the middle of is rolled back by SQLite the next time the file is
opened. So `CRASH_AFTER=1` saves nothing: the first write is inside the first transaction.

Redo, and undo again:

```bash
python3 graphundo.py redo op0001 > redo_report.json   # the dump is now the one from before the undo
python3 graphundo.py undo op0001                      # a new round: the same dump and report as the first undo
```

## Commands

- `load SEED`: creates a new database from the seed, replacing an old one. The seed is checked first:
  ids are UUIDs, ids and names are unique, every arrow joins two seed nodes, no node has two parents and
  there is no loop. `effective_tags` is worked out for the whole graph.
- `apply OPS`: applies the operations in order, one transaction each. An operation that cannot be applied
  as written is stored as rejected, with its reason, and changes nothing. An op_id names one operation:
  running apply again on the same file skips what is already there, and an op_id that was already used
  for a different operation stops apply with an error before anything changes.
- `undo OP_ID`: undoes one `delete_subtree` and prints the report. Any other op_id prints an error and
  writes nothing. It reads `CRASH_AFTER=<n>`. On stderr it says how many writes it made. The undo is
  logged in `ops` as an operation of its own (`undo:<op_id>`, by a person). Every journal row records
  which undo decided it, and a merge records what the live node held before. That is what redo needs.
- `redo OP_ID` (the extension): takes back what the undo of that delete put back, and prints a report.
  Restored nodes go out again, the arrows undo added are removed, and the fields undo filled get their
  earlier value. Anything a person wrote on or attached since the undo stays, with a reason in the
  report. It reads `CRASH_AFTER=<n>` and can be resumed and repeated like undo. A delete that was never
  undone prints an error and writes nothing.
- Undo and redo can go back and forth. The same command twice in a row is a repeat and writes nothing;
  `undo` after a `redo` starts a new round. Each one is refused while the other is unfinished after a
  crash: run the crashed command again first.
- `dump`: prints the live graph as JSONL. Node lines come first, sorted by id. Arrow lines follow, sorted
  by from_id, to_id and type. Equal graphs print the same bytes.

## Input files

```json
{"kind": "node", "id": "…uuid…", "qualified_name": "warehouse.sales.orders", "type": "table", "description": "Orders in sales", "owner": "asha", "tags": ["pii"]}
{"kind": "edge", "from_id": "…", "to_id": "…", "type": "contains"}
```

Each operation line has `op_id`, `actor` (`human` or `ingest`) and `kind`:

```json
{"op_id": "op0001", "actor": "human",  "kind": "delete_subtree", "root_id": "…"}
{"op_id": "op0002", "actor": "human",  "kind": "edit_node", "id": "…", "set": {"owner": "ben", "tags": ["pii"]}}
{"op_id": "op0003", "actor": "human",  "kind": "add_edge", "from_id": "…", "to_id": "…", "type": "references"}
{"op_id": "op0004", "actor": "human",  "kind": "repoint_edge", "from_id": "…", "to_id": "…", "type": "references", "new_to_id": "…"}
{"op_id": "op0005", "actor": "ingest", "kind": "ingest_node", "qualified_name": "warehouse.sales.orders", "type": "table", "parent": "warehouse.sales"}
```

- `repoint_edge` takes exactly one of `new_from_id` and `new_to_id`. Re-pointing a contains arrow's
  `from_id` moves a node under another parent.
- `ingest_node` updates the type of the live node that has the name. When no live node has the name, it
  creates one under `parent`, if that parent is live, and at the top level otherwise. The new node's id is
  `uuid5(2f0c9a52-8a4e-4c55-9d1b-1b0f6d1e7a3c, "<op_id>/<qualified_name>")`, so later operations in the
  same file can refer to it. Ingest never sets description, owner or tags.
- Who did an operation matters twice. An `edit_node` from ingest is refused, because ingest never sets
  description, owner or tags. An `ingest_node` from a person is refused, because people do not create
  nodes. `add_edge`, `repoint_edge` and `delete_subtree` can come from either.

## The report

```json
{
  "op_id": "op0001",
  "summary": {"nodes": {"restored": 547, "merged": 73, "skipped": 0}, "edges": {"restored": 531, "merged": 497, "skipped": 4}},
  "nodes": [
    {"id": "…", "qualified_name": "warehouse.sales.employees", "outcome": "merged", "reason": "name_in_use", "merged_into": "…", "filled": ["description", "tags"], "kept_live": ["owner"], "not_restored": {"owner": "jo"}},
    {"id": "…", "qualified_name": "warehouse.sales.invoices", "outcome": "restored"}
  ],
  "edges": [
    {"from": "…", "to": "…", "type": "references", "outcome": "skipped", "reason": "removed_by_person", "now": ["…", "…"]}
  ]
}
```

The report has one entry for every node and arrow the delete removed, in parents-first order. `merged`
and `skipped` entries always carry a reason. A merged node lists the fields filled from the journal,
the fields that kept their live value, and the old values that were not applied. When a filled value
replaced one that someone had written on the live node, `replaced` shows what was there. A merged or skipped
arrow shows where its ends are now (`now`). A row blocked by a later delete names that delete in
`waiting_on`. Undoing that delete retries the row, and the next printout of this report shows the new
outcome.

The redo report has the same shape, one entry for every node and arrow the delete removed:

```json
{
  "op_id": "op0001",
  "redo_of": "undo:op0001",
  "summary": {"nodes": {"removed": 547, "kept": 0, "reverted": 72, "unchanged": 1}, "edges": {"removed": 947, "kept": 0, "unchanged": 85}},
  "nodes": [
    {"id": "…", "qualified_name": "warehouse.sales.employees", "outcome": "reverted", "target": "…", "put_back": ["description", "tags"], "kept": {}},
    {"id": "…", "qualified_name": "warehouse.sales.invoices", "outcome": "removed"}
  ],
  "edges": [
    {"from": "…", "to": "…", "type": "references", "outcome": "removed", "was": ["…", "…"]}
  ]
}
```

A node is `removed`, `kept` (reason `written_since_undo` or `still_in_use`), `reverted` (a merged node:
`put_back` lists the fields that got their earlier value, `kept` the ones a person has written since) or
`unchanged` (undo had nothing to take back, or a later delete got there first). An arrow is `removed`,
`kept` (`end_kept`) or `unchanged`. Rows of another delete that go back to waiting are listed apart,
under `other_nodes` and `other_edges`.

## What the checker checks

The checker imports nothing from the tool. It replays the seed and the operations in plain dictionaries
(`replay.py`, its own copy of the operation rules), then compares the replay with the saved dumps and
reports.

- **0** The dump before undo equals the replayed graph, so apply did what the operations say.
- **1** Nothing a person did after the delete is lost: the fields they wrote, the arrows they added,
  the arrows they moved away and the nodes they deleted are all as they were just before undo.
- **2** Every removed node whose name is free, and whose parent is live or merged, is back under its
  old id with its description, owner and tags.
- **3** No two live nodes share a qualified_name.
- **4** The report lists every removed node and arrow exactly once, and each outcome matches the dump.
  A restored row is live, a merged row's target is live under the same name, and a skipped row is absent.
  A merged row's kept fields are unchanged, its filled fields hold the journal value, and `replaced` is
  what the target held before.
- **5** `effective_tags` recomputed from the dump matches, and no arrow points at a missing node.
- **6** A second undo gives the same dump and the same report, byte for byte, and writes nothing.
- **7** Crashed undos (CRASH_AFTER = the first write, a few in the middle, the last, past the end, two
  and three crashes in a row) finish with the same dump and report as a clean run. A dump between runs
  has no arrow to a missing node, has correct tags and has unique names.
- **8** Load, apply, dump, undo and dump take under 60 seconds (about 1.5 seconds), and so does the whole
  run with every extra undo and redo (about 17 seconds).

For every redo, from the dump before it, the dump after it and its report:

- **R1** Redo adds nothing, every value a person wrote after the undo is as it was, and a field only
  changes where the report says it was put back.
- **R2** The report lists every node and arrow the delete removed exactly once, each outcome matches the
  dumps, and nothing left the graph without being reported.
- **R3** Names are unique, no arrow points at a missing node, and `effective_tags` is correct.
- **R4** Where one delete was undone, the graph after redo equals the one the checker works out on its
  own from the dumps around the undo and the changes made since.

On the main scenario it also checks that redo straight after undo gives the dump from before the undo,
byte for byte; that a second redo changes nothing; that undo after redo gives the first undo's dump and
report again; that 10 crashed redos finish to the same dump and report; and that a redo after a crashed
undo, and an undo after a crashed redo, are refused. The 13 redo stories of the small fixture are each
held against what the design's story shows.

It also checks the 11 cases the generator planted (`planted.json`). Applying the operations file again
changes nothing, and an op_id re-used for another operation is refused without changing anything. For
the nine small runs, one per story of the design:
- **Files a and b.** Undoing both deletes gives back the dump right after load, byte for byte. Rows
  blocked by d2 wait on d2, and come back when d2 is undone.
- **File c.** It ends with one `sales.orders` owned by ben, of type view, with the old description and
  tags. The two undo orders differ only in which id survives.
- **Files d, e and f.** The report and the end state are the ones the story shows. For d, the report is
  the design's example report, word for word.

## Repository

- `graphundo.py`: the command-line entry point.
- `graph_undo/store.py`: async access to SQLite. Every sqlite3 call runs on one worker thread behind
  an async API, so the event loop never blocks on the disk. It counts writes for CRASH_AFTER.
- `graph_undo/schema.py`: the live tables (`nodes`, `edges`) and the journal (`ops`, `field_writes`,
  `edge_removals`, `removed_nodes`, `removed_edges`).
- `graph_undo/graph.py`: the reads and writes that apply, undo and the tag propagator share, on the
  live graph and the operation log.
- `graph_undo/loader.py`, `applier.py`, `tags.py`, `undo.py`, `dumper.py`, `cli.py`: load, apply, the
  tag propagator, undo, dump and the command line.
- `graph_undo/rounds.py`, `redo.py`: the entries that tell a repeated undo or redo from a new round, and
  redo.
- `replay.py`: the operations applied to plain dictionaries. The checker holds the tool against it.
  The generator applies each operation it writes to it, so every operation is valid when it runs.
- `gen_fixture.py`, `run_scenario.py`, `checker.py`, `mutation_check.py`: test data, harness, checker,
  and the checker's own test.

The database is one SQLite file in SQLite's default rollback-journal mode. While a transaction runs,
SQLite keeps a journal next to it. A crash can leave that journal behind, and the next command that
opens the database rolls the unfinished step back. `load` builds the new database as
`graph.db.loading` and renames it to `graph.db` at the end, so a load that fails leaves the old
database as it was.
