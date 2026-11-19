"""Undo for subtree deletes in a shared graph kept in one SQLite file.

Modules, in the order a command uses them:

    store    async access to the SQLite file; counts writes for CRASH_AFTER
    schema   the live tables (nodes, edges) and the journal tables
    common   names and helpers every module uses
    graph    reads and writes on the live graph that apply, undo and tags share
    loader   load: a new database from seed.jsonl
    applier  apply: run ops.jsonl, journal what a delete removes
    tags     the tag propagator, used by apply and undo
    undo     undo <op_id>: restored / merged / skipped for every removed row
    dumper   dump: the live graph as sorted JSONL
    cli      the command line
"""
