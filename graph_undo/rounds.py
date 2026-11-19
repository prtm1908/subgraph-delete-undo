"""Rounds: telling a repeated undo or redo from a new one.

One undo and the redo that follows it are a round. Each undo and each redo has an entry of its own in ops,
with the delete it is for (target), its round number and a finished mark. The same command can mean two
things, so both commands look at the newest entry for the delete:

    undo d1   no entry                 the first undo: round 1
              newest is an undo        a repeat or a resume of that undo (a finished one writes nothing)
              newest is a redo         a new round, if that redo is finished; refused if it is not
    redo d1   no entry                 refused: the delete was never undone
              newest is an undo        the redo of that round, if that undo is finished; refused if it is not
              newest is a redo         a repeat or a resume of that redo

The op_id of an entry ('undo:d1', 'redo:d1', then 'undo:d1:2', ...) is only a label. Nothing is read out of
it: journal rows point at an entry by its running number.
"""

from __future__ import annotations

from dataclasses import dataclass

from .common import HUMAN
from .graph import log_op
from .store import Store

UNDO, REDO = "undo", "redo"


class CommandError(Exception):
    """undo or redo was asked for something it cannot do. Nothing has been written."""


@dataclass(frozen=True)
class Entry:
    seq: int
    op_id: str
    kind: str
    round: int
    finished: bool


async def check_delete(store: Store, op_id: str, command: str) -> None:
    """Both commands take the op_id of a subtree delete that was applied."""
    op = await store.fetchone("SELECT kind, status FROM ops WHERE op_id = ?", (op_id,))
    if op is None:
        raise CommandError(f"no operation with op_id {op_id!r}")
    kind, status = op
    if kind != "delete_subtree":
        raise CommandError(f"{op_id} is {kind!r}, not a delete_subtree; {command} takes the op_id of a subtree delete")
    if status != "applied":
        raise CommandError(f"{op_id} was rejected when it was applied, so it removed nothing")


async def newest_entry(store: Store, delete_op: str, kind: str | None = None) -> Entry | None:
    """The newest undo or redo entry for this delete (or the newest of one kind)."""
    row = await store.fetchone(
        "SELECT seq, op_id, kind, round, finished FROM ops "
        "WHERE target = ?1 AND kind IN ('undo', 'redo') AND (?2 IS NULL OR kind = ?2) ORDER BY seq DESC LIMIT 1",
        (delete_op, kind),
    )
    return None if row is None else Entry(row[0], row[1], row[2], row[3], bool(row[4]))


async def entry_for(store: Store, delete_op: str, command: str) -> Entry:
    """The entry this call works under: the unfinished or finished one it repeats, or a new one."""
    other = REDO if command == UNDO else UNDO
    newest = await newest_entry(store, delete_op)
    if newest is None:
        if command == REDO:
            raise CommandError(f"{delete_op} has not been undone, so there is nothing to redo")
        return await _start(store, delete_op, UNDO, 1)
    if newest.kind == command:
        return newest
    if not newest.finished:
        raise CommandError(
            f"the {other} of {delete_op} is not finished; run '{other} {delete_op}' again first"
        )
    return await _start(store, delete_op, command, newest.round + 1 if command == UNDO else newest.round)


async def _start(store: Store, delete_op: str, kind: str, round_: int) -> Entry:
    """Log a new undo or redo. One transaction, so a resumed or repeated run finds it."""
    async with store.transaction():
        base = f"{kind}:{delete_op}" if round_ == 1 else f"{kind}:{delete_op}:{round_}"
        label = base
        while await store.fetchval("SELECT 1 FROM ops WHERE op_id = ?", (label,)):
            label += "'"  # the label is taken by another operation; any free one will do
        seq = await log_op(store, label, kind, HUMAN, {f"{kind}_of": delete_op, "round": round_})
        await store.write("UPDATE ops SET target = ?, round = ?, finished = 0 WHERE seq = ?", (delete_op, round_, seq))
    return Entry(seq, label, kind, round_, False)


async def finish(store: Store, entry: Entry) -> None:
    """Mark the entry complete. The last write of an undo or a redo."""
    async with store.transaction():
        await store.write("UPDATE ops SET finished = 1 WHERE seq = ?", (entry.seq,))
