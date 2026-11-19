# Decisions

Undo does not roll the graph back. It applies the inverse of one delete to the graph as it is now: it
only adds rows and fills fields, and never removes a row or replaces a value a person wrote after the
delete.

## Conflict rules

**Independent edits** touched something the delete did not, such as the deleted root's parent or
siblings, a dashboard that pointed into the deleted area, or a node created elsewhere. Rule: undo leaves
them exactly as they are, and restored nodes pick up their effects. A tag a person added above the
deleted root flows into every restored node below it.

**Conflicting edits** wrote a field that undo would also write. Rule: for each field, the most recent
write wins.
- The seed counts as written at the start.
- A value put back by an undo keeps the time of its original write.
- Everything in the journal was written before the delete, so a value a person wrote after the delete
  always wins. The journal value is then listed under `not_restored`.
- A field nobody wrote is filled from the journal.
- A field a person emptied on purpose stays empty. `field_writes` tells it apart from a field nobody wrote.
- Tags count as one value: the two lists are never mixed.
- `effective_tags` is never copied back. Undo recomputes it from the live graph in every step.
- `type` follows the same rule, with ingest as the writer.

**Caused edits** only happened because the delete did. The typical case: the ingest found a name free
and created a new node with a new id. Rule: undo treats it as the same thing coming back. It merges the
old node into the live one, keeps the live id (everything made since already points at it), and re-aims
the old arrows at it.

## The decisions undo makes

For each removed node, parents first, one transaction per node:
- Its name is held by a live node: **merged** into that node.
- Otherwise its parent is live, or was merged: **restored** with its old id and all its fields.
- Otherwise: **skipped** (`parent_not_live`). `waiting_on` names the delete that holds the parent.

For each removed arrow, after resolving each end to where it is now. The first match wins:
1. An end is not live: skipped (`endpoint_not_live`), waiting on that end's delete.
2. The arrow already exists: merged (`already_present`).
3. It is a contains arrow and the child already has a parent: skipped (`child_has_other_parent`).
   The later placement wins.
4. A person re-pointed this arrow away after the delete: skipped (`removed_by_person`).
5. It would make a node its own ancestor: skipped (`would_create_cycle`).
6. Otherwise it is added: restored, or merged (`endpoint_merged`) when an end was merged.

Undoing a later delete retries the rows that were waiting on it: newest journal first, then parents
first. In this order nothing has to wait. A row that already has an outcome is never decided again.
That makes a second run write nothing, and a crashed run resume from its last committed step.

## What the model proposed that I rejected, and why

_To write from the AI sessions._

## What I verified myself

_To write from the AI sessions._
