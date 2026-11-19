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
first. In this order nothing has to wait. Within one undo a row that has an outcome is never decided
again. That makes a second run write nothing, and a crashed run resume from its last committed step.

## Redo (the extension)

`redo <op_id>` takes back what the undo put back, on the graph as it is now. It does not replay the
delete, and it never removes or changes anything a person did after the undo.
- **Independent** edits touched nothing undo put back. They are left as they are.
- **Conflicting**: a person wrote on, or attached something to, a thing undo put back. That thing stays
  (`written_since_undo`, `still_in_use`), with its parents and arrows. A filled field a person rewrote stays.
- **Caused**: an ingest update that only happened because the node was back does not hold redo back.
- After redo every row is either kept or open again, so a row that waited on another delete stops waiting.
- Redo of d1 takes out what d1 removed, whichever undo command put it back.
- Undo and redo can be repeated and go back and forth in rounds. Each is refused while the other is unfinished.
- If a later delete removed something first, redo leaves it alone and the report says so. A later undo
  of the older delete does not bring it back either: it waits on that later delete (`deleted_again`).
- An undo after a redo checks the rows redo kept, and decides again any that are no longer true.

## What the model proposed that I rejected, and why

Design:
- **Several parents per node**, offered as a fix for a "conflict" with the task. I had it re-read the
  task: "subtree", "root" and "its parent" describe a tree, so one parent per node stayed.
- **Fixing the design page before showing me the problems.** I stopped it before any edit landed,
  had it list the problems first, and approved every change one by one after that.
- **A wrong example** of undo reversing a person's re-point (an arrow to a deleted node is deleted
  too). It corrected it when I asked for the steps, and I chose the rule that undo skips an arrow a
  person moved away.
- **Invariant 2 as a problem.** Undo brings back parent and child, so only the reading of "parent is
  live" needed one line.
- **Handling edits to deleted nodes.** The task does not ask for it; it stays one "rejected" sentence.
- **A preview before undo:** good UX, not core capability for a prototype.

Build:
- **Choices that bent the task's wording**, found when I had every deviation checked against it:
  WAL mode (extra files beside "one SQLite file"), dropping repeated tags ("tags (a list)"), and
  skipping the tag recompute after a delete. All three were put back.
- **Refusing operations by the "wrong" actor**, beyond the task's one rule. Only two refusals remain:
  ingest can't edit fields, and people can't ingest.
- **Silently skipping an op_id that was already used**, even for a different operation. Now an error.
- **The 24-hour undo limit**, my own idea: I had it enforced, then removed it, because the task says
  any delete can be undone later and puts retention out of scope.

I also weighed keeping a duplicate node instead of merging, and kept merging: invariant 3 allows one
live node per name, and the task's outcomes include "merged".

## What I verified myself

- **The design, after every iteration.** I reviewed the whole page each time it changed, and read
  every audit against the task point by point, accepting, rejecting or questioning each one myself.
- **The undo stories.** I stepped through the undo tabs, asked for the other fields' clashes beside
  tags, and asked for every exceptional case to become a story of its own, start to end. Those stories
  are now tests (`fixture_small/ops_a` to `ops_f`).
- **Claims against the task's wording.** I made the model re-check several before accepting them:
  several parents, undo of anything but a delete, only ingest creating nodes, ids built from op_id and
  name, edits to deleted nodes.
- **The code against the task and the design.** I had every deviation listed, decided each one, and
  had duplicated code removed with outputs proven byte-identical before and after.
