# Decisions

Undo does not roll the graph back. It applies the inverse of one delete to the graph as it is now: it
only adds rows and fills fields, and never removes a row or replaces a value a person wrote after the
delete. How undo and redo decide each node and arrow, with every reason, is in
[docs/design.html](docs/design.html), sections 3 and 4.

## Conflict rules

**Independent edits** touched something the delete did not, such as the deleted root's parent or
siblings, a dashboard that pointed into the deleted area, or a node created elsewhere. Rule: undo leaves
them exactly as they are, and restored nodes pick up their effects. A tag a person added above the
deleted root flows into every restored node below it.

**Conflicting edits** wrote a field that undo would also write. Rule: for each field, the most recent
write wins.
- The journal was written before the delete, so a value a person wrote after it always wins. The
  journal value is then listed under `not_restored`.
- A field nobody wrote is filled from the journal. A field a person emptied on purpose stays empty.
- Tags count as one value: the two lists are never mixed. `effective_tags` is never copied back but
  recomputed. `type` follows the same rule, with ingest as the writer.

**Caused edits** only happened because the delete did. The typical case: the ingest found a name free
and created a new node with a new id. Rule: undo treats it as the same thing coming back. It merges the
old node into the live one, keeps the live id (everything made since already points at it), and re-aims
the old arrows at it.

## Redo (the extension)

`redo <op_id>` takes back what the undo put back, on the graph as it is now, and never removes or
changes anything a person did after the undo.
- **Independent** edits touched nothing undo put back. They are left as they are.
- **Conflicting**: a person wrote on, or attached something to, a thing undo put back. That thing stays,
  with its parents and arrows. A filled field a person rewrote stays.
- **Caused**: an ingest update that only happened because the node was back does not hold redo back.

Undo and redo can alternate in rounds, each refused while the other is unfinished. A node a later delete
removed is left to that delete.

## What the model proposed that I rejected, and why

Design:
- **Several parents per node**, offered as a fix for a "conflict" with the task. I had it re-read the
  task: "subtree", "root" and "its parent" describe a tree, so one parent per node stayed.
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
- **Random testing.** I had undo and redo tested with thousands of random sequences of edits, deletes,
  undos and redos, checking the invariants after every command. It found cases redo did not handle; I
  had each one explained and decided the fix, then had the tests rerun until none failed.
