# Decisions

Undo does not rewind the graph. It looks at the graph as it is now and puts back what one delete
removed. It only adds things back and fills in empty fields. It never removes anything, and never
overwrites a change a person made after the delete. The full reasoning is in
[docs/design.html](docs/design.html), sections 3 and 4.

## How undo treats changes made after the delete

**Unrelated changes** (independent) touched things the delete did not, such as the deleted node's parent, a
dashboard that pointed into the deleted area, or a new node somewhere else. Undo leaves them alone,
and restored nodes still feel their effects: a tag added above the deleted node shows up on every
restored node below it.

**Clashing changes** (conflicting) wrote a field that undo also wants to write. The newest value wins.
- Undo's saved copy (the journal) is from before the delete, so a value a person wrote later always
  wins. The saved value is then listed under `not_restored`.
- An empty field is filled from the saved copy, unless a person emptied it on purpose.
- A tag list counts as one value: undo keeps one whole list and never mixes two. `effective_tags`
  is always recalculated, never copied back. `type` works like any other field, with ingest as the
  writer.
- Arrows follow the same idea. If a node already has a parent when undo wants to put its old parent
  arrow back, or a person moved the arrow elsewhere after the delete, undo skips the old arrow and
  the later placement stays.

**Changes caused by the delete** only happened because of it. Usually the ingest saw that a name was
free and created a new node with a new id. Undo treats this as the same node coming back: it merges
the old node into the new one, keeps the new id (newer things already point at it), and points the
old arrows at it.

**When undo cannot bring something back.** If a node's parent is no longer in the graph, because a
later delete removed it, the node is skipped and the report names the delete it is waiting on. When
that delete is undone, the waiting node comes back in the same run. An arrow is skipped the same way
when one of its ends is not live.

## Redo

`redo <op_id>` takes away again what an undo put back, based on the graph as it is now. It never
removes or changes anything a person did after the undo.
- Unrelated changes are left alone.
- If a person edited something undo put back, or attached something to it, that thing stays, along
  with its parents and arrows. A field a person rewrote keeps the new value.
- An ingest update that only happened because the node was back does not stop redo.

You can undo, redo, undo again and so on. If one of them stops halfway, for example after a crash,
the other is refused until the same command is run again to finish it. If a later delete removed a
node, redo leaves that node to the later delete.

## Model suggestions I rejected

Design:
- **Letting a node have several parents.** Words like subtree, root and "its parent" describe a
  tree, so I kept one parent per node.
- **A wrong example** where undo reversed a person moving an arrow. An arrow to a deleted node is
  deleted too, so this could not happen. The model fixed it when I asked for the steps, and I chose
  the rule that undo skips an arrow a person moved elsewhere.
- **Treating the "parent must be live" rule as a problem.** Undo brings back parents along with their
  children, so it only needed one line of explanation.
- **Handling edits to deleted nodes.** A deleted node is no longer in the graph, so an edit to it is
  simply rejected. There is nothing more to handle.
- **A preview before undo.** The report after undo already lists everything it skipped or did not
  restore, and redo can take the undo back, so a preview adds little to a prototype.

Build:
- **Small choices I didn't want**, found when I had every one checked. WAL mode keeps extra files
  next to the database, and I wanted the graph in one file. Removing repeated tags changed what
  people had written. Skipping the tag recalculation after a delete left inherited tags wrong. I had
  all three undone.
- **Refusing commands from the "wrong" actor.** Extra refusals would turn away normal work, such as
  a delete by the import job. I kept only two: ingest can't edit fields, and people can't ingest.
- **Quietly ignoring an op_id that was already used**, even for a different command. It is now an
  error, so no edit can disappear without a word.
- **A 24-hour undo limit.** This was my own idea. I added it and then removed it from the tool: the
  test operations carry no real clock, so undo has to work whenever it is run. The limit belongs in
  the full system.

Found on re-check:
- **Mistakes I had fixed.** Each time I had the model check its work again, it found mistakes of
  its own, and I had them fixed. Examples: a query that could loop forever, random ids that made two
  runs differ, a rule that let undo put back an arrow a person had moved, lost nodes when two
  deletes overlapped, redo failing the second time round, and reports calling nodes restored when
  they weren't. The full list is in [MODEL_MISTAKES.md](MODEL_MISTAKES.md).

I also thought about keeping a duplicate node instead of merging. I chose merging, so that only one
live node ever has a given name.

## What I checked myself

- **The design.** After every change I reviewed what had changed, and questioned the decisions the
  model had made, point by point, before accepting or rejecting each one.
- **The undo stories.** I stepped through each undo tab, asked how fields other than tags clash, and
  asked for every special case to get its own story from start to end. These stories are now tests
  (`fixture_small/ops_a` to `ops_f`).
- **The model's claims.** I made the model re-check several before accepting them: several parents,
  undoing things other than deletes, only ingest creating nodes, ids built from op_id and name, and
  edits to deleted nodes.
- **The code against the design.** I had a model list every place where the code differed from the
  design, then went through the list and decided how to handle each one.
  I had repeated code removed and checked that the output was exactly the same before and after.
- **Random testing.** I had undo and redo tested with thousands of random mixes of edits, deletes,
  undos and redos, checking the graph's rules after every command. It found cases redo did not handle.
  For each one I had it explained, chose the fix, and had the tests rerun until none failed.
