# When the model was wrong

This project is a command-line tool that keeps a graph in a SQLite file. A graph here is a set of
items, such as tables and columns, joined by links. The tool can delete an item along with
everything under it, and later undo or redo that delete, even after people and an automated import
job have kept editing the graph. An AI model helped design and build it over three sessions, which
are saved in [AI sessions/](AI%20sessions/).

Below are the model's mistakes in the design and the code. Some I pointed out myself. Others came up
when I asked the model to check its own work.

## Designing undo

- It claimed the design broke the task by giving each item only one parent. The task describes a
  tree, so the design was right.
- It called the rule that an item's parent must be in the graph a problem. Undo brings a parent
  back together with everything under it, so nothing was wrong.
- Its example of undo reversing a link a person had moved could not happen, because a link to a
  deleted item is deleted too.
- It listed edits to deleted items as a gap in the design. An item that is no longer in the graph
  can't be edited, so rejecting the edit was enough.
- The query that finds everything under an item could loop forever if two items ever contained each
  other.
- New items got random ids, so two runs on the same input gave different results, and the
  crash-recovery test would have failed.
- The output order could change between runs, so the same graph could print differently.
- It had no rule for an item that came back as a different kind of thing, such as a view instead of
  a table.
- With two deletes, undoing the older one first could lose some items and links for good.
- Its first design showed how tags clash, but not description or owner.
- It had no rule for a link a person moved elsewhere after the delete, so undo would have put the
  old link back.
- The design said a person could create an item with a deleted name, but no operation lets a person
  create an item.
- It didn't say what undo does when given something other than a delete, or an id that doesn't
  exist.
- It didn't say how "most recent" is judged for a value an earlier undo had put back, and one
  example gave the wrong owner without it.

## Building undo

- It used a SQLite mode that keeps extra files next to the database, while the task asks for one
  file.
- It dropped repeated tags, which changed what people had written.
- It skipped recalculating inherited tags after a delete, which the task requires.
- It refused operations the task never asks it to refuse, such as a delete by the import job.
- It silently skipped an operation whose id had already been used, so an edit could disappear
  without a word.
- The undo report didn't show which values it had overwritten.
- Only three of the design's six example stories were actually tested.
- Its checker didn't notice a deliberately broken version of undo.
- It wrote the same logic in several places and left code that could never run.
- Undo wasn't recorded as an operation, although the design says every change records who made it.
- The design stated the merge rule two different ways.
- Its checker took different inputs from the ones in the design, so results produced by hand
  couldn't be checked.

## Designing and building redo

- With two deletes, a delete that had been redone could still come back.
- Undoing and redoing a second time failed, because marks left by the first redo made the second
  one skip everything.
- Redo could do nothing at all, depending on which undo had brought the items back.
- It had no rule for items that changed between the undo and the redo.
- One of its own recommended rules let undo bring back an item a person had deleted.
- Undo reports called items "restored" when they weren't in the graph.
- Undo reports called items "skipped" when they were back in the graph.
- The design took merges back after removing items, which is the wrong order, because a merge can
  sit on an item that redo then removes.
- Redo straight after undo could keep items because of an older undo that no longer applied.
- It didn't say what happens if undo stops halfway and redo is run next.
- A link with one end leaving and one staying matched two rules at once.
- The design promised that redo straight after undo always gives back the earlier graph, which
  isn't always true.
- Its plan for the checker had no test that redo doesn't keep too much.
