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
- The query that finds everything under an item could loop forever if two items ever contained each
  other.
- New items got random ids, so two runs on the same input gave different results, and the
  crash-recovery test would have failed.
- The output order could change between runs, so the same graph could print differently.
- It had no rule for an item that came back as a different kind of thing, such as a view instead of
  a table.
- With two deletes, undoing the older one first could lose some items and links for good.

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

## Designing and building redo

- With two deletes, a delete that had been redone could still come back.
- Undoing and redoing a second time failed, because marks left by the first redo made the second
  one skip everything.
- Redo could do nothing at all, depending on which undo had brought the items back.
- It had no rule for items that changed between the undo and the redo.
- One of its own recommended rules let undo bring back an item a person had deleted.
- Undo reports called items "restored" when they weren't in the graph.
- It added a 60-second limit on the whole test run, which the task doesn't ask for and which could
  fail on slower machines.
