"""Names and small helpers shared by the tool's modules."""

from __future__ import annotations

import asyncio
import datetime
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

HUMAN = "human"
INGEST = "ingest"
ACTORS = (HUMAN, INGEST)

CONTAINS = "contains"
EDGE_TYPES = (CONTAINS, "references")

# The three fields people write. Ingest never sets them.
AUTHORED = ("description", "owner", "tags")
# type is written by ingest (and by the seed). On merge it follows the same "most recent write wins" rule.
WRITTEN_FIELDS = AUTHORED + ("type",)


class InputError(Exception):
    """A file, argument or environment value the tool cannot use. Nothing has been written."""


def valid_value(field: str, value: Any) -> bool:
    """description and owner are a string or null; tags is a list of strings, kept exactly as written."""
    if field == "tags":
        return isinstance(value, list) and all(isinstance(tag, str) for tag in value)
    return value is None or isinstance(value, str)


def utc_now() -> str:
    """The time an operation is logged, as stored in ops.applied_at."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def tags_json(tags: Iterable[str]) -> str:
    """The one way a tag list is stored, so stored text can be compared directly."""
    return json.dumps(list(tags), ensure_ascii=False)


async def read_jsonl(path: str | Path) -> list[tuple[int, dict[str, Any]]]:
    """Read a JSONL file off the event loop. Returns (line number, object) pairs; blank lines are ignored."""
    try:
        text = await asyncio.to_thread(Path(path).read_text, encoding="utf-8")
    except OSError as exc:
        raise InputError(f"cannot read {path}: {exc.strerror or exc}") from exc
    records: list[tuple[int, dict[str, Any]]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise InputError(f"{path}:{lineno}: not valid JSON ({exc.msg})") from exc
        if not isinstance(value, dict):
            raise InputError(f"{path}:{lineno}: expected a JSON object")
        records.append((lineno, value))
    return records
