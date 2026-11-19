#!/usr/bin/env python3
"""Undo in a shared graph: load, apply, undo, dump. See README.md."""

import sys

from graph_undo.cli import main

if __name__ == "__main__":
    sys.exit(main())
