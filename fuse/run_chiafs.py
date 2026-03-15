#!/usr/bin/env python3
# Copyright 2025 Chia Network Inc
# Entry point to run chiafs from the fuse directory.
# Usage: python run_chiafs.py <backing_dir> <mountpoint> [options]
#    or: python -m run_chiafs <backing_dir> <mountpoint> [options]

import sys
from pathlib import Path

# Ensure fuse directory is on path so "chiafs" package is found
_fuse_dir = Path(__file__).resolve().parent
if str(_fuse_dir) not in sys.path:
    sys.path.insert(0, str(_fuse_dir))

from chiafs.main import main

if __name__ == "__main__":
    main()
