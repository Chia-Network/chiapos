# Copyright 2025 Chia Network Inc
# Mount chiafs: FUSE filesystem backed by Chia plots.

import argparse
import logging
import re
from typing import Optional

import pyfuse3

from .fuse_ops import ChiaFsOperations

_SIZE_SUFFIX = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}


def parse_size(size_str: str) -> Optional[int]:
    """Parse a size string like 100G, 1T, 500M into bytes. Returns None if invalid."""
    size_str = size_str.strip().upper()
    if not size_str:
        return None
    m = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT])?$", size_str)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2)
    if suffix:
        num *= _SIZE_SUFFIX[suffix]
    return int(num)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mount a FUSE filesystem backed by a directory that also holds Chia plots. "
        "When the filesystem needs more space, Chia plots are deleted. "
        "When files are deleted, new Chia plots are created in the freed space."
    )
    parser.add_argument(
        "backing_dir",
        type=str,
        help="Backing directory (contains _chiafs/ and *.plot files)",
    )
    parser.add_argument(
        "mountpoint",
        type=str,
        help="Mount point for the FUSE filesystem",
    )
    parser.add_argument(
        "-k",
        "--plot-size",
        type=int,
        default=20,
        help="Chia plot k size for newly created plots (default: 20; min 18, max 50)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--debug-fuse",
        action="store_true",
        help="Enable FUSE debug output",
    )
    parser.add_argument(
        "--max-size",
        type=str,
        metavar="SIZE",
        default=None,
        help="Maximum backing store size (e.g. 100G, 1T). Do not create plots beyond this.",
    )
    args = parser.parse_args()

    if args.plot_size < 18 or args.plot_size > 50:
        parser.error("Plot size -k must be between 18 and 50")

    max_backing_bytes = None
    if args.max_size is not None:
        max_backing_bytes = parse_size(args.max_size)
        if max_backing_bytes is None or max_backing_bytes <= 0:
            parser.error("--max-size must be a positive size (e.g. 100G, 1T)")

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    operations = ChiaFsOperations(
        args.backing_dir,
        plot_k=args.plot_size,
        max_backing_bytes=max_backing_bytes,
    )
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add("fsname=chiafs")
    if args.debug_fuse:
        fuse_options.add("debug")

    pyfuse3.init(operations, args.mountpoint, fuse_options)
    try:
        import trio
        trio.run(pyfuse3.main)
    except Exception:
        pyfuse3.close(unmount=False)
        raise
    pyfuse3.close()


if __name__ == "__main__":
    main()
