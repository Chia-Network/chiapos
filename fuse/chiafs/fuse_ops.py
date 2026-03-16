# Copyright 2025 Chia Network Inc
# FUSE operations for chiafs: uses Store and PlotManager.

import errno
import logging
import os
import stat
import threading
from typing import Optional

import pyfuse3
from pyfuse3 import (
    EntryAttributes,
    FileHandleT,
    FileInfo,
    FUSEError,
    InodeT,
    ReaddirToken,
    RequestContext,
    SetattrFields,
    StatvfsData,
)

from .plot_manager import PlotManager
from .store import Store

log = logging.getLogger(__name__)


def _plot_poll_loop(plot_mgr: PlotManager, stop_event: threading.Event) -> None:
    """Daemon loop: every 10 seconds, create a plot if under max size and space available."""
    while not stop_event.wait(timeout=10):
        try:
            plot_mgr.schedule_create_plot_if_possible()
        except Exception:
            log.exception("Plot schedule poll failed")


def _row_to_attrs(
    row: tuple,
    uid: int,
    gid: int,
) -> EntryAttributes:
    ino, parent_ino, name, mode, size, mtime_ns, ctime_ns, is_dir = row
    entry = EntryAttributes()
    entry.st_ino = ino
    entry.generation = 0
    entry.entry_timeout = 300
    entry.attr_timeout = 300
    entry.st_mode = mode
    entry.st_nlink = 2 if is_dir else 1
    entry.st_uid = uid
    entry.st_gid = gid
    entry.st_rdev = 0
    entry.st_size = size
    entry.st_blksize = 4096
    entry.st_blocks = (size + 511) // 512
    entry.st_atime_ns = mtime_ns
    entry.st_mtime_ns = mtime_ns
    entry.st_ctime_ns = ctime_ns
    return entry


class ChiaFsOperations(pyfuse3.Operations):
    """FUSE operations: backing store + plot management."""

    enable_writeback_cache = True

    def __init__(
        self,
        backing_dir: str,
        plot_k: int = 20,
        max_backing_bytes: Optional[int] = None,
    ):
        super().__init__()
        self.store = Store(backing_dir)
        self.plot_mgr = PlotManager(
            backing_dir,
            k=plot_k,
            max_backing_bytes=max_backing_bytes,
            get_data_size=self.store.total_data_size,
        )
        self.backing_dir = backing_dir
        self._plot_poll_stop = threading.Event()
        self._plot_poll_thread: Optional[threading.Thread] = None

    def init(self) -> None:
        """Called when FUSE starts; poll every 10s and create plots when under max size."""
        super().init()
        self._plot_poll_thread = threading.Thread(
            target=_plot_poll_loop,
            args=(self.plot_mgr, self._plot_poll_stop),
            daemon=True,
        )
        self._plot_poll_thread.start()

    def destroy(self) -> None:
        self._plot_poll_stop.set()
        self.store.close()

    def _uid_gid(self, ctx: Optional[RequestContext]) -> tuple:
        if ctx is not None:
            return (ctx.uid, ctx.gid)
        return (os.getuid(), os.getgid())

    async def getattr(
        self, inode: InodeT, ctx: Optional[RequestContext] = None
    ) -> EntryAttributes:
        try:
            uid, gid = self._uid_gid(ctx)
            if inode == pyfuse3.ROOT_INODE:
                row = self.store.get_root()
            else:
                row = self.store.get_attr(inode)
            if row is None:
                raise FUSEError(errno.ENOENT)
            return _row_to_attrs(row, uid, gid)
        except FUSEError:
            raise
        except Exception as e:
            log.exception("getattr inode=%s: %s", inode, e)
            raise FUSEError(errno.EIO)

    async def lookup(
        self, parent_inode: InodeT, name: bytes, ctx: RequestContext
    ) -> EntryAttributes:
        uid, gid = self._uid_gid(ctx)
        if name == b".":
            return await self.getattr(parent_inode, ctx)
        if name == b"..":
            row = self.store.get_attr(parent_inode)
            if row is None:
                raise FUSEError(errno.ENOENT)
            parent_ino = row[1]
            if parent_ino == parent_inode:
                return await self.getattr(parent_inode, ctx)
            row = self.store.get_attr(parent_ino)
            if row is None:
                raise FUSEError(errno.ENOENT)
            return _row_to_attrs(row, uid, gid)
        name_str = name.decode("utf-8", errors="surrogateescape")
        row = self.store.lookup(parent_inode, name_str)
        if row is None:
            raise FUSEError(errno.ENOENT)
        return _row_to_attrs(row, uid, gid)

    async def opendir(self, inode: InodeT, ctx: RequestContext) -> FileHandleT:
        if inode == pyfuse3.ROOT_INODE:
            row = self.store.get_root()
            dir_ino = 1  # store's root inode for readdir/list_dir
        else:
            row = self.store.get_attr(inode)
            dir_ino = inode
        if row is None or not row[7]:
            raise FUSEError(errno.ENOTDIR)
        return FileHandleT(dir_ino)

    async def readdir(
        self, fh: FileHandleT, start_id: int, token: ReaddirToken
    ) -> None:
        try:
            entries = self.store.list_dir(fh)
            uid, gid = os.getuid(), os.getgid()
            row_fh = self.store.get_attr(fh)
            if row_fh is None:
                log.error("readdir: get_attr(%s) returned None", fh)
                raise FUSEError(errno.EIO)
            parent_ino = row_fh[1]

            # . and .. with fixed ids 0 and 1
            if start_id == 0:
                if not pyfuse3.readdir_reply(
                    token, b".", await self.getattr(fh, None), 1
                ):
                    return
                parent_row = self.store.get_attr(parent_ino)
                if parent_row is not None and not pyfuse3.readdir_reply(
                    token, b"..", _row_to_attrs(parent_row, uid, gid), 2
                ):
                    return

            next_id = max(2, start_id)
            for i, (name, ino, is_dir) in enumerate(entries):
                if i + 2 < start_id:
                    continue
                row = self.store.get_attr(ino)
                if row is not None:
                    attr = _row_to_attrs(row, uid, gid)
                    name_b = name.encode("utf-8", errors="surrogateescape")
                    if not pyfuse3.readdir_reply(token, name_b, attr, next_id + 1):
                        return
                    next_id += 1
        except FUSEError:
            raise
        except Exception as e:
            log.exception("readdir fh=%s start_id=%s: %s", fh, start_id, e)
            raise FUSEError(errno.EIO)

    async def open(self, inode: InodeT, flags: int, ctx: RequestContext) -> FileInfo:
        row = self.store.get_attr(inode)
        if row is None:
            raise FUSEError(errno.ENOENT)
        if row[7]:
            raise FUSEError(errno.EISDIR)
        return FileInfo(fh=FileHandleT(inode))

    async def read(
        self, fh: FileHandleT, off: int, size: int
    ) -> bytes:
        path = self.store.get_data_path(fh)
        if not path.exists():
            return b""
        with open(path, "rb") as f:
            f.seek(off)
            return f.read(size)

    async def write(
        self, fh: FileHandleT, off: int, buf: bytes
    ) -> int:
        path = self.store.get_data_path(fh)
        current = path.stat().st_size if path.exists() else 0
        new_end = off + len(buf)
        if new_end > current:
            need = new_end - current
            if not self.plot_mgr.ensure_space(need):
                raise FUSEError(errno.ENOSPC)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "r+b") as f:
            f.seek(off)
            f.write(buf)
        if new_end > current:
            self.store.set_size(fh, new_end)
        return len(buf)

    async def create(
        self,
        parent_inode: InodeT,
        name: bytes,
        mode: int,
        flags: int,
        ctx: RequestContext,
    ) -> tuple:
        uid, gid = self._uid_gid(ctx)
        name_str = name.decode("utf-8", errors="surrogateescape")
        if self.store.lookup(parent_inode, name_str) is not None:
            raise FUSEError(errno.EEXIST)
        if not self.plot_mgr.ensure_space(0):
            raise FUSEError(errno.ENOSPC)
        ino = self.store.create_file(parent_inode, name_str, mode)
        row = self.store.get_attr(ino)
        assert row is not None
        return (FileInfo(fh=FileHandleT(ino)), _row_to_attrs(row, uid, gid))

    async def release(self, fh: FileHandleT) -> None:
        pass

    async def unlink(
        self, parent_inode: InodeT, name: bytes, ctx: RequestContext
    ) -> None:
        name_str = name.decode("utf-8", errors="surrogateescape")
        ino = self.store.unlink(parent_inode, name_str)
        if ino is None:
            raise FUSEError(errno.ENOENT)

    async def mkdir(
        self, parent_inode: InodeT, name: bytes, mode: int, ctx: RequestContext
    ) -> EntryAttributes:
        uid, gid = self._uid_gid(ctx)
        name_str = name.decode("utf-8", errors="surrogateescape")
        if self.store.lookup(parent_inode, name_str) is not None:
            raise FUSEError(errno.EEXIST)
        ino = self.store.create_dir(parent_inode, name_str, mode)
        row = self.store.get_attr(ino)
        assert row is not None
        return _row_to_attrs(row, uid, gid)

    async def rmdir(
        self, parent_inode: InodeT, name: bytes, ctx: RequestContext
    ) -> None:
        name_str = name.decode("utf-8", errors="surrogateescape")
        ino = self.store.rmdir(parent_inode, name_str)
        if ino is None:
            raise FUSEError(errno.ENOENT)

    async def releasedir(self, fh: FileHandleT) -> None:
        pass

    async def setattr(
        self,
        inode: InodeT,
        attr: EntryAttributes,
        fields: SetattrFields,
        fh: Optional[FileHandleT],
        ctx: RequestContext,
    ) -> EntryAttributes:
        if getattr(fields, "update_size", False):
            path = self.store.get_data_path(inode)
            if path.exists():
                current = path.stat().st_size
                if attr.st_size < current:
                    with open(path, "r+b") as f:
                        f.truncate(attr.st_size)
                    self.store.set_size(inode, attr.st_size)
                elif attr.st_size > current:
                    need = attr.st_size - current
                    if not self.plot_mgr.ensure_space(need):
                        raise FUSEError(errno.ENOSPC)
                    with open(path, "r+b") as f:
                        f.seek(attr.st_size - 1)
                        f.write(b"\x00")
                    self.store.set_size(inode, attr.st_size)
        return await self.getattr(inode, ctx)

    async def statfs(self, ctx: RequestContext) -> StatvfsData:
        stat_ = StatvfsData()
        stat_.f_bsize = 4096
        stat_.f_frsize = 4096
        total_plots = self.plot_mgr.total_plots_size()
        data_size = self.store.total_data_size()
        free = self.plot_mgr.effective_free_space()
        stat_.f_blocks = (total_plots + data_size + free) // 4096
        stat_.f_bfree = free // 4096
        stat_.f_bavail = stat_.f_bfree
        stat_.f_files = 1000000
        stat_.f_ffree = 1000000
        stat_.f_favail = 1000000
        stat_.f_namemax = 255
        return stat_
