# Copyright 2025 Chia Network Inc
# Manages Chia plots in the backing store: list, delete, create.

import os
import shutil
import threading
import uuid
from pathlib import Path
from typing import List, Optional, Callable

# Approximate final plot size in bytes for a given k (conservative).
# Formula: ((2*k)+1) * (2**(k-1)) * 0.78 bytes for uncompressed.
def plot_size_for_k(k: int) -> int:
    return int(((2 * k + 1) * (1 << (k - 1))) * 0.78)


# Plotting needs temp space ~2-3x final size in the same dirs
def plot_temp_overhead_for_k(k: int) -> int:
    return plot_size_for_k(k) * 2


class PlotManager:
    """List, delete, and create Chia plots in the backing directory."""

    PLOT_EXT = ".plot"

    def __init__(
        self,
        backing_dir: str,
        k: int = 20,
        plot_created_callback: Optional[Callable[[str], None]] = None,
        max_backing_bytes: Optional[int] = None,
        get_data_size: Optional[Callable[[], int]] = None,
    ):
        self.backing = Path(backing_dir).resolve()
        self.k = k
        self.min_plot_bytes = plot_size_for_k(k)
        self.temp_overhead_bytes = plot_temp_overhead_for_k(k)
        self._plot_created = plot_created_callback
        self._plot_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._max_backing_bytes = max_backing_bytes
        self._get_data_size = get_data_size or (lambda: 0)

    def list_plots(self) -> List[tuple]:
        """Return list of (path, size_bytes) for each .plot file."""
        out = []
        for p in self.backing.iterdir():
            if p.is_file() and p.suffix.lower() == self.PLOT_EXT:
                try:
                    out.append((str(p), p.stat().st_size))
                except OSError:
                    pass
        return out

    def total_plots_size(self) -> int:
        return sum(s for _, s in self.list_plots())

    def delete_smallest_plot(self) -> Optional[int]:
        """Delete the smallest plot file. Returns bytes freed or None."""
        plots = self.list_plots()
        if not plots:
            return None
        plots.sort(key=lambda x: x[1])
        path, size = plots[0]
        try:
            Path(path).unlink()
            return size
        except OSError:
            return None

    def delete_oldest_plot(self) -> Optional[int]:
        """Delete the oldest (by mtime) plot file. Returns bytes freed or None."""
        plots = self.list_plots()
        if not plots:
            return None
        with_times = [(p, Path(p).stat().st_mtime, Path(p).stat().st_size) for p, _ in plots]
        with_times.sort(key=lambda x: x[1])
        path = with_times[0][0]
        size = with_times[0][2]
        try:
            Path(path).unlink()
            return size
        except OSError:
            return None

    def free_space(self) -> int:
        """Return free space on the backing filesystem."""
        stat = os.statvfs(str(self.backing))
        return stat.f_bavail * stat.f_frsize

    def effective_free_space(self) -> int:
        """Free space we are allowed to use (capped by max_backing_bytes if set)."""
        raw = self.free_space()
        if self._max_backing_bytes is None:
            return raw
        used = self.total_plots_size() + self._get_data_size()
        allowed = max(0, self._max_backing_bytes - used)
        return min(raw, allowed)

    def ensure_space(self, needed: int) -> bool:
        """
        Ensure at least `needed` bytes available: delete plots if necessary.
        Respects max_backing_bytes when set (deletes plots so total usage stays under cap).
        Returns True if we have (or made) enough space.
        """
        while True:
            used = self.total_plots_size() + self._get_data_size()
            if self._max_backing_bytes is not None:
                if used + needed > self._max_backing_bytes:
                    freed = self.delete_smallest_plot()
                    if freed is None:
                        return False
                    continue
            if self.free_space() < needed:
                freed = self.delete_smallest_plot()
                if freed is None:
                    return False
                continue
            return True

    def _would_exceed_max(self) -> bool:
        """True if creating one more plot would exceed max_backing_bytes."""
        if self._max_backing_bytes is None:
            return False
        used = self.total_plots_size() + self._get_data_size()
        return (used + self.min_plot_bytes) > self._max_backing_bytes

    def _maybe_start_plotter(self) -> None:
        needed = self.min_plot_bytes + self.temp_overhead_bytes
        if self.free_space() < needed:
            return
        if self._would_exceed_max():
            return
        with self._lock:
            if self._plot_thread is not None and self._plot_thread.is_alive():
                return
            self._plot_thread = threading.Thread(target=self._create_one_plot)
            self._plot_thread.daemon = True
            self._plot_thread.start()

    def _create_one_plot(self) -> None:
        needed = self.min_plot_bytes + self.temp_overhead_bytes
        if self.free_space() < needed:
            return
        if self._would_exceed_max():
            return
        try:
            self._create_plot_impl()
        except Exception:
            pass
        # Clear ourselves so the next schedule can start another plotter thread
        with self._lock:
            self._plot_thread = None
        # Chain: start next plot to keep filling until max or out of space
        self.schedule_create_plot_if_possible()

    def _create_plot_impl(self) -> None:
        try:
            from chiapos import DiskPlotter
        except ImportError:
            return
        tmp_dir = str(self.backing / "_chiafs" / "plot_tmp")
        tmp2_dir = str(self.backing / "_chiafs" / "plot_tmp2")
        final_dir = str(self.backing)
        os.makedirs(tmp_dir, exist_ok=True)
        os.makedirs(tmp2_dir, exist_ok=True)
        filename = f"plot_{uuid.uuid4().hex[:16]}.plot"
        memo = b"chiafs"
        plot_id = os.urandom(32)
        pl = DiskPlotter()
        pl.create_plot_disk(
            tmp_dir,
            tmp2_dir,
            final_dir,
            filename,
            self.k,
            memo,
            plot_id,
            300,
            32,
            8192,
            2,
            False,
        )
        if self._plot_created:
            self._plot_created(str(self.backing / filename))
        # Clean temp files
        for d in (tmp_dir, tmp2_dir):
            if os.path.exists(d):
                try:
                    shutil.rmtree(d, ignore_errors=True)
                except Exception:
                    pass

    def schedule_create_plot_if_possible(self) -> None:
        """Called when the user frees space; may start a background plot."""
        self._maybe_start_plotter()
