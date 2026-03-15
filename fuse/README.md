# chiafs — FUSE filesystem backed by Chia plots

A Linux FUSE filesystem that uses a single backing directory for both normal file data and Chia proof-of-space plot files (`.plot`). Space is shared:

- **When the filesystem needs more space** (e.g. writing new or larger files), chiafs deletes Chia plots from the backing directory until there is enough free space.
- **When you delete files** from the mounted filesystem, chiafs may create new Chia plots in the newly available space (in the background).

## Requirements

- **Linux** with FUSE 3 (libfuse3)
- **Python 3.8+**
- **chiapos** (from this repository) with Python bindings
- **pyfuse3** and **trio**

## Install

From the chiapos repository root:

```bash
# Build chiapos with Python bindings
mkdir -p build && cd build
cmake ..
cmake --build . -- -j$(nproc)

# Install chiapos Python module (use the built wheel or install in development mode)
cd ..
pip install .

# Install FUSE dependencies (Debian/Ubuntu)
sudo apt-get install libfuse3-dev

# Install Python deps for chiafs
pip install pyfuse3 trio
```

## Usage

1. Create a backing directory that will hold both the filesystem data and Chia plots:

   ```bash
   mkdir -p /path/to/backing
   ```

2. Mount the filesystem (from the chiapos repo root):

   ```bash
   python fuse/run_chiafs.py /path/to/backing /path/to/mountpoint
   ```

   Or from the `fuse` directory:

   ```bash
   cd fuse && python run_chiafs.py /path/to/backing /path/to/mountpoint
   ```

3. Use the mount point like a normal directory. File data is stored under `_chiafs/` in the backing directory. Any `.plot` files in the backing directory are treated as expendable when more space is needed.

4. Unmount when done:

   ```bash
   fusermount3 -u /path/to/mountpoint
   ```

### Options

- `-k N`, `--plot-size N` — Use Chia plot size `k=N` for newly created plots (default: 20). Must be between 18 and 50. Smaller k means smaller plots and faster creation but more overhead.
- `--debug` — Enable debug logging.
- `--debug-fuse` — Enable FUSE-level debug output.
- `--max-size SIZE` — Cap the total size of the backing store (plots + filesystem data). Use a size such as `100G` or `1T` (suffixes `K`, `M`, `G`, `T` are supported). The filesystem will not create new plots beyond this limit.

### Max backing store

```bash
# Mount with at most 500 GB used for plots + file data
python3 -m fuse.chiafs.main --max-size 500G /path/to/backing_dir /path/to/mountpoint

# From the fuse directory:
cd fuse && python3 -m run_chiafs --max-size 1T /path/to/backing_dir /path/to/mountpoint
```

## How it works

- **Backing layout**: The backing directory contains:
  - `_chiafs/` — SQLite metadata and file data for the FUSE filesystem. Do not remove manually.
  - `*.plot` — Chia plot files. These may be deleted automatically when the filesystem needs space.

- **Freeing space**: On write (or truncate) if the backing filesystem has insufficient free space, chiafs deletes the **smallest** plot first, then retries until there is enough space or no plots remain.

- **Creating plots**: After a file or directory is removed, chiafs checks whether there is enough free space to create a new plot (final size + temp overhead for the chosen `k`). If so, it starts a **background** plot creation job. New plots are named `plot_<uuid>.plot` and are valid Chia plots (with a random plot id and memo `b"chiafs"`).

## Notes

- Plot creation is CPU- and I/O-intensive and runs in a background thread. Use a small `k` (e.g. 20) for testing to keep plot size and creation time manageable.
- The backing directory should be on a filesystem with enough space for both FUSE data and plots (and for plot temp files during creation).
- This tool is for experimentation and integration with Chia plotting; it is not an official Chia Network product.
