import argparse
import binascii

from chiapos import DiskPlotter
from enum import Enum

class Options(Enum):
    TMP_DIR = 1
    TMP_DIR2 = 2
    FINAL_DIR = 3
    FILENAME = 4
    K = 5
    MEMO = 6
    ID = 7
    BUFF = 8
    NUM_BUCKETS = 9
    STRIPE_SIZE = 10
    NUM_THREADS = 11
    NOBITFIELD = 12
    MADMAX_PLOT_COUNT = 13
    MADMAX_NUM_BUCKETS_PHRASE3 = 14
    MADMAX_WAITFORCOPY = 15
    MADMAX_POOLKEY = 16
    MADMAX_FARMERKEY = 17
    MADMAX_TMPTOGGLE = 18

chia_plotter = [
    Options.TMP_DIR,
    Options.TMP_DIR2,
    Options.FINAL_DIR,
    Options.FILENAME,
    Options.K,
    Options.MEMO,
    Options.ID,
    Options.BUFF,
    Options.NUM_BUCKETS,
    Options.STRIPE_SIZE,
    Options.NUM_THREADS,
    Options.NOBITFIELD,
]

madmax_plotter = [
    Options.MADMAX_PLOT_COUNT,
    Options.NUM_THREADS,
    Options.NUM_BUCKETS,
    Options.MADMAX_NUM_BUCKETS_PHRASE3,
    Options.TMP_DIR,
    Options.TMP_DIR2,
    Options.FINAL_DIR,
    Options.MADMAX_WAITFORCOPY,
    Options.MADMAX_POOLKEY,
    Options.MADMAX_FARMERKEY,
    Options.MADMAX_TMPTOGGLE,
]


def build_parser(subparsers, option_list, name, plotter_desc):
    parser = subparsers.add_parser(name, description=plotter_desc)
    for option in option_list:
        if option is Options.K:
            parser.add_argument(
                '-k', '--size', type=int, help='K value.', default=32,
            )
        u_default = 0 if name == 'chiapos' else 256
        if option is Options.NUM_BUCKETS:
            parser.add_argument(
                '-u', '--buckets', type=int, help='Number of buckets.', default=u_default,
            )
        if option is Options.STRIPE_SIZE:
            parser.add_argument(
                '-s', '--stripes', type=int, help='Stripe size.', default=0,
            )
        if option is Options.TMP_DIR:
            parser.add_argument(
                '-t', '--tempdir', type=str, help='Temporary directory 1.', default='.',
            )
        if option is Options.TMP_DIR2:
            parser.add_argument(
                '-2', '--tempdir2', type=str, help='Temporary directory 2.', default='.',
            )
        if option is Options.FINAL_DIR:
            parser.add_argument(
                '-d', '--finaldir', type=str, help='Final directory.', default='.',
            )
        if option is Options.FILENAME:
            parser.add_argument(
                '-f', '--filename', type=str, help='Plot filename.', default='plot.dat',
            )
        if option is Options.BUFF:
            parser.add_argument(
                '-b', '--buffer', type=int, help='Size of the buffer, in MB.', default=0,
            )
        r_default = 0 if name == 'chiapos' else 4
        if option is Options.NUM_THREADS:
            parser.add_argument(
                '-r', '--threads', type=int, help='Num threads.', default=r_default,
            )
        if option is Options.NOBITFIELD:
            parser.add_argument(
                '-e', '--nobitfield', type=bool, help='Disable bitfield.', default=False,
            )
        if option is Options.MEMO:
            parser.add_argument(
                'memo', type=binascii.unhexlify, help='Memo variable.',
            )
        if option is Options.ID:
            parser.add_argument(
                'id', type=binascii.unhexlify, help='Plot id',
            )
        if option is Options.MADMAX_PLOT_COUNT:
            parser.add_argument(
                '-n', '--count', type=int, help='Number of plots to create (default = 1, -1 = infinite)', default=1,
            )
        if option is Options.MADMAX_NUM_BUCKETS_PHRASE3:
            parser.add_argument(
                '-v', '--buckets3', type=int, help='Number of buckets for phase 3+4 (default = 256)', default=256,
            )
        if option is Options.MADMAX_WAITFORCOPY:
            parser.add_argument(
                '-w', '--waitforcopy', type=bool, help='Wait for copy to start next plot', default=True,
            )
        if option is Options.MADMAX_TMPTOGGLE:
            parser.add_argument(
                '-G', '--tmptoggle', help='Alternate tmpdir/tmpdir2 (default = false)', default=False,
            )
        if option is Options.MADMAX_POOLKEY:
            parser.add_argument(
                'poolkey', type=binascii.unhexlify, help='Pool Public Key (48 bytes)',
            )
        if option is Options.MADMAX_FARMERKEY:
            parser.add_argument(
                'farmerkey', type=binascii.unhexlify, help='Farmer Public Key (48 bytes)',
            )

def plot_chia(args):
    try:
        plotter = DiskPlotter()
        plotter.create_plot_disk(
            args.tempdir,
            args.tempdir2,
            args.finaldir,
            args.filename,
            args.size,
            args.memo,
            args.id,
            args.buffer,
            args.buckets,
            args.stripes,
            args.threads,
            args.nobitfield,
        )
    except Exception as e:
        print(f"Exception while plotting: {e}")

def plot_madmax(args):
    print(args)

def main():
    plotters = argparse.ArgumentParser(description='Available plotters.')
    subparsers = plotters.add_subparsers(help="Available plotters", dest="plotter")
    build_parser(subparsers, chia_plotter, "chiapos", "Chiapos Plotter")
    build_parser(subparsers, madmax_plotter, "madmax", "Madmax Plotter")
    args = plotters.parse_args()
    if args.plotter == 'chiapos':
        plot_chia(args)
    if args.plotter == 'madmax':
        plot_madmax(args)

if __name__ == "__main__":
    main()
