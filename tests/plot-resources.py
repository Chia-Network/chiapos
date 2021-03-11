#  pip3 install psutil
import subprocess
import time
import os
import threading
import psutil
from argparse import ArgumentParser
from pathlib import Path


tempdir = "plots/temp"
finaldir = "plots/final"

"""
This script polls time, memory and temporary directory space used while
running ./ProofOfSpace from the C++ binary build of chiapos. It assumes
you are in the chiapos/build directory. If plotting large plots you should
ln -s a temp and final directory into chiapos/build/plots.
It takes one argument to specify the k size to plot.
"""


def create_parser() -> ArgumentParser:
    parser: ArgumentParser = ArgumentParser(
        description="Monitor resources while plotting with ./ProofOfSpace",
        epilog="Try python3 ../test/plot-resources.py from the build directory\n"
        + "Requires a plots/temp and plots/final directory to exist in cwd.",
    )

    parser.add_argument(
        "-k",
        help="Which k size to plot.",
        type=str,
        default="",
    )
    parser.add_argument(
        "-r",
        "--threads",
        help="How many threads to use.",
        type=str,
        default="2",
    )
    parser.add_argument(
        "-e",
        "--no-bitfield",
        action="store_true",
        help="Disable using bitfield sort.",
        default=False,
    )
    return parser


def kill_everything(pid):
    parent = psutil.Process(pid)
    for child in parent.children(recursive=True):
        child.kill()
    parent.kill()


#  Note that this is dangerous when there are subfolders
def get_size(path):
    return sum(p.stat().st_size for p in Path(path).rglob("*"))


bPollSpace = False
pollDisk = 0
pollMem = 0


def pollSpace():
    global pollDisk
    global pollMem

    tempdirpath = os.getcwd() + "/" + tempdir
    print("Temporary directory path is " + tempdirpath)
    used = 0
    while bPollSpace:
        try:
            used = get_size(tempdirpath)
        except FileNotFoundError:
            print("A temp file was deleted while polling the temp directory. Skipping.")
            pass
        if used > pollDisk:
            pollDisk = used

        tot = 0
        parent = psutil.Process(os.getpid())
        for child in parent.children(recursive=True):
            tot = tot + child.memory_info().vms
        if tot > pollMem:
            pollMem = tot

        time.sleep(1)


def run_ProofOfSpace(k_size, threads, disable_bitfield):
    if os.path.isfile("./ProofOfSpace"):
        global bPollSpace
        bPollSpace = True
        plot_out = ""
        out = ""
        if disable_bitfield:
            e = " -e"
        else:
            e = ""
        threading.Thread(target=pollSpace).start()
        start = time.time()
        print(f"Starting ProofOfSpace create -k {k_size}")
        cmd = (
            "exec ./ProofOfSpace create -k "
            + k_size
            + " -r "
            + threads
            + " -b 4608"
            + " -u 64"
            + e
            + " -t "
            + tempdir
            + " -2 "
            + finaldir
            + " -d "
            + finaldir
        )
        print("cmd is ", cmd, "\n")
        try:
            pro = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
            )
            # out_script, err = pro.communicate(timeout=43200)  # 12 hour time out
            while pro.poll() is None:
                std_output = pro.stdout.readline().rstrip()
                print(std_output.decode(), flush=True)
        except subprocess.TimeoutExpired:
            kill_everything(pro.pid)
            plot_out += (
                "Timeout of 12 hours for running expired! Halting the execution" + "\n"
            )
            return plot_out
        except subprocess.CalledProcessError as e:
            plot_out += "Errors running: "
            plot_out += e.output.decode()
            return plot_out
        except Exception as e:
            plot_out += "Error running: "
            plot_out += str(e)
            return plot_out
        print(f"Finished k={k_size}", flush=True)
        end = time.time()
        bPollSpace = False
        plot_out += "\nTotal time for ProofOfSpace: " + str(end - start) + " seconds\n"
        gigabytes = pollDisk / 1024 ** 3
        plot_out += "Total temp space used: " + str(gigabytes) + " GiB\n"
        megabytes = pollMem / 1024 ** 2
        plot_out += "Memory used: " + str(megabytes) + " MiB\n"
        try:
            plot_out += (
                "Total plot size: "
                + str(os.path.getsize(finaldir + "/plot.dat") / 1024 ** 3)
                + " GiB\n\n"
            )
        except Exception as e:
            plot_out += "Total plot size: error" + str(e)
        start = time.time()
        print("Checking plot\n", flush=True)
        cmd = "exec ./ProofOfSpace check -f " + finaldir + "/plot.dat 100"
        try:
            pro = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
            )
            out_script, err = pro.communicate(timeout=600)
        except subprocess.TimeoutExpired:
            kill_everything(pro.pid)
            out += (
                "Timeout of 10 minutes for running expired! Halting the execution"
                + "\n"
            )
            return out
        except subprocess.CalledProcessError as e:
            out += "Errors running: "
            out += e.output.decode()
            return out
        except Exception as e:
            out += "Error running: "
            out += str(e)
            return out
        print("Finished checking plot", flush=True)
        end = time.time()
        out += "Run Output: " + out_script.decode() + "\n"
        if len(err) > 1:
            out += "Run Errors: " + err.decode() + "\n"
        out += (
            "Total time for check with 100 proofs: " + str(end - start) + " seconds\n"
        )
        out += plot_out
    else:
        out += "ProofOfSpace binary not found!\n"
    return out


def Main():
    parser = create_parser()
    args = parser.parse_args()
    if args.k and 22 <= int(args.k) <= 50:
        final_output = run_ProofOfSpace(args.k, args.threads, args.no_bitfield)
        print(final_output, flush=True)
    else:
        print("Please specify a k size between 22 and 50")


if __name__ == "__main__":
    Main()
