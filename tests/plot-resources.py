#  pip3 install psutil
import sys
import subprocess
import time
import os
import threading
import shutil
import psutil


tempdir = "plots/temp"
finaldir = "plots/final"

"""
This script polls time, memory and temporary directory space used while
running ./ProofOfSpace from the C++ binary build of chiapos. It assumes
you are in the chiapos/build directory. If plotting large plots you should
ln -s a temp and final directory into chiapos/build/plots.
It takes one argument to specify the k size to plot.
"""

def kill_everything(pid):
    parent = psutil.Process(pid)
    for child in parent.children(recursive=True):
        child.kill()
    parent.kill()


bPollSpace = False
pollDisk = 0
pollMem = 0


def pollSpace():
    global pollDisk
    global pollMem

    tempdirpath = os.getcwd() + "/" + tempdir
    print("Temporary directory path is " + tempdirpath)
    while bPollSpace:
        total, used, free = shutil.disk_usage(tempdirpath)
        if used > pollDisk:
            pollDisk = used

        tot = 0
        parent = psutil.Process(os.getpid())
        for child in parent.children(recursive=True):
            tot = tot + child.memory_info().vms
        if tot > pollMem:
            pollMem = tot

        time.sleep(1)


def run_ProofOfSpace(k_size):
    if os.path.isfile("./ProofOfSpace"):
        global bPollSpace
        bPollSpace = True
        plot_out = ""
        out = ""
        threading.Thread(target=pollSpace).start()
        start = time.time()
        print(f"Starting ProofOfSpace create -k {k_size}\n")
        cmd = (
            "exec ./ProofOfSpace create -k "
            + k_size
            + " -r 2"
#            + " -b 4608"
            + " -u 64"
            + " -t "
            + tempdir
            + " -2 "
            + finaldir
            + " -d "
            + finaldir
        )
        print("command is " + cmd)
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
    if 22 <= int(sys.argv[1]) <= 50:
        final_output = run_ProofOfSpace(sys.argv[1])
        print(final_output, flush=True)
    else:
        print("Please specify a k size between 22 and 50")


if __name__ == "__main__":
    Main()
