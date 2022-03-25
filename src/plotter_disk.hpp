// Copyright 2018 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_CPP_PLOTTER_DISK_HPP_
#define SRC_CPP_PLOTTER_DISK_HPP_

#ifndef _WIN32
#include <semaphore.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <math.h>
#include <stdio.h>
#include <sys/stat.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <memory>

#include "chia_filesystem.hpp"

#include "calculate_bucket.hpp"
#include "encoding.hpp"
#include "exceptions.hpp"
#include "phases.hpp"
#include "phase1.hpp"
#include "phase2.hpp"
#include "b17phase2.hpp"
#include "phase3.hpp"
#include "b17phase3.hpp"
#include "phase4.hpp"
#include "b17phase4.hpp"
#include "pos_constants.hpp"
#include "sort_manager.hpp"
#include "util.hpp"

#define B17PHASE23

class DiskPlotter {
public:
    // This method creates a plot on disk with the filename. Many temporary files
    // (filename + ".table1.tmp", filename + ".p2.t3.sort_bucket_4.tmp", etc.) are created
    // and their total size will be larger than the final plot file. Temp files are deleted at the
    // end of the process.
    void CreatePlotDisk(
        std::string tmp_dirname,
        std::string tmp2_dirname,
        std::string final_dirname,
        std::string filename,
        uint8_t k,
        const uint8_t* memo,
        uint32_t memo_len,
        const uint8_t* id,
        uint32_t id_len,
        uint32_t buf_megabytes_input = 0,
        uint32_t num_buckets_input = 0,
        uint64_t stripe_size_input = 0,
        uint8_t num_threads_input = 0,
        uint8_t phases_flags = ENABLE_BITFIELD)
    {
        // Increases the open file limit, we will open a lot of files.
#ifndef _WIN32
        struct rlimit the_limit = {600, 600};
        if (-1 == setrlimit(RLIMIT_NOFILE, &the_limit)) {
            std::cout << "setrlimit failed" << std::endl;
        }
#endif
        if (k < kMinPlotSize || k > kMaxPlotSize) {
            throw InvalidValueException("Plot size k= " + std::to_string(k) + " is invalid");
        }

        uint32_t stripe_size, buf_megabytes, num_buckets;
        uint8_t num_threads;
        if (stripe_size_input != 0) {
            stripe_size = stripe_size_input;
        } else {
            stripe_size = 65536;
        }
        if (num_threads_input != 0) {
            num_threads = num_threads_input;
        } else {
            num_threads = 2;
        }
        if (buf_megabytes_input != 0) {
            buf_megabytes = buf_megabytes_input;
        } else {
            buf_megabytes = 4608;
        }

        if (buf_megabytes < 10) {
            throw InsufficientMemoryException("Please provide at least 10MiB of ram");
        }

        // Subtract some ram to account for dynamic allocation through the code
        uint64_t thread_memory = num_threads * (2 * (stripe_size + 5000)) *
                                 EntrySizes::GetMaxEntrySize(k, 4, true) / (1024 * 1024);
        uint64_t sub_mbytes = (5 + (int)std::min(buf_megabytes * 0.05, (double)50) + thread_memory);
        if (sub_mbytes > buf_megabytes) {
            throw InsufficientMemoryException(
                "Please provide more memory. At least " + std::to_string(sub_mbytes));
        }
        uint64_t memory_size = ((uint64_t)(buf_megabytes - sub_mbytes)) * 1024 * 1024;
        double max_table_size = 0;
        for (size_t i = 1; i <= 7; i++) {
            double memory_i = 1.3 * ((uint64_t)1 << k) * EntrySizes::GetMaxEntrySize(k, i, true);
            if (memory_i > max_table_size)
                max_table_size = memory_i;
        }
        if (num_buckets_input != 0) {
            num_buckets = Util::RoundPow2(num_buckets_input);
        } else {
            num_buckets = 2 * Util::RoundPow2(ceil(
                                  ((double)max_table_size) / (memory_size * kMemSortProportion)));
        }

        if (num_buckets < kMinBuckets) {
            if (num_buckets_input != 0) {
                throw InvalidValueException("Minimum buckets is " + std::to_string(kMinBuckets));
            }
            num_buckets = kMinBuckets;
        } else if (num_buckets > kMaxBuckets) {
            if (num_buckets_input != 0) {
                throw InvalidValueException("Maximum buckets is " + std::to_string(kMaxBuckets));
            }
            double required_mem =
                (max_table_size / kMaxBuckets) / kMemSortProportion / (1024 * 1024) + sub_mbytes;
            throw InsufficientMemoryException(
                "Do not have enough memory. Need " + std::to_string(required_mem) + " MiB");
        }
        uint32_t log_num_buckets = log2(num_buckets);
        assert(log2(num_buckets) == ceil(log2(num_buckets)));

        if (max_table_size / num_buckets < stripe_size * 30) {
            throw InvalidValueException("Stripe size too large");
        }

#if defined(_WIN32) || defined(__x86_64__)
        if (phases_flags & ENABLE_BITFIELD && !Util::HavePopcnt()) {
            throw InvalidValueException("Bitfield plotting not supported by CPU");
        }
#endif /* defined(_WIN32) || defined(__x86_64__) */

        std::cout << std::endl
                  << "Starting plotting progress into temporary dirs: " << tmp_dirname << " and "
                  << tmp2_dirname << std::endl;
        std::cout << "ID: " << Util::HexStr(id, id_len) << std::endl;
        std::cout << "Plot size is: " << static_cast<int>(k) << std::endl;
        std::cout << "Buffer size is: " << buf_megabytes << "MiB" << std::endl;
        std::cout << "Using " << num_buckets << " buckets" << std::endl;
        std::cout << "Final Directory is: " << final_dirname << std::endl;
        std::cout << "Using " << (int)num_threads << " threads of stripe size " << stripe_size
                  << std::endl;
        std::cout << "Process ID is: " << ::getpid() << std::endl;

        // Cross platform way to concatenate paths, gulrak library.
        std::vector<fs::path> tmp_1_filenames = std::vector<fs::path>();

        // The table0 file will be used for sort on disk spare. tables 1-7 are stored in their own
        // file.
        tmp_1_filenames.push_back(fs::path(tmp_dirname) / fs::path(filename + ".sort.tmp"));
        for (size_t i = 1; i <= 7; i++) {
            tmp_1_filenames.push_back(
                fs::path(tmp_dirname) / fs::path(filename + ".table" + std::to_string(i) + ".tmp"));
        }
        fs::path tmp_2_filename = fs::path(tmp2_dirname) / fs::path(filename + ".2.tmp");
        fs::path final_2_filename = fs::path(final_dirname) / fs::path(filename + ".2.tmp");
        fs::path final_filename = fs::path(final_dirname) / fs::path(filename);

        // Check if the paths exist
        if (!fs::exists(tmp_dirname)) {
            throw InvalidValueException("Temp directory " + tmp_dirname + " does not exist");
        }

        if (!fs::exists(tmp2_dirname)) {
            throw InvalidValueException("Temp2 directory " + tmp2_dirname + " does not exist");
        }

        if (!fs::exists(final_dirname)) {
            throw InvalidValueException("Final directory " + final_dirname + " does not exist");
        }
        for (fs::path& p : tmp_1_filenames) {
            fs::remove(p);
        }
        fs::remove(tmp_2_filename);
        fs::remove(final_filename);

        {
            // Scope for FileDisk
            std::vector<FileDisk> tmp_1_disks;
            for (auto const& fname : tmp_1_filenames)
                tmp_1_disks.emplace_back(fname);

            FileDisk tmp2_disk(tmp_2_filename);

            assert(id_len == kIdLen);

            std::cout << std::endl
                      << "Starting phase 1/4: Forward Propagation into tmp files... "
                      << Timer::GetNow();

            Timer p1;
            Timer all_phases;
            std::vector<uint64_t> table_sizes = RunPhase1(
                tmp_1_disks,
                k,
                id,
                tmp_dirname,
                filename,
                memory_size,
                num_buckets,
                log_num_buckets,
                stripe_size,
                num_threads,
                phases_flags);
            p1.PrintElapsed("Time for phase 1 =");

            uint64_t finalsize=0;

            if((phases_flags & ENABLE_BITFIELD) == 0)
            {
                // Memory to be used for sorting and buffers
                std::unique_ptr<uint8_t[]> memory(new uint8_t[memory_size + 7]);

                std::cout << std::endl
                      << "Starting phase 2/4: Backpropagation without bitfield into tmp files... "
                      << Timer::GetNow();

                Timer p2;
                std::vector<uint64_t> backprop_table_sizes = b17RunPhase2(
                    memory.get(),
                    tmp_1_disks,
                    table_sizes,
                    k,
                    id,
                    tmp_dirname,
                    filename,
                    memory_size,
                    num_buckets,
                    log_num_buckets,
                    phases_flags);
                p2.PrintElapsed("Time for phase 2 =");

                // Now we open a new file, where the final contents of the plot will be stored.
                uint32_t header_size = WriteHeader(tmp2_disk, k, id, memo, memo_len);

                std::cout << std::endl
                      << "Starting phase 3/4: Compression without bitfield from tmp files into " << tmp_2_filename
                      << " ... " << Timer::GetNow();
                Timer p3;
                b17Phase3Results res = b17RunPhase3(
                    memory.get(),
                    k,
                    tmp2_disk,
                    tmp_1_disks,
                    backprop_table_sizes,
                    id,
                    tmp_dirname,
                    filename,
                    header_size,
                    memory_size,
                    num_buckets,
                    log_num_buckets,
                    phases_flags);
                p3.PrintElapsed("Time for phase 3 =");

                std::cout << std::endl
                      << "Starting phase 4/4: Write Checkpoint tables into " << tmp_2_filename
                      << " ... " << Timer::GetNow();
                Timer p4;
                b17RunPhase4(k, k + 1, tmp2_disk, res, phases_flags, 16);
                p4.PrintElapsed("Time for phase 4 =");
                finalsize = res.final_table_begin_pointers[11];
            }
            else {
                std::cout << std::endl
                      << "Starting phase 2/4: Backpropagation into tmp files... "
                      << Timer::GetNow();

                Timer p2;
                Phase2Results res2 = RunPhase2(
                    tmp_1_disks,
                    table_sizes,
                    k,
                    id,
                    tmp_dirname,
                    filename,
                    memory_size,
                    num_buckets,
                    log_num_buckets,
                    phases_flags);
                p2.PrintElapsed("Time for phase 2 =");

                // Now we open a new file, where the final contents of the plot will be stored.
                uint32_t header_size = WriteHeader(tmp2_disk, k, id, memo, memo_len);

                std::cout << std::endl
                      << "Starting phase 3/4: Compression from tmp files into " << tmp_2_filename
                      << " ... " << Timer::GetNow();
                Timer p3;
                Phase3Results res = RunPhase3(
                    k,
                    tmp2_disk,
                    std::move(res2),
                    id,
                    tmp_dirname,
                    filename,
                    header_size,
                    memory_size,
                    num_buckets,
                    log_num_buckets,
                    phases_flags);
                p3.PrintElapsed("Time for phase 3 =");

                std::cout << std::endl
                      << "Starting phase 4/4: Write Checkpoint tables into " << tmp_2_filename
                      << " ... " << Timer::GetNow();
                Timer p4;
                RunPhase4(k, k + 1, tmp2_disk, res, phases_flags, 16);
                p4.PrintElapsed("Time for phase 4 =");
                finalsize = res.final_table_begin_pointers[11];
            }

            // The total number of bytes used for sort is saved to table_sizes[0]. All other
            // elements in table_sizes represent the total number of entries written by the end of
            // phase 1 (which should be the highest total working space time). Note that the max
            // sort on disk space does not happen at the exact same time as max table sizes, so this
            // estimate is conservative (high).
            uint64_t total_working_space = table_sizes[0];
            for (size_t i = 1; i <= 7; i++) {
                total_working_space += table_sizes[i] * EntrySizes::GetMaxEntrySize(k, i, false);
            }
            std::cout << "Approximate working space used (without final file): "
                      << static_cast<double>(total_working_space) / (1024 * 1024 * 1024) << " GiB"
                      << std::endl;

            std::cout << "Final File size: "
                      << static_cast<double>(finalsize) /
                             (1024 * 1024 * 1024)
                      << " GiB" << std::endl;
            all_phases.PrintElapsed("Total time =");
        }

        for (fs::path p : tmp_1_filenames) {
            fs::remove(p);
        }

        bool bCopied = false;
        bool bRenamed = false;
        Timer copy;
        do {
            std::error_code ec;
            struct stat tmp2_stat, final_stat;
            int rc;
            rc = ::stat(reinterpret_cast<const char *>(tmp_2_filename.c_str()), &tmp2_stat);
            if (rc == 0)
                rc = ::stat(reinterpret_cast<const char *>(final_filename.parent_path().c_str()), &final_stat);
            if ((rc == 0 && tmp2_stat.st_dev == final_stat.st_dev) ||
                tmp_2_filename.parent_path() == final_filename.parent_path()) {
                fs::rename(tmp_2_filename, final_filename, ec);
                if (ec.value() != 0) {
                    std::cout << "Could not rename " << tmp_2_filename << " to " << final_filename
                              << ". Error " << ec.message() << ". Retrying in five minutes."
                              << std::endl;
                } else {
                    bRenamed = true;
                    std::cout << "Renamed final file from " << tmp_2_filename << " to "
                              << final_filename << std::endl;
                }
            } else {
                if (!bCopied) {
                    fs::copy(
                        tmp_2_filename, final_2_filename, fs::copy_options::overwrite_existing, ec);
                    if (ec.value() != 0) {
                        std::cout << "Could not copy " << tmp_2_filename << " to "
                                  << final_2_filename << ". Error " << ec.message()
                                  << ". Retrying in five minutes." << std::endl;
                    } else {
                        std::cout << "Copied final file from " << tmp_2_filename << " to "
                                  << final_2_filename << std::endl;
                        copy.PrintElapsed("Copy time =");
                        bCopied = true;

                        bool removed_2 = fs::remove(tmp_2_filename);
                        std::cout << "Removed temp2 file " << tmp_2_filename << "? " << removed_2
                                  << std::endl;
                    }
                }
                if (bCopied && (!bRenamed)) {
                    fs::rename(final_2_filename, final_filename, ec);
                    if (ec.value() != 0) {
                        std::cout << "Could not rename " << tmp_2_filename << " to "
                                  << final_filename << ". Error " << ec.message()
                                  << ". Retrying in five minutes." << std::endl;
                    } else {
                        std::cout << "Renamed final file from " << final_2_filename << " to "
                                  << final_filename << std::endl;
                        bRenamed = true;
                    }
                }
            }

            if (!bRenamed) {
#ifdef _WIN32
                Sleep(5 * 60000);
#else
                sleep(5 * 60);
#endif
            }
        } while (!bRenamed);
    }

private:
    // Writes the plot file header to a file
    uint32_t WriteHeader(
        FileDisk& plot_Disk,
        uint8_t k,
        const uint8_t* id,
        const uint8_t* memo,
        uint32_t memo_len)
    {
        // 19 bytes  - "Proof of Space Plot" (utf-8)
        // 32 bytes  - unique plot id
        // 1 byte    - k
        // 2 bytes   - format description length
        // x bytes   - format description
        // 2 bytes   - memo length
        // x bytes   - memo

        std::string header_text = "Proof of Space Plot";
        uint64_t write_pos = 0;
        plot_Disk.Write(write_pos, (uint8_t*)header_text.data(), header_text.size());
        write_pos += header_text.size();
        plot_Disk.Write(write_pos, (id), kIdLen);
        write_pos += kIdLen;

        uint8_t k_buffer[1];
        k_buffer[0] = k;
        plot_Disk.Write(write_pos, (k_buffer), 1);
        write_pos += 1;

        uint8_t size_buffer[2];
        Util::IntToTwoBytes(size_buffer, kFormatDescription.size());
        plot_Disk.Write(write_pos, (size_buffer), 2);
        write_pos += 2;
        plot_Disk.Write(write_pos, (uint8_t*)kFormatDescription.data(), kFormatDescription.size());
        write_pos += kFormatDescription.size();

        Util::IntToTwoBytes(size_buffer, memo_len);
        plot_Disk.Write(write_pos, (size_buffer), 2);
        write_pos += 2;
        plot_Disk.Write(write_pos, (memo), memo_len);
        write_pos += memo_len;

        uint8_t pointers[10 * 8];
        memset(pointers, 0, 10 * 8);
        plot_Disk.Write(write_pos, (pointers), 10 * 8);
        write_pos += 10 * 8;

        uint32_t bytes_written =
            header_text.size() + kIdLen + 1 + 2 + kFormatDescription.size() + 2 + memo_len + 10 * 8;
        std::cout << "Wrote: " << bytes_written << std::endl;
        return bytes_written;
    }
};

#endif  // SRC_CPP_PLOTTER_DISK_HPP_
