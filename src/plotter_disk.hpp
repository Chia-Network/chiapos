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
#include <unistd.h>
#endif
#include <stdio.h>
#include <math.h>

#include <iostream>
#include <fstream>
#include <map>
#include <algorithm>
#include <vector>
#include <string>
#include <utility>
#include <unordered_set>
#include <unordered_map>

// Gulrak filesystem brings in Windows headers that cause some issues with std
#define _HAS_STD_BYTE 0
#define NOMINMAX

#include "../lib/include/filesystem.hh"
namespace fs = ghc::filesystem;
#include "util.hpp"
#include "encoding.hpp"
#include "calculate_bucket.hpp"
#include "sort_on_disk.hpp"
#include "pos_constants.hpp"

// Constants that are only relevant for the plotting process.
// Other constants can be found in pos_constants.hpp

// Distance between matching entries is stored in the offset
const uint32_t kOffsetSize = 10;

// Number of buckets to use for SortOnDisk.
const uint32_t kNumSortBuckets = 16;
const uint32_t kLogNumSortBuckets = 4;

// During backprop and compress, the write pointer is ahead of the read pointer
// Note that the large the offset, the higher these values must be
const uint32_t kReadMinusWrite = 1U << kOffsetSize;
const uint32_t kCachedPositionsSize = kReadMinusWrite * 4;

// Max matches a single entry can have, used for hardcoded memory allocation
const uint32_t kMaxMatchesSingleEntry = 30;

// Results of phase 3. These are passed into Phase 4, so the checkpoint tables
// can be properly built.
struct Phase3Results {
    // Pointers to each table start byet in the final file
    vector<uint64_t> final_table_begin_pointers;
    // Number of entries written for f7
    uint64_t final_entries_written;
    uint32_t right_entry_size_bits;

    uint32_t header_size;
};

const Bits empty_bits;

class DiskPlotter {
 public:
    // This method creates a plot on disk with the filename. A temporary file, "plotting" + filename,
    // is created and will be larger than the final plot file. This file is deleted at the end of
    // the process.
    void CreatePlotDisk(std::string tmp_dirname, std::string tmp2_dirname, std::string final_dirname, std::string filename,
                        uint8_t k, const uint8_t* memo,
                        uint32_t memo_len, const uint8_t* id, uint32_t id_len, uint32_t buffmegabytes = 2*1024) {
        if (k < kMinPlotSize || k > kMaxPlotSize) {
            std::cout << "Plot size k=" << std::to_string(k) << " is invalid" << std::endl;
            return;
        }

        memorySize=((uint64_t)buffmegabytes)*1024*1024;

        std::cout << std::endl << "Starting plotting progress into temporary dirs: " << tmp_dirname << " and " << tmp2_dirname << std::endl;
        std::cout << "Memo: " << Util::HexStr(memo, memo_len) << std::endl;
        std::cout << "ID: " << Util::HexStr(id, id_len) << std::endl;
        std::cout << "Plot size is: " << static_cast<int>(k) << std::endl;
        std::cout << "Buffer size is: " << memorySize << std::endl;

        // Cross platform way to concatenate paths, gulrak library.
        std::vector<fs::path> tmp_1_filenames = std::vector<fs::path>();

        // The table0 file will be used for sort on disk spare. tables 1-7 are stored in their own file.
        tmp_1_filenames.push_back(fs::path(tmp_dirname) / fs::path(filename + ".sort.tmp"));
        for (size_t i=1; i<=7; i++) {
            tmp_1_filenames.push_back(fs::path(tmp_dirname) / fs::path(filename + ".table" + to_string(i) + ".tmp"));
        }
        fs::path tmp_2_filename = fs::path(tmp2_dirname) / fs::path(filename + ".2.tmp");
        fs::path final_2_filename = fs::path(final_dirname) / fs::path(filename + ".2.tmp");
        fs::path final_filename = fs::path(final_dirname) / fs::path(filename);

        // Check if the paths exist
        if (!fs::exists(tmp_dirname)) {
            std::cout << "Temp directory " << tmp_dirname << " does not exist" << std::endl;
            return;
        }

        if (!fs::exists(tmp2_dirname)) {
            std::cout << "Temp2 directory " << tmp2_dirname << " does not exist" << std::endl;
            return;
        }

        if (!fs::exists(final_dirname)) {
            std::cout << "Final directory " << final_dirname << " does not exist" << std::endl;
            return;
        }
        for (fs::path& p : tmp_1_filenames) {
            fs::remove(p);
        }
        fs::remove(tmp_2_filename);
        fs::remove(final_filename);

        std::ios_base::sync_with_stdio(false);
        std::ostream *prevstr = std::cin.tie(NULL);

        {
            // Scope for FileDisk
            std::vector<FileDisk> tmp_1_disks;
            for (int i = 0; i <= 7; i++) {
                tmp_1_disks.push_back(FileDisk(tmp_1_filenames[i]));
            }

            FileDisk tmp2_disk(tmp_2_filename);
            if(!tmp2_disk.isOpen()) {
                std::cout << "Could not open " << tmp_2_filename << std::endl;
                return;
            }

            // These variables are used in the WriteParkToFile method. They are preallocatted here
            // to save time.
            parkToFileBytesSize = CalculateLinePointSize(k)+CalculateStubsSize(k)+2+CalculateMaxDeltasSize(k, 1);
            parkToFileBytes = new uint8_t[parkToFileBytesSize];

            assert(id_len == kIdLen);

            // Memory to be used for sorting and buffers
            uint8_t* memory = new uint8_t[memorySize]();

            std::cout << std::endl << "Starting phase 1/4: Forward Propagation into tmp files... " << Timer::GetNow();

            Timer p1;
            Timer all_phases;
            std::vector<uint64_t> table_sizes = WritePlotFile(memory, tmp_1_disks, k, id, memo, memo_len);
            p1.PrintElapsed("Time for phase 1 =");

            std::cout << std::endl << "Starting phase 2/4: Backpropagation into tmp files... " << Timer::GetNow();

            Timer p2;
            std::vector<uint64_t> backprop_table_sizes = Backpropagate(memory, tmp_1_disks, table_sizes, k, id, memo, memo_len);
            p2.PrintElapsed("Time for phase 2 =");

            std::cout << std::endl << "Starting phase 3/4: Compression from tmp files into " << tmp_2_filename << " ... " << Timer::GetNow();
            Timer p3;
            Phase3Results res = CompressTables(memory, k, tmp2_disk, tmp_1_disks, backprop_table_sizes, id, memo, memo_len);
            p3.PrintElapsed("Time for phase 3 =");

            std::cout << std::endl << "Starting phase 4/4: Write Checkpoint tables into " << tmp_2_filename << " ... " << Timer::GetNow();
            Timer p4;
            WriteCTables(k, k + 1, tmp2_disk, tmp_1_disks, res);
            p4.PrintElapsed("Time for phase 4 =");

            // The total number of bytes used for sort is saved to table_sizes[0]. All other elements in table_sizes
            // represent the total number of entries written by the end of phase 1 (which should be the highest total
            // working space time). Note that the max sort on disk space does not happen at the exact same time as
            // max table sizes, so this estimate is conservative (high).
            uint64_t total_working_space = table_sizes[0];
            for (size_t i=1; i <=7; i++) {
                total_working_space += table_sizes[i] * GetMaxEntrySize(k, i, false);
            }
            std::cout << "Approximate working space used (without final file): " << static_cast<double>(total_working_space) / (1024*1024*1024) << " GiB" << std::endl;

            std::cout << "Final File size: " <<
                     static_cast<double>(res.final_table_begin_pointers[11])/(1024*1024*1024) << " GiB" << std::endl;
            all_phases.PrintElapsed("Total time =");

            delete[] memory;
            delete[] parkToFileBytes;
        }

        std::cin.tie (prevstr);
        std::ios_base::sync_with_stdio(true);

        for (fs::path p : tmp_1_filenames) {
            fs::remove(p);
        }

        bool bCopied=false;
        bool bRenamed=false;
        do {
            std::error_code ec;
            if(tmp_2_filename.parent_path() == final_filename.parent_path()) {
                fs::rename(tmp_2_filename, final_filename, ec);
                if(ec.value()!=0) {
                    std::cout << "Could not rename " << tmp_2_filename << " to " << final_filename << ". Error " << ec.message() << ". Retrying in five minutes." << std::endl;
                }
                else {
                    bRenamed=true;
                    std::cout << "Renamed final file from " << tmp_2_filename << " to " << final_filename << std::endl;
                }
            }
            else {
                if(!bCopied) {
                    fs::copy(tmp_2_filename, final_2_filename, fs::copy_options::overwrite_existing, ec);
                    if(ec.value()!=0) {
                        std::cout << "Could not copy " << tmp_2_filename << " to " << final_2_filename << ". Error " << ec.message() << ". Retrying in five minutes." << std::endl;
                    }
                    else {
                        std::cout << "Copied final file from " << tmp_2_filename << " to " << final_2_filename << std::endl;
                        bCopied=true;

                        bool removed_2 = fs::remove(tmp_2_filename);
                        std::cout << "Removed temp2 file " << tmp_2_filename << "? " << removed_2 << std::endl;
                    }
                }
                if(bCopied && (!bRenamed)) {
                    fs::rename(final_2_filename, final_filename, ec);
                    if(ec.value()!=0) {
                        std::cout << "Could not rename " << tmp_2_filename << " to " << final_filename << ". Error " << ec.message() << ". Retrying in five minutes." << std::endl;
                    }
                    else {
                        std::cout << "Renamed final file from " << final_2_filename << " to " << final_filename << std::endl;
                        bRenamed=true;
                    }
                }
            }

            if(!bRenamed) {
#ifdef WIN32
                Sleep(5 * 60000);
#else
                sleep(5 * 60);
#endif
            }
        } while (!bRenamed);
    }

    static uint32_t GetMaxEntrySize(uint8_t k, uint8_t table_index, bool phase_1_size) {
        // This represents the largest entry size that each table will have, throughout the
        // entire plotting process. This is useful because it allows us to rewrite tables
        // on top of themselves without running out of space.
        switch (table_index) {
            case 1:
               // Represents f1, x
               if (phase_1_size) {
                    return Util::ByteAlign(k + kExtraBits + k) / 8;
               } else {
                    // After computing matches, table 1 is rewritten without the f1, which
                    // is useless after phase1.
                    return Util::ByteAlign(k) / 8;
               }
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
                if (phase_1_size)
                    // If we are in phase 1, use the max size, with metadata.
                    // Represents f, pos, offset, and metadata
                    return Util::ByteAlign(k + kExtraBits + (k) + kOffsetSize +
                                           k * kVectorLens[table_index + 1]) / 8;
                else
                    // If we are past phase 1, we can use a smaller size, the smaller between
                    // phases 2 and 3. Represents either:
                    //    a:  sort_key, pos, offset        or
                    //    b:  line_point, sort_key
                    return Util::ByteAlign(max(static_cast<uint32_t>(k + 1 + (k) + kOffsetSize),
                                               static_cast<uint32_t>(2 * k + k+1))) / 8;
            case 7:
            default:
                // Represents line_point, f7
                return Util::ByteAlign(3 * k) / 8;
        }
    }

    // Calculates the size of one C3 park. This will store bits for each f7 between
    // two C1 checkpoints, depending on how many times that f7 is present. For low
    // values of k, we need extra space to account for the additional variability.
    static uint32_t CalculateC3Size(uint8_t k) {
        if (k < 20) {
            return Util::ByteAlign(8 * kCheckpoint1Interval) / 8;
        } else {
            // TODO(alex): tighten this bound, based on formula
            return Util::ByteAlign(kC3BitsPerEntry * kCheckpoint1Interval) / 8;
        }
    }

    static uint32_t CalculateLinePointSize(uint8_t k) {
        return Util::ByteAlign(2*k) / 8;
    }

    // This is the full size of the deltas section in a park. However, it will not be fully filled
    static uint32_t CalculateMaxDeltasSize(uint8_t k, uint8_t table_index) {
        if (table_index == 1) {
            return Util::ByteAlign((kEntriesPerPark - 1) * kMaxAverageDeltaTable1) / 8;
        }
        return Util::ByteAlign((kEntriesPerPark - 1) * kMaxAverageDelta) / 8;
    }

    static uint32_t CalculateStubsSize(uint32_t k) {
        return Util::ByteAlign((kEntriesPerPark - 1) * (k - kStubMinusBits)) / 8;
    }

    static uint32_t CalculateParkSize(uint8_t k, uint8_t table_index) {
        return CalculateLinePointSize(k) + CalculateStubsSize(k) + CalculateMaxDeltasSize(k, table_index);
    }

 private:
    uint64_t memorySize;
    uint8_t* parkToFileBytes;
    uint32_t parkToFileBytesSize;

    // Writes the plot file header to a file
    uint32_t WriteHeader(FileDisk& plot_Disk, uint8_t k, const uint8_t* id, const uint8_t* memo,
                         uint32_t memo_len) {
        // 19 bytes  - "Proof of Space Plot" (utf-8)
        // 32 bytes  - unique plot id
        // 1 byte    - k
        // 2 bytes   - format description length
        // x bytes   - format description
        // 2 bytes   - memo length
        // x bytes   - memo

        std::string header_text = "Proof of Space Plot";
        uint64_t write_pos=0;
        plot_Disk.Write(write_pos,(uint8_t *)header_text.data(), header_text.size());
        write_pos+=header_text.size();
        plot_Disk.Write(write_pos, (id), kIdLen);
        write_pos+=kIdLen;

        uint8_t k_buffer[1];
        k_buffer[0] = k;
        plot_Disk.Write(write_pos, (k_buffer), 1);
        write_pos+=1;

        uint8_t size_buffer[2];
        Util::IntToTwoBytes(size_buffer, kFormatDescription.size());
        plot_Disk.Write(write_pos, (size_buffer), 2);
        write_pos+=2;
        plot_Disk.Write(write_pos,(uint8_t *)kFormatDescription.data(), kFormatDescription.size());
        write_pos+=kFormatDescription.size();

        Util::IntToTwoBytes(size_buffer, memo_len);
        plot_Disk.Write(write_pos, (size_buffer), 2);
        write_pos+=2;
        plot_Disk.Write(write_pos, (memo), memo_len);
        write_pos+=memo_len;

        uint8_t pointers[10*8];
        memset(pointers, 0, 10*8);
        plot_Disk.Write(write_pos, (pointers), 10*8);
        write_pos+=10*8;

        uint32_t bytes_written = header_text.size() + kIdLen + 1 + 2 + kFormatDescription.size()
                                 + 2 + memo_len + 10*8;
        std::cout << "Wrote: " << bytes_written << std::endl;
        return bytes_written;
    }

    // This is Phase 1, or forward propagation. During this phase, all of the 7 tables,
    // and f functions, are evaluated. The result is an intermediate plot file, that is
    // several times larger than what the final file will be, but that has all of the
    // proofs of space in it. First, F1 is computed, which is special since it uses
    // ChaCha8, and each encryption provides multiple output values. Then, the rest of the
    // f functions are computed, and a sort on disk happens for each table.
    std::vector<uint64_t> WritePlotFile(uint8_t* memory, std::vector<FileDisk>& tmp_1_disks, uint8_t k, const uint8_t* id,
                                        const uint8_t* memo, uint8_t memo_len) {
        uint64_t plot_file=0;

        std::cout << "Computing table 1" << std::endl;
        Timer f1_start_time;
        F1Calculator f1(k, id);
        uint64_t x = 0;

        uint32_t entry_size_bytes = GetMaxEntrySize(k, 1, true);

        // The max value our input (x), can take. A proof of space is 64 of these x values.
        uint64_t max_value = ((uint64_t)1 << (k)) - 1;
        uint8_t buf[14];

        // These are used for sorting on disk. The sort on disk code needs to know how
        // many elements are in each bucket.
        std::vector<uint64_t> bucket_sizes(kNumSortBuckets, 0);
        std::vector<uint64_t> right_bucket_sizes(kNumSortBuckets, 0);
        std::vector<uint64_t> table_sizes = std::vector<uint64_t>(8, 0);

        // Instead of computing f1(1), f1(2), etc, for each x, we compute them in batches
        // to increase CPU efficency.
        for (uint64_t lp = 0; lp <= (((uint64_t)1) << (k-kBatchSizes)); lp++) {
            // For each pair x, y in the batch
            for (auto kv : f1.CalculateBuckets(Bits(x, k), 2 << (kBatchSizes - 1))) {
                // TODO(mariano): fix inefficient memory alloc here
                (std::get<0>(kv) + std::get<1>(kv)).ToBytes(buf);

                // We write the x, y pair
                tmp_1_disks[1].Write(plot_file, (buf), entry_size_bytes);
                plot_file+=entry_size_bytes;

                bucket_sizes[SortOnDiskUtils::ExtractNum(buf, entry_size_bytes, 0, kLogNumSortBuckets)] += 1;

                if (x + 1 > max_value) {
                    break;
                }
                ++x;
            }
            if (x + 1 > max_value) {
                break;
            }
        }
        // A zero entry is the end of table symbol.
        memset(buf, 0x00, entry_size_bytes);
        tmp_1_disks[1].Write(plot_file, buf, entry_size_bytes);
        table_sizes[1] = x + 1;
        plot_file+=entry_size_bytes;

        f1_start_time.PrintElapsed("F1 complete, Time = ");

        // Total number of entries in the current table (f1)
        uint64_t total_table_entries = ((uint64_t)1) << k;

        // Store positions to previous tables, in k bits.
        uint8_t pos_size = k;
        uint32_t right_entry_size_bytes = 0;
        uint64_t max_spare_written = 0;

        // Number of buckets that y values will be put into.
        double num_buckets = ((uint64_t)1 << (k + kExtraBits)) / static_cast<double>(kBC) + 1;

        // For tables 1 through 6, sort the table, calculate matches, and write
        // the next table. This is the left table index.
        for (uint8_t table_index = 1; table_index < 7; table_index++) {
            Timer table_timer;
            uint8_t metadata_size = kVectorLens[table_index + 1] * k;

            // Determines how many bytes the entries in our left and right tables will take up.
            uint32_t entry_size_bytes = GetMaxEntrySize(k, table_index, true);
            uint32_t compressed_entry_size_bytes = GetMaxEntrySize(k, table_index, false);
            right_entry_size_bytes = GetMaxEntrySize(k, table_index + 1, true);

            std::cout << "Computing table " << int{table_index + 1} << std::endl;

            total_table_entries = 0;

            std::cout << "\tSorting table " << int{table_index} << std::endl;

            // Performs a sort on the left table,
            Timer sort_timer;
            uint64_t spare_written = Sorting::SortOnDisk(tmp_1_disks[table_index], 0, tmp_1_disks[0], entry_size_bytes,
                                                         0, bucket_sizes, memory, memorySize);
            if (spare_written > max_spare_written) {
                max_spare_written = spare_written;
            }
            sort_timer.PrintElapsed("\tSort time:");

            Timer computation_pass_timer;

            // Streams to read and right to tables. We will have handles to two tables. We will
            // read through the left table, compute matches, and evaluate f for matching entries,
            // writing results to the right table.
            uint64_t left_reader=0;
            uint64_t left_writer=0;
            uint64_t right_writer=0;
            uint8_t *right_writer_buf=&(memory[0]);
            uint8_t *left_writer_buf=&(memory[memorySize/2]);
            uint64_t left_buf_entries=memorySize/2/compressed_entry_size_bytes;
            uint64_t right_buf_entries=memorySize/2/right_entry_size_bytes;
            uint64_t left_writer_count=0;
            uint64_t right_writer_count=0;

            FxCalculator f(k, table_index + 1);

            // This is a sliding window of entries, since things in bucket i can match with things in bucket
            // i + 1. At the end of each bucket, we find matches between the two previous buckets.
            std::vector<PlotEntry> bucket_L;
            std::vector<PlotEntry> bucket_R;

            uint64_t bucket = 0;
            uint64_t pos = 0;  // Position into the left table
            bool end_of_table = false;  // We finished all entries in the left table
            uint64_t matches = 0;  // Total matches

            // Buffers for storing a left or a right entry, used for disk IO
            uint8_t *left_buf = new uint8_t[entry_size_bytes + 7];
            uint8_t* right_buf;
            uint8_t* tmp_buf;

            Bits new_left_entry(0, pos_size + kOffsetSize);
            std::vector<std::tuple<PlotEntry, PlotEntry, std::pair<Bits, Bits>> > current_entries_to_write;
            std::vector<std::tuple<PlotEntry, PlotEntry, std::pair<Bits, Bits>> > future_entries_to_write;
            std::vector<std::pair<uint16_t, uint16_t> > match_indexes;
            std::vector<PlotEntry*> not_dropped;  // Pointers are stored to avoid copying entries

            // Stores map of old positions to new positions (positions after dropping entries from L table that did not match)
            // Map ke
            uint16_t position_map_size = 2000;
            uint16_t* L_position_map = new uint16_t[position_map_size];  // Should comfortably fit 2 buckets worth of items
            uint16_t* R_position_map = new uint16_t[position_map_size];
            uint64_t L_position_base = 0;
            uint64_t R_position_base = 0;
            uint64_t newlpos, newrpos;

            // Start at left table pos = 0 and iterate through the whole table. Note that the left table
            // will already be sorted by y
            while (!end_of_table) {
                PlotEntry left_entry;
                left_entry.right_metadata = 0;
                // Reads a left entry from disk
                tmp_1_disks[table_index].Read(left_reader, left_buf, entry_size_bytes);

                left_reader += entry_size_bytes;

                if (table_index == 1) {
                    // For table 1, we only have y and metadata
                    left_entry.y = Util::SliceInt64FromBytes(left_buf,
                                                             0, k + kExtraBits);
                    left_entry.left_metadata = Util::SliceInt64FromBytes(left_buf,
                                                                          k + kExtraBits, metadata_size);
                } else {
                    // For tables 2-6, we we also have pos and offset. We need to read this because this entry
                    // will be written again to the table without the y (and some entries are dropped).
                    left_entry.y = Util::SliceInt64FromBytes(left_buf, 0, k + kExtraBits);
                    left_entry.read_posoffset = Util::SliceInt64FromBytes(left_buf, k + kExtraBits, pos_size + kOffsetSize);
                    if (metadata_size <= 128) {
                        left_entry.left_metadata = Util::SliceInt128FromBytes(left_buf,
                                                                              k + kExtraBits + pos_size + kOffsetSize,
                                                                              metadata_size);
                    } else {
                        // Large metadatas that don't fit into 128 bits. (k > 32).
                        left_entry.left_metadata = Util::SliceInt128FromBytes(left_buf,
                                                                              k + kExtraBits + pos_size
                                                                                + kOffsetSize, 128);
                        left_entry.right_metadata = Util::SliceInt128FromBytes(left_buf,
                                                                               k + kExtraBits + pos_size
                                                                                 + kOffsetSize + 128,
                                                                               metadata_size - 128);
                    }
                }

                // This is not the pos that was read from disk,but the position of the entry we read, within L table.
                left_entry.pos = pos;
                left_entry.used = false;

                end_of_table = (left_entry.y == 0 && left_entry.left_metadata == 0 && left_entry.right_metadata == 0);
                uint64_t y_bucket = left_entry.y / kBC;

                // Keep reading left entries into bucket_L and R, until we run out of things
                if (y_bucket == bucket) {
                    bucket_L.emplace_back(std::move(left_entry));
                } else if (y_bucket == bucket + 1) {
                    bucket_R.emplace_back(std::move(left_entry));
                } else {
                    // This is reached when we have finished adding stuff to bucket_R and bucket_L,
                    // so now we can compare entries in both buckets to find matches. If two entries match,
                    // the result is written to the right table. However the writing happens in the next iteration
                    // of the loop, since we need to remap positions.
                    if (bucket_L.size() > 0) {
                        not_dropped.clear();

                        if (bucket_R.size() > 0) {
                            // Compute all matches between the two buckets and save indeces.
                            match_indexes = f.FindMatches(bucket_L, bucket_R);

                            // We mark entries as used if they took part in a match.
                            for (auto& indeces : match_indexes) {
                                bucket_L[std::get<0>(indeces)].used = true;
                                if (end_of_table) {
                                    bucket_R[std::get<1>(indeces)].used = true;
                                }
                            }
                        }

                        // Adds L_bucket entries that are used to not_dropped. They are used if they either
                        // matched with something to the left (in the previous iteration), or matched with something
                        // in bucket_R (in this iteration).
                        for (size_t bucket_index = 0; bucket_index < bucket_L.size(); bucket_index++) {
                            PlotEntry& L_entry = bucket_L[bucket_index];
                            if (L_entry.used) {
                                not_dropped.emplace_back(&bucket_L[bucket_index]);
                            }
                        }
                        if (end_of_table) {
                            // In the last two buckets, we will not get a chance to enter the next iteration due to breaking
                            // from loop. Therefore to write the final bucket in this iteration, we have to add the R entries
                            // to the not_dropped list.
                            for (size_t bucket_index = 0; bucket_index < bucket_R.size(); bucket_index++) {
                                PlotEntry& R_entry = bucket_R[bucket_index];
                                if (R_entry.used) {
                                    not_dropped.emplace_back(&R_entry);
                                }
                            }
                        }
                        // We keep maps from old positions to new positions. We only need two maps, one for L bucket and one
                        // for R bucket, and we cycle through them. Map keys are stored as positions % 2^10 for efficiency.
                        // Map values are stored as offsets from the base position for that bucket, for efficiency.
                        uint16_t* tmp = L_position_map;
                        L_position_map = R_position_map;
                        R_position_map = tmp;
                        L_position_base = R_position_base;
                        R_position_base = left_writer_count;

                        for (PlotEntry* &entry : not_dropped) {
                            // Rewrite left entry with just pos and offset, to reduce working space
                            if (table_index == 1) {
                                // Table 1 goes from (f1, x) to just (x)
                                new_left_entry = Bits(entry->left_metadata, k);
                            } else {
                                // Other tables goes from (f1, pos, offset, metadata) to just (pos, offset)
                                new_left_entry = Bits(entry->read_posoffset, pos_size + kOffsetSize);
                            }
                            tmp_buf=left_writer_buf + (left_writer_count % left_buf_entries) * compressed_entry_size_bytes;
                            // The new position for this entry = the total amount of thing written to L so far. Since we only
                            // write entries in not_dropped, about 14% of entries are dropped.
                            R_position_map[entry->pos % position_map_size] = left_writer_count - R_position_base;
                            left_writer_count++;
                            new_left_entry.ToBytes(tmp_buf);
                            if(left_writer_count % left_buf_entries==0) {
                                tmp_1_disks[table_index].Write(left_writer, left_writer_buf,
                                         left_buf_entries*compressed_entry_size_bytes);
                                left_writer+=left_buf_entries*compressed_entry_size_bytes;
                            }
                        }

                        // Two vectors to keep track of things from previous iteration and from this iteration.
                        current_entries_to_write.swap(future_entries_to_write);
                        future_entries_to_write.clear();

                        for (auto& indeces : match_indexes) {
                            PlotEntry& L_entry = bucket_L[std::get<0>(indeces)];
                            PlotEntry& R_entry = bucket_R[std::get<1>(indeces)];

                            ++matches;
                            ++total_table_entries;

                            // Sets the R entry to used so that we don't drop in next iteration
                            R_entry.used = true;
                            // Computes the output pair (fx, new_metadata)
                            if (metadata_size <= 128) {
                                const std::pair<Bits, Bits>& f_output = f.CalculateBucket(Bits(L_entry.y, k + kExtraBits),
                                                             Bits(R_entry.y, k + kExtraBits),
                                                             Bits(L_entry.left_metadata, metadata_size),
                                                             Bits(R_entry.left_metadata, metadata_size));
                                future_entries_to_write.push_back(std::make_tuple(L_entry, R_entry, f_output));
                            } else {
                                // Metadata does not fit into 128 bits
                                const std::pair<Bits, Bits>& f_output = f.CalculateBucket(Bits(L_entry.y, k + kExtraBits),
                                                                  Bits(R_entry.y, k + kExtraBits),
                                                             Bits(L_entry.left_metadata, 128)
                                                              + Bits(L_entry.right_metadata, metadata_size - 128),
                                                             Bits(R_entry.left_metadata, 128)
                                                              + Bits(R_entry.right_metadata, metadata_size - 128));
                                future_entries_to_write.push_back(std::make_tuple(L_entry, R_entry, f_output));
                            }
                        }
                        // At this point, future_entries_to_write contains the matches of buckets L and R, and current_entries_to_write
                        // contains the matches of L and the bucket left of L. These are the ones that we will write.
                        uint16_t final_current_entry_size = current_entries_to_write.size();
                        if (end_of_table) {
                            // For the final bucket, write the future entries now as well, since we will break from loop
                            current_entries_to_write.insert(current_entries_to_write.end(), future_entries_to_write.begin(), future_entries_to_write.end());
                        }
                        for (size_t i = 0; i < current_entries_to_write.size(); i++) {
                            const auto& entry_tuple = current_entries_to_write[i];
                            const PlotEntry& L_entry = std::get<0>(entry_tuple);
                            const PlotEntry& R_entry = std::get<1>(entry_tuple);

                            const std::pair<Bits, Bits>& f_output = std::get<2>(entry_tuple);
                            // We only need k instead of k + kExtraBits bits for the last table
                            Bits new_entry = table_index + 1 == 7 ? std::get<0>(f_output).Slice(0, k) : std::get<0>(f_output);

                            // Maps the new positions. If we hit end of pos, we must write things in both final_entries to write
                            // and current_entries_to_write, which are in both position maps.
                            if (!end_of_table || i < final_current_entry_size) {
                                newlpos = L_position_map[L_entry.pos % position_map_size] + L_position_base;
                            } else {
                                newlpos = R_position_map[L_entry.pos % position_map_size] + R_position_base;
                            }
                            newrpos = R_position_map[R_entry.pos % position_map_size] + R_position_base;
                            // Position in the previous table
                            new_entry.AppendValue(newlpos, pos_size);

                            // Offset for matching entry
                            if (newrpos - newlpos > (1U << kOffsetSize) * 97 / 100) {
                                std::cout << "Offset: " <<  newrpos - newlpos << std::endl;
                                abort();
                            }
                            new_entry.AppendValue(newrpos - newlpos, kOffsetSize);
                            // New metadata which will be used to compute the next f
                            new_entry += std::get<1>(f_output);

                            right_buf=right_writer_buf+(right_writer_count%right_buf_entries)*right_entry_size_bytes;
                            right_writer_count++;

                            // Writes the new entry into the right table
                            new_entry.ToBytes(right_buf);
                            if(right_writer_count%right_buf_entries==0) {
                                tmp_1_disks[table_index + 1].Write(right_writer, right_writer_buf,
                                                                   right_buf_entries*right_entry_size_bytes);
                                right_writer+=right_buf_entries*right_entry_size_bytes;
                            }

                            // Computes sort bucket, so we can sort the table by y later, more easily
                            right_bucket_sizes[SortOnDiskUtils::ExtractNum(right_buf, right_entry_size_bytes, 0,
                                                                           kLogNumSortBuckets)] += 1;
                        }
                    }
                    if (y_bucket == bucket + 2) {
                        // We saw a bucket that is 2 more than the current, so we just set L = R, and R = [entry]
                        bucket_L = bucket_R;
                        bucket_R = std::vector<PlotEntry>();
                        bucket_R.emplace_back(std::move(left_entry));
                        ++bucket;
                    } else {
                        // We saw a bucket that >2 more than the current, so we just set L = [entry], and R = []
                        bucket = y_bucket;
                        bucket_L = std::vector<PlotEntry>();
                        bucket_L.emplace_back(std::move(left_entry));
                        bucket_R = std::vector<PlotEntry>();
                    }
                }
                // Increase the read pointer in the left table, by one
                ++pos;
            }

            // Total matches found in the left table
            std::cout << "\tTotal matches: " << matches << ". Per bucket: "
                      << (matches / num_buckets) << std::endl;

            table_sizes[table_index] = left_writer_count + 1;
            if (table_index == 6) {
                table_sizes[7] = right_writer_count + 1;
            }
            // Finishes writing L table, and writes the 0 entry (EOT) for left table
            // Also truncates the file after the final write position, deleting no longer useful working space
            tmp_1_disks[table_index].Write(left_writer, left_writer_buf,
                (left_writer_count%left_buf_entries)*compressed_entry_size_bytes);
            left_writer+=(left_writer_count%left_buf_entries)*compressed_entry_size_bytes;

            memset(left_buf, 0x00, compressed_entry_size_bytes);
            tmp_1_disks[table_index].Write(left_writer, left_buf, compressed_entry_size_bytes);
            left_writer += compressed_entry_size_bytes;
            tmp_1_disks[table_index].Truncate(left_writer);

            tmp_1_disks[table_index + 1].Write(right_writer, right_writer_buf,
                (right_writer_count%right_buf_entries)*right_entry_size_bytes);
            right_writer+=(right_writer_count%right_buf_entries)*right_entry_size_bytes;

            // Writes the 0 entry (EOT) for right table
            memset(right_writer_buf, 0x00, right_entry_size_bytes);
            tmp_1_disks[table_index + 1].Write(right_writer, right_writer_buf, right_entry_size_bytes);


            // Resets variables
            bucket_sizes = right_bucket_sizes;
            right_bucket_sizes = std::vector<uint64_t>(kNumSortBuckets, 0);

            computation_pass_timer.PrintElapsed("\tComputation pass time:");
            table_timer.PrintElapsed("Forward propagation table time:");

            delete[] left_buf;
            delete[] L_position_map;
            delete[] R_position_map;
        }
        table_sizes[0] = max_spare_written;
        return table_sizes;
    }

    // Backpropagate takes in as input, a file on which forward propagation has been done.
    // The purpose of backpropagate is to eliminate any dead entries that don't contribute
    // to final values in f7, to minimize disk usage. A sort on disk is applied to each table,
    // so that they are sorted by position.
    std::vector<uint64_t> Backpropagate(uint8_t* memory, std::vector<FileDisk>& tmp_1_disks, std::vector<uint64_t> table_sizes, uint8_t k,
                                        const uint8_t* id, const uint8_t* memo, uint32_t memo_len) {
        // An extra bit is used, since we may have more than 2^k entries in a table. (After pruning, each table will
        // have 0.8*2^k or less entries).
        uint8_t pos_size = k;

        std::vector<uint64_t> bucket_sizes_pos(kNumSortBuckets, 0);
        std::vector<uint64_t> new_table_sizes = std::vector<uint64_t>(8, 0);
        new_table_sizes[7] = table_sizes[7];

        // Iterates through each table (with a left and right pointer), starting at 6 & 7.
        for (uint8_t table_index = 7; table_index > 1; --table_index) {
            //std::vector<std::pair<uint64_t, uint64_t> > match_positions;
            Timer table_timer;

            std::cout << "Backpropagating on table " << int{table_index} << std::endl;

            std::vector<uint64_t> new_bucket_sizes_pos(kNumSortBuckets, 0);

            uint16_t left_metadata_size = kVectorLens[table_index] * k;

            // The entry that we are reading (no metadata)
            uint16_t left_entry_size_bytes = GetMaxEntrySize(k, table_index - 1, false);

            // The right entries which we read and write (the already have no metadata, since they have
            // been pruned in previous iteration)
            uint16_t right_entry_size_bytes = GetMaxEntrySize(k, table_index, false);

            // Doesn't sort table 7, since it's already sorted by pos6 (position into table 6).
            // The reason we sort, is so we can iterate through both tables at once. For example,
            // if we read a right entry (pos, offset) = (456, 2), the next one might be (458, 19),
            // and in the left table, we are reading entries around pos 450, etc..
            if (table_index != 7) {
                std::cout << "\tSorting table " << int{table_index} << std::endl;
                Timer sort_timer;
                Sorting::SortOnDisk(tmp_1_disks[table_index], 0, tmp_1_disks[0],
                                    right_entry_size_bytes,
                                    0, bucket_sizes_pos, memory, memorySize);

                sort_timer.PrintElapsed("\tSort time:");
            }
            Timer computation_pass_timer;

            uint64_t left_reader=0;
            uint64_t left_writer=0;
            uint64_t right_reader=0;
            uint64_t right_writer=0;
            uint8_t *left_reader_buf=&(memory[0]);
            uint8_t *left_writer_buf=&(memory[memorySize/4]);
            uint8_t *right_reader_buf=&(memory[memorySize/2]);
            uint8_t *right_writer_buf=&(memory[3*memorySize/4]);
            uint64_t left_buf_entries=memorySize/4/left_entry_size_bytes;
            uint64_t new_left_buf_entries=memorySize/4/left_entry_size_bytes;
            uint64_t right_buf_entries=memorySize/4/right_entry_size_bytes;
            uint64_t left_reader_count=0;
            uint64_t right_reader_count=0;
            uint64_t left_writer_count=0;
            uint64_t right_writer_count=0;

            // We will divide by 2, so it must be even.
            assert(kCachedPositionsSize % 2 == 0);

            // Used positions will be used to mark which posL are present in table R, the rest will be pruned
            bool used_positions[kCachedPositionsSize];
            memset(used_positions, 0, sizeof(used_positions));

            bool should_read_entry = true;

            // Cache for when we read a right entry that is too far forward
            uint64_t cached_entry_sort_key = 0;  // For table_index == 7, y is here
            uint64_t cached_entry_pos = 0;
            uint64_t cached_entry_offset = 0;

            uint64_t left_entry_counter = 0;  // Total left entries written

            // Sliding window map, from old position to new position (after pruning)
            uint64_t new_positions[kCachedPositionsSize];

            // Sort keys represent the ordering of entries, sorted by (y, pos, offset),
            // but using less bits (only k+1 instead of 2k + 9, etc.)
            // This is a map from old position to array of sort keys (one for each R entry with this pos)
            uint64_t old_sort_keys[kReadMinusWrite][kMaxMatchesSingleEntry];
            // Map from old position to other positions that it matches with
            uint64_t old_offsets[kReadMinusWrite][kMaxMatchesSingleEntry];
            // Map from old position to count (number of times it appears)
            uint16_t old_counters[kReadMinusWrite];

            for (uint32_t i = 0; i < kReadMinusWrite; i++) {
                old_counters[i] = 0;
            }

            bool end_of_right_table = false;
            uint64_t current_pos = 0;  // This is the current pos that we are looking for in the L table
            uint64_t end_of_table_pos = 0;
            uint64_t greatest_pos = 0;  // This is the greatest position we have seen in R table

            // Buffers for reading and writing to disk
            uint8_t* left_entry_buf;
            uint8_t* new_left_entry_buf;
            uint8_t* right_entry_buf;

            // Go through all right entries, and keep going since write pointer is behind read pointer
            while (!end_of_right_table || (current_pos - end_of_table_pos <= kReadMinusWrite)) {
                old_counters[current_pos % kReadMinusWrite] = 0;

                // Resets used positions after a while, so we use little memory
                if ((current_pos - kReadMinusWrite) % (kCachedPositionsSize / 2) == 0) {
                    if ((current_pos - kReadMinusWrite) % kCachedPositionsSize == 0) {
                        for (uint32_t i = kCachedPositionsSize / 2; i < kCachedPositionsSize; i++) {
                            used_positions[i] = false;
                        }
                    } else {
                        for (uint32_t i = 0; i < kCachedPositionsSize / 2; i++) {
                            used_positions[i] = false;
                        }
                    }
                }
                // Only runs this code if we are still reading the right table, or we still need to read
                // more left table entries (current_pos <= greatest_pos), otherwise, it skips to the
                // writing of the final R table entries
                if (!end_of_right_table || current_pos <= greatest_pos) {
                    uint64_t entry_sort_key = 0;
                    uint64_t entry_pos = 0;
                    uint64_t entry_offset = 0;

                    while (!end_of_right_table) {
                        if (should_read_entry) {
                            // Need to read another entry at the current position
                           if(right_reader_count%right_buf_entries==0) {
                               uint64_t readAmt=std::min(right_buf_entries*right_entry_size_bytes,
                                   (new_table_sizes[table_index]-right_reader_count)*right_entry_size_bytes);

                               tmp_1_disks[table_index].Read(right_reader, right_reader_buf,
                                              readAmt);
                               right_reader+=readAmt;
                            }
                            right_entry_buf=right_reader_buf+(right_reader_count%right_buf_entries)*right_entry_size_bytes;
                            right_reader_count++;

                            if (table_index == 7) {
                                // This is actually y for table 7
                                entry_sort_key = Util::SliceInt64FromBytes(right_entry_buf,
                                                                           0, k);
                                entry_pos = Util::SliceInt64FromBytes(right_entry_buf,
                                                                      k, pos_size);
                                entry_offset = Util::SliceInt64FromBytes(right_entry_buf,
                                                                         k + pos_size, kOffsetSize);
                            } else {
                                entry_pos = Util::SliceInt64FromBytes(right_entry_buf,
                                                                      0, pos_size);
                                entry_offset = Util::SliceInt64FromBytes(right_entry_buf,
                                                                         pos_size, kOffsetSize);
                                entry_sort_key = Util::SliceInt64FromBytes(right_entry_buf,
                                                                           pos_size + kOffsetSize, k + 1);
                            }
                        } else if (cached_entry_pos == current_pos) {
                            // We have a cached entry at this position
                            entry_sort_key = cached_entry_sort_key;
                            entry_pos = cached_entry_pos;
                            entry_offset = cached_entry_offset;
                        } else {
                            // The cached entry is at a later pos, so we don't read any more R entries,
                            // read more L entries instead.
                            break;
                        }

                        should_read_entry = true;  // By default, read another entry
                        if (entry_pos + entry_offset > greatest_pos) {
                            // Greatest L pos that we should look for
                            greatest_pos = entry_pos + entry_offset;
                        }
                        if (entry_sort_key == 0 && entry_pos == 0 && entry_offset == 0) {
                            // Table R has ended, don't read any more (but keep writing)
                            end_of_right_table = true;
                            end_of_table_pos = current_pos;
                            break;
                        } else if (entry_pos == current_pos) {
                            // The current L position is the current R entry
                            // Marks the two matching entries as used (pos and pos+offset)
                            used_positions[entry_pos % kCachedPositionsSize] = true;
                            used_positions[(entry_pos + entry_offset) % kCachedPositionsSize] = true;

                            uint64_t old_write_pos = entry_pos % kReadMinusWrite;

                            // Stores the sort key for this R entry
                            old_sort_keys[old_write_pos][old_counters[old_write_pos]] = entry_sort_key;

                            // Stores the other matching pos for this R entry (pos6 + offset)
                            old_offsets[old_write_pos][old_counters[old_write_pos]] = entry_pos + entry_offset;
                            ++old_counters[old_write_pos];
                        } else {
                            // Don't read any more right entries for now, because we haven't caught up on the
                            // left table yet
                            should_read_entry = false;
                            cached_entry_sort_key = entry_sort_key;
                            cached_entry_pos = entry_pos;
                            cached_entry_offset = entry_offset;
                            break;
                        }
                    }
                    // ***Reads a left entry
                    if(left_reader_count%left_buf_entries==0) {
                         uint64_t readAmt=std::min(left_buf_entries*left_entry_size_bytes,
                            (table_sizes[table_index - 1] - left_reader_count) * left_entry_size_bytes);
                         tmp_1_disks[table_index - 1].Read(left_reader, left_reader_buf,
                                readAmt);
                         left_reader+=readAmt;
                    }
                    left_entry_buf=left_reader_buf+(left_reader_count%left_buf_entries)*left_entry_size_bytes;
                    left_reader_count++;

                    // If this left entry is used, we rewrite it. If it's not used, we ignore it.
                    if (used_positions[current_pos % kCachedPositionsSize]) {
                        uint64_t entry_metadata;

                        if (table_index > 2) {
                            // For tables 2-6, the entry is: pos, offset
                            entry_pos = Util::SliceInt64FromBytes(left_entry_buf,
                                                                  0, pos_size);
                            entry_offset = Util::SliceInt64FromBytes(left_entry_buf,
                                                                     pos_size, kOffsetSize);
                        } else {
                            entry_metadata = Util::SliceInt64FromBytes(left_entry_buf,
                                                                        0, left_metadata_size);
                        }

                        new_left_entry_buf=left_writer_buf+(left_writer_count%new_left_buf_entries)*left_entry_size_bytes;
                        left_writer_count++;

                        Bits new_left_entry;
                        if (table_index > 2) {
                            // The new left entry is slightly different. Metadata is dropped, to save space,
                            // and the counter of the entry is written (sort_key). We use this instead of
                            // (y + pos + offset) since its smaller.
                            new_left_entry += Bits(entry_pos, pos_size);
                            new_left_entry += Bits(entry_offset, kOffsetSize);
                            new_left_entry += Bits(left_entry_counter, k + 1);

                            // If we are not taking up all the bits, make sure they are zeroed
                            if (Util::ByteAlign(new_left_entry.GetSize()) < left_entry_size_bytes * 8) {
                                memset(new_left_entry_buf, 0, left_entry_size_bytes);
                            }
                        } else {
                            // For table one entries, we don't care about sort key, only x.
                            new_left_entry += Bits(entry_metadata, left_metadata_size);
                        }
                        new_left_entry.ToBytes(new_left_entry_buf);

                        if(left_writer_count%new_left_buf_entries==0) {
                            tmp_1_disks[table_index - 1].Write(left_writer, left_writer_buf,
                                new_left_buf_entries*left_entry_size_bytes);
                            left_writer+=new_left_buf_entries*left_entry_size_bytes;
                        }

                        new_bucket_sizes_pos[SortOnDiskUtils::ExtractNum(new_left_entry_buf, left_entry_size_bytes,
                                                                        0, kLogNumSortBuckets)] += 1;
                        // Mapped positions, so we can rewrite the R entry properly
                        new_positions[current_pos % kCachedPositionsSize] = left_entry_counter;

                        // Counter for new left entries written
                        ++left_entry_counter;
                    }
                }
                // Write pointer lags behind the read pointer
                int64_t write_pointer_pos = current_pos - kReadMinusWrite + 1;

                // Only write entries for write_pointer_pos, if we are above 0, and there are actually R entries
                // for that pos.
                if (write_pointer_pos >= 0 && used_positions[write_pointer_pos % kCachedPositionsSize]) {
                    uint64_t new_pos = new_positions[write_pointer_pos % kCachedPositionsSize];
                    Bits new_pos_bin(new_pos, pos_size);
                    // There may be multiple R entries that share the write_pointer_pos, so write all of them
                    for (uint32_t counter = 0; counter < old_counters[write_pointer_pos % kReadMinusWrite]; counter++) {
                        // Creates and writes the new right entry, with the cached data
                        uint64_t new_offset_pos = new_positions[old_offsets[write_pointer_pos % kReadMinusWrite]
                                                                [counter] % kCachedPositionsSize];
                        Bits new_right_entry = table_index == 7 ? Bits(old_sort_keys[write_pointer_pos % kReadMinusWrite][counter], k) :
                                                                  Bits(old_sort_keys[write_pointer_pos % kReadMinusWrite][counter], k + 1);
                        new_right_entry += new_pos_bin;
                        //match_positions.push_back(std::make_pair(new_pos, new_offset_pos));
                        new_right_entry.AppendValue(new_offset_pos - new_pos, kOffsetSize);

                        // Calculate right entry pointer for output
                        right_entry_buf=right_writer_buf+(right_writer_count%right_buf_entries)*right_entry_size_bytes;
                        right_writer_count++;

                        if (Util::ByteAlign(new_right_entry.GetSize()) < right_entry_size_bytes * 8) {
                            memset(right_entry_buf, 0, right_entry_size_bytes);
                        }
                        new_right_entry.ToBytes(right_entry_buf);

                        // Check for write out to disk
                        if(right_writer_count%right_buf_entries==0) {
                            tmp_1_disks[table_index].Write(right_writer, right_writer_buf,
                                right_buf_entries*right_entry_size_bytes);
                            right_writer+=right_buf_entries*right_entry_size_bytes;
                        }

                    }
                }
                ++current_pos;
            }
            new_table_sizes[table_index - 1] = left_entry_counter + 1;

            std::cout << "\tWrote left entries: " <<  left_entry_counter << std::endl;
            computation_pass_timer.PrintElapsed("\tComputation pass time:");
            table_timer.PrintElapsed("Total backpropagation time::");

            tmp_1_disks[table_index].Write(right_writer, right_writer_buf,
                (right_writer_count%right_buf_entries)*right_entry_size_bytes);
            right_writer+=(right_writer_count%right_buf_entries)*right_entry_size_bytes;

            // Writes the 0 entry (EOT) for right table
            memset(right_writer_buf, 0x00, right_entry_size_bytes);
            tmp_1_disks[table_index].Write(right_writer, right_writer_buf, right_entry_size_bytes);
            right_writer += right_entry_size_bytes;
            tmp_1_disks[table_index].Truncate(right_writer);

            tmp_1_disks[table_index - 1].Write(left_writer, left_writer_buf,
                (left_writer_count%new_left_buf_entries)*left_entry_size_bytes);
            left_writer+=(left_writer_count%new_left_buf_entries)*left_entry_size_bytes;

            // Writes the 0 entry (EOT) for left table
            memset(left_writer_buf, 0x00, left_entry_size_bytes);
            tmp_1_disks[table_index - 1].Write(left_writer, left_writer_buf, left_entry_size_bytes);
            left_writer += left_entry_size_bytes;
            tmp_1_disks[table_index - 1].Truncate(left_writer);

            bucket_sizes_pos = new_bucket_sizes_pos;
        }
        return new_table_sizes;
    }

    // This writes a number of entries into a file, in the final, optimized format. The park contains
    // a checkpoint value (whicch is a 2k bits line point), as well as EPP (entries per park) entries.
    // These entries are each divded into stub and delta section. The stub bits are encoded as is, but
    // the delta bits are optimized into a variable encoding scheme. Since we have many entries in each
    // park, we can approximate how much space each park with take.
    // Format is: [2k bits of first_line_point]  [EPP-1 stubs] [Deltas size] [EPP-1 deltas]....  [first_line_point] ...
    void WriteParkToFile(FileDisk& final_disk, uint64_t table_start, uint64_t park_index, uint32_t park_size_bytes,
                         uint128_t first_line_point, const std::vector<uint8_t>& park_deltas,
                         const std::vector<uint64_t>& park_stubs, uint8_t k, uint8_t table_index) {
        // Parks are fixed size, so we know where to start writing. The deltas will not go over
        // into the next park.
        uint64_t writer=table_start + park_index * park_size_bytes;
        uint8_t *index = parkToFileBytes;

        Bits first_line_point_bits(first_line_point, 2*k);
        first_line_point_bits.ToBytes(index);
        index+=CalculateLinePointSize(k);

        // We use ParkBits insted of Bits since it allows storing more data
        ParkBits park_stubs_bits;
        for (uint64_t stub : park_stubs) {
            park_stubs_bits.AppendValue(stub, (k - kStubMinusBits));
        }
        uint32_t stubs_size = CalculateStubsSize(k);
        uint32_t stubs_valid_size = CDIV(park_stubs_bits.GetSize(), 8);
        park_stubs_bits.ToBytes(index);
        memset(index + stubs_valid_size, 0, stubs_size - stubs_valid_size);
        index += stubs_size;

        // The stubs are random so they don't need encoding. But deltas are more likely to
        // be small, so we can compress them
        double R = kRValues[table_index - 1];
        uint8_t *deltas_start = index + 2;
        size_t deltas_size = Encoding::ANSEncodeDeltas(park_deltas, R, deltas_start);

        if (!deltas_size) {
            // Uncompressed
            deltas_size = park_deltas.size();
            Util::IntToTwoBytesLE(index, deltas_size | 0x8000);
            memcpy(deltas_start, park_deltas.data(), deltas_size);
        } else {
            // Compressed
            Util::IntToTwoBytesLE(index, deltas_size);
        }

        index += 2 + deltas_size;

        if((uint32_t)(index-parkToFileBytes) > parkToFileBytesSize)
            std::cout << "index-parkToFileBytes " << index-parkToFileBytes << " parkToFileBytesSize " << parkToFileBytesSize << std::endl;

        final_disk.Write(writer, (uint8_t *)parkToFileBytes, index - parkToFileBytes);
    }

    // Compresses the plot file tables into the final file. In order to do this, entries must be
    // reorganized from the (pos, offset) bucket sorting order, to a more free line_point sorting
    // order. In (pos, offset ordering), we store two pointers two the previous table, (x, y) which
    // are very close together, by storing  (x, y-x), or (pos, offset), which can be done in about k + 8 bits,
    // since y is in the next bucket as x. In order to decrease this, We store the actual entries from the
    // previous table (e1, e2), instead of pos, offset pointers, and sort the entire table by (e1,e2).
    // Then, the deltas between each (e1, e2) can be stored, which require around k bits.

    // Converting into this format requires a few passes and sorts on disk. It also assumes that the
    // backpropagation step happened, so there will be no more dropped entries. See the design
    // document for more details on the algorithm.
    Phase3Results CompressTables(uint8_t* memory, uint8_t k, FileDisk& tmp2_disk /*filename*/,
                                 std::vector<FileDisk>& tmp_1_disks /*plot_filename*/, std::vector<uint64_t> table_sizes, const uint8_t* id, const uint8_t* memo,
                                 uint32_t memo_len) {
        // In this phase we open a new file, where the final contents of the plot will be stored.
        uint32_t header_size = WriteHeader(tmp2_disk, k, id, memo, memo_len);

        uint8_t pos_size = k;


        std::vector<uint64_t> final_table_begin_pointers(12, 0);
        final_table_begin_pointers[1] = header_size;

        uint8_t table_pointer_bytes[8];
        Util::IntToEightBytes(table_pointer_bytes, final_table_begin_pointers[1]);
        tmp2_disk.Write(header_size - 10*8, table_pointer_bytes, 8);

        uint64_t final_entries_written = 0;
        uint32_t right_entry_size_bytes = 0;

        // Iterates through all tables, starting at 1, with L and R pointers.
        // For each table, R entries are rewritten with line points. Then, the right table is
        // sorted by line_point. After this, the right table entries are rewritten as (sort_key, new_pos),
        // where new_pos is the position in the table, where it's sorted by line_point, and the line_points
        // are written to disk to a final table. Finally, table_i is sorted by sort_key. This allows us to
        // compare to the next table.
        for (uint8_t table_index = 1; table_index < 7; table_index++) {
            Timer table_timer;
            Timer computation_pass_1_timer;
            std::cout << "Compressing tables " << int{table_index} << " and " << int{table_index + 1} << std::endl;

            // The park size must be constant, for simplicity, but must be big enough to store EPP entries.
            // entry deltas are encoded with variable length, and thus there is no guarantee that they
            // won't override into the next park. It is only different (larger) for table 1
            uint32_t park_size_bytes = CalculateParkSize(k, table_index);

            std::vector<uint64_t> bucket_sizes(kNumSortBuckets, 0);

            // Sort key for table 7 is just y, which is k bits. For all other tables it can
            // be higher than 2^k and therefore k+1 bits are used.
            uint32_t right_sort_key_size = table_index == 6 ? k : k + 1;

            uint32_t left_entry_size_bytes = GetMaxEntrySize(k, table_index, false);
            right_entry_size_bytes = GetMaxEntrySize(k, table_index + 1, false);

            uint64_t left_reader=0;
            uint64_t right_reader=0;
            uint64_t right_writer=0;
            uint8_t *left_reader_buf=&(memory[0]);
            uint8_t *right_reader_buf=&(memory[memorySize/3]);
            uint8_t *right_writer_buf=&(memory[2*memorySize/3]);
            uint64_t left_buf_entries=memorySize/3/left_entry_size_bytes;
            uint64_t right_buf_entries=memorySize/3/right_entry_size_bytes;
            uint64_t left_reader_count=0;
            uint64_t right_reader_count=0;
            uint64_t right_writer_count=0;

            bool should_read_entry = true;
            std::vector<uint64_t> left_new_pos(kCachedPositionsSize);

            uint64_t old_sort_keys[kReadMinusWrite][kMaxMatchesSingleEntry];
            uint64_t old_offsets[kReadMinusWrite][kMaxMatchesSingleEntry];
            uint16_t old_counters[kReadMinusWrite];
            for (uint32_t i = 0; i < kReadMinusWrite; i++) {
                old_counters[i] = 0;
            }
            bool end_of_right_table = false;
            uint64_t current_pos = 0;
            uint64_t end_of_table_pos = 0;
            uint64_t greatest_pos = 0;

            uint8_t* right_entry_buf;
            uint8_t* right_entry_buf_out;
            uint8_t* left_entry_disk_buf;
            uint64_t entry_sort_key, entry_pos, entry_offset;
            uint64_t cached_entry_sort_key = 0;
            uint64_t cached_entry_pos = 0;
            uint64_t cached_entry_offset = 0;

            // Similar algorithm as Backprop, to read both L and R tables simultaneously
            while (!end_of_right_table || (current_pos - end_of_table_pos <= kReadMinusWrite)) {
                old_counters[current_pos % kReadMinusWrite] = 0;

                if (end_of_right_table || current_pos <= greatest_pos) {
                    while (!end_of_right_table) {
                        if (should_read_entry) {
                            // The right entries are in the format from backprop, (sort_key, pos, offset)
                            if(right_reader_count%right_buf_entries==0) {
                                uint64_t readAmt=std::min(right_buf_entries*right_entry_size_bytes,
                                    (table_sizes[table_index + 1]-right_reader_count) * right_entry_size_bytes);

                                tmp_1_disks[table_index + 1].Read(right_reader, right_reader_buf, readAmt);
                                right_reader+=readAmt;
                            }
                            right_entry_buf=right_reader_buf+(right_reader_count%right_buf_entries)*right_entry_size_bytes;
                            right_reader_count++;

                            entry_sort_key = Util::SliceInt64FromBytes(right_entry_buf,
                                                                       0, right_sort_key_size);
                            entry_pos = Util::SliceInt64FromBytes(right_entry_buf,
                                                                  right_sort_key_size, pos_size);
                            entry_offset = Util::SliceInt64FromBytes(right_entry_buf,
                                                                     right_sort_key_size + pos_size, kOffsetSize);
                        } else if (cached_entry_pos == current_pos) {
                            entry_sort_key = cached_entry_sort_key;
                            entry_pos = cached_entry_pos;
                            entry_offset = cached_entry_offset;
                        } else {
                            break;
                        }

                        should_read_entry = true;

                        if (entry_pos + entry_offset > greatest_pos) {
                            greatest_pos = entry_pos + entry_offset;
                        }
                        if (entry_sort_key == 0 && entry_pos == 0 && entry_offset == 0) {
                            end_of_right_table = true;
                            end_of_table_pos = current_pos;
                            break;
                        } else if (entry_pos == current_pos) {
                            uint64_t old_write_pos = entry_pos % kReadMinusWrite;
                            old_sort_keys[old_write_pos][old_counters[old_write_pos]]
                                = entry_sort_key;
                            old_offsets[old_write_pos][old_counters[old_write_pos]] = (entry_pos + entry_offset);
                            ++old_counters[old_write_pos];
                        } else {
                            should_read_entry = false;
                            cached_entry_sort_key = entry_sort_key;
                            cached_entry_pos = entry_pos;
                            cached_entry_offset = entry_offset;
                            break;
                        }
                    }
                    // The left entries are in the new format: (sort_key, new_pos), except for table 1: (y, x).
                    if(left_reader_count%left_buf_entries==0) {
                         uint64_t readAmt=std::min(left_buf_entries*left_entry_size_bytes,
                            (table_sizes[table_index]-left_reader_count) * left_entry_size_bytes);

                         tmp_1_disks[table_index].Read(left_reader, left_reader_buf,
                                readAmt);
                         left_reader+=readAmt;
                    }
                    left_entry_disk_buf=left_reader_buf+(left_reader_count%left_buf_entries)*left_entry_size_bytes;
                    left_reader_count++;

                    // We read the "new_pos" from the L table, which for table 1 is just x. For other tables,
                    // the new_pos
                    if (table_index == 1) {
                        // Only k bits, since this is x
                        left_new_pos[current_pos % kCachedPositionsSize]
                                = Util::SliceInt64FromBytes(left_entry_disk_buf, 0, k);
                    } else {
                        // k+1 bits in case it overflows
                        left_new_pos[current_pos % kCachedPositionsSize]
                                = Util::SliceInt64FromBytes(left_entry_disk_buf, k + 1, k + 1);
                    }
                }

                uint64_t write_pointer_pos = current_pos - kReadMinusWrite + 1;

                // Rewrites each right entry as (line_point, sort_key)
                if (current_pos + 1 >= kReadMinusWrite) {
                    uint64_t left_new_pos_1 = left_new_pos[write_pointer_pos % kCachedPositionsSize];
                    for (uint32_t counter = 0; counter < old_counters[write_pointer_pos % kReadMinusWrite]; counter++) {
                        uint64_t left_new_pos_2 = left_new_pos[old_offsets[write_pointer_pos % kReadMinusWrite][counter]
                                                % kCachedPositionsSize];

                        // A line point is an encoding of two k bit values into one 2k bit value.
                        uint128_t line_point = Encoding::SquareToLinePoint(left_new_pos_1, left_new_pos_2);

                        if (left_new_pos_1 > ((uint64_t)1 << k) || left_new_pos_2 > ((uint64_t)1 << k)) {
                            std::cout << "left or right positions too large" << std::endl;
                            std::cout << (line_point > ((uint128_t)1 << (2*k)));
                            if ((line_point > ((uint128_t)1 << (2*k)))) {
                                std::cout << "L, R: " << left_new_pos_1 <<  " " << left_new_pos_2 << std::endl;
                                std::cout << "Line point: " << line_point << std::endl;
                                abort();
                            }
                        }
                        Bits to_write = Bits(line_point, 2*k);
                        to_write += Bits(old_sort_keys[write_pointer_pos % kReadMinusWrite][counter], right_sort_key_size);

                        right_entry_buf=right_writer_buf+(right_writer_count%right_buf_entries)*right_entry_size_bytes;
                        right_writer_count++;

                        to_write.ToBytes(right_entry_buf);

                        if(right_writer_count%right_buf_entries==0) {
                            tmp_1_disks[table_index + 1].Write(right_writer, right_writer_buf,
                                right_buf_entries*right_entry_size_bytes);
                            right_writer+=right_buf_entries*right_entry_size_bytes;
                        }

                        bucket_sizes[SortOnDiskUtils::ExtractNum(right_entry_buf, right_entry_size_bytes, 0,
                                                                 kLogNumSortBuckets)] += 1;
                    }
                }
                current_pos += 1;
            }

            right_entry_buf=right_writer_buf+(right_writer_count%right_buf_entries)*right_entry_size_bytes;
            right_writer_count++;

            memset(right_entry_buf, 0x00, right_entry_size_bytes);

            tmp_1_disks[table_index + 1].Write(right_writer, right_writer_buf,
                (right_writer_count%right_buf_entries)*right_entry_size_bytes);
            right_writer+=(right_writer_count%right_buf_entries)*right_entry_size_bytes;
            tmp_1_disks[table_index + 1].Truncate(right_writer);

            // Remove no longer needed file
            tmp_1_disks[table_index].Truncate(0);

            computation_pass_1_timer.PrintElapsed("\tFirst computation pass time:");
            Timer sort_timer;
            std::cout << "\tSorting table " << int{table_index + 1} << std::endl;

            Sorting::SortOnDisk(tmp_1_disks[table_index + 1], 0, tmp_1_disks[0],
                                right_entry_size_bytes, 0, bucket_sizes, memory, memorySize, /*quicksort=*/1);

            sort_timer.PrintElapsed("\tSort time:");
            Timer computation_pass_2_timer;

            right_reader=0;
            right_writer=0;
            right_reader_buf=memory;
            right_writer_buf=&(memory[memorySize/2]);
            right_buf_entries=memorySize/2/right_entry_size_bytes;
            right_reader_count=0;
            right_writer_count=0;
            uint64_t final_table_writer=final_table_begin_pointers[table_index];

            final_entries_written = 0;

            std::vector<uint64_t> new_bucket_sizes(kNumSortBuckets, 0);
            std::vector<uint8_t> park_deltas;
            std::vector<uint64_t> park_stubs;
            uint128_t checkpoint_line_point = 0;
            uint128_t last_line_point = 0;
            uint64_t park_index = 0;

            uint64_t total_r_entries = 0;
            for (auto x : bucket_sizes) {
                total_r_entries += x;
            }
            // Now we will write on of the final tables, since we have a table sorted by line point. The final
            // table will simply store the deltas between each line_point, in fixed space groups(parks), with a
            // checkpoint in each group.
            Bits right_entry_bits;
            for (uint64_t index = 0; index < total_r_entries; index++) {
                if(right_reader_count%right_buf_entries==0) {
                      uint64_t readAmt=std::min(right_buf_entries*right_entry_size_bytes,
                           (total_r_entries-right_reader_count)*right_entry_size_bytes);

                      tmp_1_disks[table_index + 1].Read(right_reader, right_reader_buf, readAmt);
                      right_reader+=readAmt;
                }
                right_entry_buf=right_reader_buf+(right_reader_count%right_buf_entries)*right_entry_size_bytes;
                right_reader_count++;

                // Right entry is read as (line_point, sort_key)
                uint128_t line_point = Util::SliceInt128FromBytes(right_entry_buf,
                                                                  0, 2*k);
                uint64_t sort_key = Util::SliceInt64FromBytes(right_entry_buf,
                                                              2*k, right_sort_key_size);

                // Write the new position (index) and the sort key
                Bits to_write = Bits(sort_key, right_sort_key_size);
                to_write += Bits(index, k + 1);

                // Calculate right entry pointer for output
                right_entry_buf_out=right_writer_buf+(right_writer_count%right_buf_entries)*right_entry_size_bytes;
                right_writer_count++;

                memset(right_entry_buf_out, 0, right_entry_size_bytes);
                to_write.ToBytes(right_entry_buf_out);

                // Check for write out to disk
                if(right_writer_count%right_buf_entries==0) {
                    tmp_1_disks[table_index + 1].Write(right_writer, right_writer_buf,
                        right_buf_entries*right_entry_size_bytes);
                    right_writer+=right_buf_entries*right_entry_size_bytes;
                }

                new_bucket_sizes[SortOnDiskUtils::ExtractNum(right_entry_buf_out, right_entry_size_bytes, 0,
                                                             kLogNumSortBuckets)] += 1;
                // Every EPP entries, writes a park
                if (index % kEntriesPerPark == 0) {
                    if (index != 0) {
                        WriteParkToFile(tmp2_disk, final_table_begin_pointers[table_index],
                                        park_index, park_size_bytes, checkpoint_line_point, park_deltas,
                                        park_stubs, k, table_index);
                        park_index += 1;
                        final_entries_written += (park_stubs.size() + 1);
                    }
                    park_deltas.clear();
                    park_stubs.clear();

                    checkpoint_line_point = line_point;
                }
                uint128_t big_delta = line_point - last_line_point;

                // Since we have approx 2^k line_points between 0 and 2^2k, the average
                // space between them when sorted, is k bits. Much more efficient than storing each
                // line point. This is diveded into the stub and delta. The stub is the least
                // significant (k-kMinusStubs) bits, and largely random/incompressible. The small delta is the rest,
                // which can be efficiently encoded since it's usually very small.

                uint64_t stub = big_delta & ((1ULL << (k - kStubMinusBits)) - 1);
                uint64_t small_delta = big_delta >> (k - kStubMinusBits);

                // std::cout << "LP and last LP: " << (int)line_point << " ... " << (int)last_line_point << std::endl;
                // std::cout << "Big delta: " << big_delta << std::endl;
                // std::cout << "Small delta: " << small_delta << std::endl;

                assert(small_delta < 256);

                if ((index % kEntriesPerPark != 0)) {
                    park_deltas.push_back(small_delta);
                    park_stubs.push_back(stub);
                }
                last_line_point = line_point;
            }

            tmp_1_disks[table_index + 1].Write(right_writer, right_writer_buf,
                (right_writer_count%right_buf_entries)*right_entry_size_bytes);
            right_writer+=(right_writer_count%right_buf_entries)*right_entry_size_bytes;
            memset(right_writer_buf, 0x00, right_entry_size_bytes);
            tmp_1_disks[table_index + 1].Write(right_writer, right_writer_buf, right_entry_size_bytes);
            right_writer += right_entry_size_bytes;

            tmp_1_disks[table_index + 1].Truncate(right_writer);


            if (park_deltas.size() > 0) {
                // Since we don't have a perfect multiple of EPP entries, this writes the last ones
                WriteParkToFile(tmp2_disk, final_table_begin_pointers[table_index],
                                park_index, park_size_bytes, checkpoint_line_point, park_deltas,
                                park_stubs, k, table_index);
                final_entries_written += (park_stubs.size() + 1);
            }

            Encoding::ANSFree(kRValues[table_index - 1]);
            std::cout << "\tWrote " << final_entries_written << " entries" << std::endl;

            final_table_begin_pointers[table_index + 1] = final_table_begin_pointers[table_index]
                                                          + (park_index + 1) * park_size_bytes;

            final_table_writer=header_size - 8 * (10 - table_index);
            Util::IntToEightBytes(table_pointer_bytes, final_table_begin_pointers[table_index + 1]);
            tmp2_disk.Write(final_table_writer, (table_pointer_bytes), 8);
            final_table_writer+=8;

            computation_pass_2_timer.PrintElapsed("\tSecond computation pass time:");
            Timer sort_timer_2;
            std::cout << "\tRe-Sorting table " << int{table_index + 1} << std::endl;
            // This sort is needed so that in the next iteration, we can iterate through both tables
            // at ones. Note that sort_key represents y ordering, and the pos, offset coordinates from
            // forward/backprop represent positions in y ordered tables.
            Sorting::SortOnDisk(tmp_1_disks[table_index + 1], 0, tmp_1_disks[0],
                                right_entry_size_bytes, 0, new_bucket_sizes, memory, memorySize);

            sort_timer_2.PrintElapsed("\tSort time:");

            table_timer.PrintElapsed("Total compress table time:");
        }

        // These results will be used to write table P7 and the checkpoint tables in phase 4.
        return Phase3Results{final_table_begin_pointers, final_entries_written,
                             right_entry_size_bytes * 8, header_size};
    }

    // Writes the checkpoint tables. The purpose of these tables, is to store a list of ~2^k values
    // of size k (the proof of space outputs from table 7), in a way where they can be looked up for
    // proofs, but also efficiently. To do this, we assume table 7 is sorted by f7, and we write the
    // deltas between each f7 (which will be mostly 1s and 0s), with a variable encoding scheme (C3).
    // Furthermore, we create C1 checkpoints along the way.  For example, every 10,000 f7 entries,
    // we can have a C1 checkpoint, and a C3 delta encoded entry with 10,000 deltas.

    // Since we can't store all the checkpoints in
    // memory for large plots, we create checkpoints for the checkpoints (C2), that are meant to be
    // stored in memory during proving. For example, every 10,000 C1 entries, we can have a C2 entry.

    // The final table format for the checkpoints will be:
    // C1 (checkpoint values)
    // C2 (checkpoint values into)
    // C3 (deltas of f7s between C1 checkpoints)
    void WriteCTables(uint8_t k, uint8_t pos_size, FileDisk& tmp2_disk /*filename*/, std::vector<FileDisk>& tmp_1_disks /*plot_filename*/,
                      Phase3Results& res) {

        uint32_t P7_park_size = Util::ByteAlign((k+1) * kEntriesPerPark)/8;
        uint64_t number_of_p7_parks = ((res.final_entries_written == 0 ? 0 : res.final_entries_written - 1)
                                       / kEntriesPerPark) + 1;

        uint64_t begin_byte_C1 = res.final_table_begin_pointers[7] + number_of_p7_parks * P7_park_size;

        uint64_t total_C1_entries = CDIV(res.final_entries_written,
                                         kCheckpoint1Interval);
        uint64_t begin_byte_C2 = begin_byte_C1 + (total_C1_entries + 1) * (Util::ByteAlign(k) / 8);
        uint64_t total_C2_entries = CDIV(total_C1_entries, kCheckpoint2Interval);
        uint64_t begin_byte_C3 = begin_byte_C2 + (total_C2_entries + 1) * (Util::ByteAlign(k) / 8);

        uint32_t size_C3 = CalculateC3Size(k);
        uint64_t end_byte = begin_byte_C3 + (total_C1_entries) * size_C3;

        res.final_table_begin_pointers[8] = begin_byte_C1;
        res.final_table_begin_pointers[9] = begin_byte_C2;
        res.final_table_begin_pointers[10] = begin_byte_C3;
        res.final_table_begin_pointers[11] = end_byte;

        uint64_t plot_file_reader=0;
        uint64_t final_file_writer_1=begin_byte_C1;
        uint64_t final_file_writer_2=begin_byte_C3;
        uint64_t final_file_writer_3=res.final_table_begin_pointers[7];

        uint64_t prev_y = 0;
        std::vector<Bits> C2;
        uint64_t num_C1_entries = 0;
        vector<uint8_t> deltas_to_write;
        uint32_t right_entry_size_bytes = res.right_entry_size_bits / 8;

        uint8_t* right_entry_buf = new uint8_t[right_entry_size_bytes + 7];
        uint8_t* C1_entry_buf = new uint8_t[Util::ByteAlign(k) / 8];
        uint8_t* C3_entry_buf = new uint8_t[size_C3];
        uint8_t* P7_entry_buf = new uint8_t[P7_park_size];

        std::cout << "\tStarting to write C1 and C3 tables" << std::endl;

        ParkBits to_write_p7;

        // We read each table7 entry, which is sorted by f7, but we don't need f7 anymore. Instead,
        // we will just store pos6, and the deltas in table C3, and checkpoints in tables C1 and C2.
        for (uint64_t f7_position = 0; f7_position < res.final_entries_written; f7_position++) {
            tmp_1_disks[7].Read(plot_file_reader, (right_entry_buf),
                                  right_entry_size_bytes);
            plot_file_reader+=right_entry_size_bytes;
            uint64_t entry_y = Util::SliceInt64FromBytes(right_entry_buf, 0, k);
            uint64_t entry_new_pos = Util::SliceInt64FromBytes(right_entry_buf, k, pos_size);

            Bits entry_y_bits = Bits(entry_y, k);

            if (f7_position % kEntriesPerPark == 0 && f7_position > 0) {
                memset(P7_entry_buf, 0, P7_park_size);
                to_write_p7.ToBytes(P7_entry_buf);
                tmp2_disk.Write(final_file_writer_3, (P7_entry_buf), P7_park_size);
                final_file_writer_3+=P7_park_size;
                to_write_p7 = ParkBits();
            }

            to_write_p7 += ParkBits(entry_new_pos, k+1);

            if (f7_position % kCheckpoint1Interval == 0) {
                entry_y_bits.ToBytes(C1_entry_buf);
                tmp2_disk.Write(final_file_writer_1, (C1_entry_buf),
                                          Util::ByteAlign(k) / 8);
                final_file_writer_1+= Util::ByteAlign(k) / 8;
                if (num_C1_entries > 0) {
                    final_file_writer_2=begin_byte_C3 + (num_C1_entries - 1) * size_C3;
                    size_t num_bytes = Encoding::ANSEncodeDeltas(deltas_to_write, kC3R, C3_entry_buf + 2) + 2;

                    // We need to be careful because deltas are variable sized, and they need to fit
                    assert(size_C3 * 8 > num_bytes);

                    // Write the size
                    Util::IntToTwoBytes(C3_entry_buf, num_bytes - 2);

                    tmp2_disk.Write(final_file_writer_2, (C3_entry_buf), num_bytes);
                    final_file_writer_2+=num_bytes;
                }
                prev_y = entry_y;
                if (f7_position % (kCheckpoint1Interval * kCheckpoint2Interval) == 0) {
                    C2.emplace_back(std::move(entry_y_bits));
                }
                deltas_to_write.clear();
                ++num_C1_entries;
            } else {
                if (entry_y == prev_y) {
                    deltas_to_write.push_back(0);
                } else {
                    deltas_to_write.push_back(entry_y - prev_y);
                }
                prev_y = entry_y;
            }
        }
        Encoding::ANSFree(kC3R);


        // Writes the final park to disk
        memset(P7_entry_buf, 0, P7_park_size);
        to_write_p7.ToBytes(P7_entry_buf);

        tmp2_disk.Write(final_file_writer_3, (P7_entry_buf), P7_park_size);
        final_file_writer_3+=P7_park_size;

        if (deltas_to_write.size() != 0) {
            size_t num_bytes = Encoding::ANSEncodeDeltas(deltas_to_write, kC3R, C3_entry_buf + 2);
            memset(C3_entry_buf + num_bytes + 2, 0, size_C3 - (num_bytes + 2));
            final_file_writer_2=begin_byte_C3 + (num_C1_entries - 1) * size_C3;

            // Write the size
            Util::IntToTwoBytes(C3_entry_buf, num_bytes);

            tmp2_disk.Write(final_file_writer_2, (C3_entry_buf), size_C3);
            final_file_writer_2+=size_C3;
            Encoding::ANSFree(kC3R);
        }

        Bits(0, Util::ByteAlign(k)).ToBytes(C1_entry_buf);
        tmp2_disk.Write(final_file_writer_1, (C1_entry_buf),
                                  Util::ByteAlign(k)/8);
        final_file_writer_1+=Util::ByteAlign(k)/8;
        std::cout << "\tFinished writing C1 and C3 tables" << std::endl;
        std::cout << "\tWriting C2 table" << std::endl;

        for (Bits& C2_entry : C2) {
            C2_entry.ToBytes(C1_entry_buf);
            tmp2_disk.Write(final_file_writer_1, (C1_entry_buf),
                                      Util::ByteAlign(k)/8);
        final_file_writer_1+=Util::ByteAlign(k)/8;
        }
        Bits(0, Util::ByteAlign(k)).ToBytes(C1_entry_buf);
        tmp2_disk.Write(final_file_writer_1, (C1_entry_buf),
                                  Util::ByteAlign(k)/8);
        final_file_writer_1+=Util::ByteAlign(k)/8;
        std::cout << "\tFinished writing C2 table" << std::endl;

        delete[] C3_entry_buf;
        delete[] C1_entry_buf;
        delete[] P7_entry_buf;
        delete[] right_entry_buf;

        final_file_writer_1=res.header_size - 8 * 3;
        uint8_t table_pointer_bytes[8];

        // Writes the pointers to the start of the tables, for proving
        for (int i = 8; i <= 10; i++) {
            Util::IntToEightBytes(table_pointer_bytes, res.final_table_begin_pointers[i]);
            tmp2_disk.Write(final_file_writer_1, table_pointer_bytes, 8);
            final_file_writer_1 += 8;
        }

        std::cout << "\tFinal table pointers:" << std::endl << std::hex;

        for (int i = 1; i <= 10; i++) {
            std::cout << "\t" << (i < 8 ? "P" : "C") << (i < 8 ? i : i - 7);
            std::cout << ": 0x" << res.final_table_begin_pointers[i] << std::endl;
        }
        std::cout << std::dec;
    }
};

#endif  // SRC_CPP_PLOTTER_DISK_HPP_
