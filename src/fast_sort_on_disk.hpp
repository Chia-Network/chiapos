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

#ifndef SRC_CPP_FAST_SORT_ON_DISK_HPP_
#define SRC_CPP_FAST_SORT_ON_DISK_HPP_

#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>

// Gulrak filesystem brings in Windows headers that cause some issues with std
#define _HAS_STD_BYTE 0
#define NOMINMAX

#include "../lib/include/filesystem.hh"

namespace fs = ghc::filesystem;

#include "./util.hpp"
#include "./bits.hpp"
#include "./disk.hpp"
#include "./sort_on_disk.hpp"


class SortManager {
public:
    SortManager(uint8_t *memory, uint64_t memory_size, uint32_t num_buckets, uint32_t log_num_buckets,
                uint16_t entry_size, const std::string &tmp_dirname, const std::string &filename, Disk *output_file,
                Disk *spare, uint32_t begin_bits) {
        this->memory_start = memory;
        this->memory_size = memory_size;
        this->output_file = output_file;
        this->spare = spare;
        this->size_per_bucket = memory_size / num_buckets;
        this->log_num_buckets = log_num_buckets;
        this->entry_size = entry_size;
        this->begin_bits = begin_bits;
        this->done = false;
        // Cross platform way to concatenate paths, gulrak library.
        std::vector<fs::path> bucket_filenames = std::vector<fs::path>();
        this->sub_bucket_sizes = std::vector<std::vector<uint64_t>>();

        for (size_t bucket_i = 0; bucket_i < num_buckets; bucket_i++) {
            this->mem_bucket_pointers.push_back(memory + bucket_i * size_per_bucket);
            this->mem_bucket_sizes.push_back(0);
            this->bucket_write_pointers.push_back(0);
            this->sub_bucket_sizes.emplace_back(std::vector<uint64_t>(num_buckets, 0));
            fs::path bucket_filename =
                    fs::path(tmp_dirname) / fs::path(filename + ".sort_bucket_" + to_string(bucket_i) + ".tmp");
            fs::remove(bucket_filename);
            this->bucket_files.emplace_back(FileDisk(bucket_filename));
        }
    }

    inline void AddToCache(Bits &entry) {
        if (this->done) {
            throw std::string("Already finished.");
        }
        uint64_t bucket_index = ExtractNum(entry, this->begin_bits, this->log_num_buckets);
        uint64_t mem_write_offset = mem_bucket_sizes[bucket_index] * entry_size;
        if (mem_write_offset + entry_size > this->size_per_bucket) {
            this->FlushCache();
            mem_write_offset = 0;
        }

        uint64_t sub_bucket_index = ExtractNum(entry, this->begin_bits + this->log_num_buckets, this->log_num_buckets);

        this->sub_bucket_sizes[bucket_index][sub_bucket_index] += 1;

        uint8_t *mem_write_pointer = mem_bucket_pointers[bucket_index] + mem_write_offset;
        assert(Util::ByteAlign(entry.GetSize()) / 8 == this->entry_size);
        entry.ToBytes(mem_write_pointer);
        mem_bucket_sizes[bucket_index] += 1;
    }

    inline uint64_t ExecuteSort(uint8_t *sort_memory, uint64_t memory_len, bool quicksort = 0) {
        if (this->done) {
            throw std::string("Already finished.");
        }
        this->done = true;
        this->FlushCache();
        uint64_t output_file_written = 0;

        for (size_t bucket_i = 0; bucket_i < this->mem_bucket_pointers.size(); bucket_i++) {
            // Reads an entire bucket into memory
            std::cout << "Total bytes reading into memory: "
                      << to_string(this->bucket_write_pointers[bucket_i] / (1024.0 * 1024.0 * 1024.0)) << " Mem size "
                      << to_string(this->memory_size / (1024.0 * 1024.0 * 1024.0)) << std::endl;
            if (this->bucket_write_pointers[bucket_i] > this->memory_size) {
                std::cout << "Not enough memory for sort in memory. Need to sort " +
                             to_string(this->bucket_write_pointers[bucket_i] / (1024.0 * 1024.0 * 1024.0)) + "GiB"
                          << std::endl;
            }

            // This actually sorts in memory if the entire data fits in our memory buffer (passes the above check)
            Sorting::SortOnDisk(this->bucket_files[bucket_i],
                                *this->output_file,
                                0,
                                output_file_written,
                                *this->spare,
                                this->entry_size,
                                this->begin_bits + this->log_num_buckets,
                                this->sub_bucket_sizes[bucket_i],
                                sort_memory, memory_len, quicksort);

            // Deletes the bucket file
            fs::remove(fs::path(this->bucket_files[bucket_i].GetFileName()));

            output_file_written += this->bucket_write_pointers[bucket_i];
        }
        Close();
        return output_file_written;
    }

    void Close() {
        // for (FileDisk& fd : this->bucket_files) {
        //     fd.Close();
        // }
        for (auto &fd : this->bucket_files) {
            fs::remove(fs::path(fd.GetFileName()));
        }
    }

    ~SortManager() {
        Close();
    }

private:
    inline void FlushCache() {
        for (size_t bucket_i = 0; bucket_i < this->mem_bucket_pointers.size(); bucket_i++) {
            uint64_t start_write = this->bucket_write_pointers[bucket_i];
            uint64_t write_len = this->mem_bucket_sizes[bucket_i] * this->entry_size;

            // Flush each bucket to disk
            bucket_files[bucket_i].Write(start_write, this->mem_bucket_pointers[bucket_i], write_len);
            this->bucket_write_pointers[bucket_i] += write_len;

            // Reset memory caches
            this->mem_bucket_pointers[bucket_i] = this->memory_start + bucket_i * this->size_per_bucket;
            this->mem_bucket_sizes[bucket_i] = 0;
        }
    }

    inline static uint64_t ExtractNum(Bits &bytes, uint32_t begin_bits, uint32_t take_bits) {
        return (uint64_t) (bytes.Slice(begin_bits, begin_bits + take_bits).GetValue128());
    }

    // Start of the whole memory array. This will be diveded into buckets
    uint8_t *memory_start;
    // Size of the whole memory array
    uint64_t memory_size;
    // One file for each bucket
    std::vector<FileDisk> bucket_files;
    // One output file for the result of the sort
    Disk *output_file;
    // One spare file for sorting
    Disk *spare;
    // Size of each entry
    uint16_t entry_size;
    // Bucket determined by the first "log_num_buckets" bits starting at "begin_bits"
    uint32_t begin_bits;
    // Portion of memory to allocate to each bucket
    uint64_t size_per_bucket;
    // Log of the number of buckets; num bits to use to determine bucket
    uint32_t log_num_buckets;
    // One pointer to the start of each bucket memory
    vector<uint8_t *> mem_bucket_pointers;
    // The number of entries written to each bucket
    vector<uint64_t> mem_bucket_sizes;
    // The amount of data written to each disk bucket
    vector<uint64_t> bucket_write_pointers;

    // When doing recursive sort on disk, we need to subdivide each bucket into subbuckets
    vector<vector<uint64_t>> sub_bucket_sizes;
    bool done;
};

#endif  // SRC_CPP_FAST_SORT_ON_DISK_HPP_
