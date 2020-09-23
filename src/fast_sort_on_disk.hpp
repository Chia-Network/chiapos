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

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

// Gulrak filesystem brings in Windows headers that cause some issues with std
#define _HAS_STD_BYTE 0
#define NOMINMAX

#include "../lib/include/filesystem.hh"

namespace fs = ghc::filesystem;

#include "./bits.hpp"
#include "./disk.hpp"
#include "./util.hpp"
#include "./uniformsort.hpp"
#include "./quicksort.hpp"
#include "sort_on_disk.hpp"

class LazySortManager {
public:
    LazySortManager(
            uint8_t *memory,
            uint64_t memory_size,
            uint32_t num_buckets,
            uint32_t log_num_buckets,
            uint16_t entry_size,
            const std::string &tmp_dirname,
            const std::string &filename,
            Disk *output_file,
            uint32_t begin_bits) {
        this->memory_start = memory;
        this->memory_size = memory_size;
        this->output_file = output_file;
        this->size_per_bucket = memory_size / num_buckets;
        this->log_num_buckets = log_num_buckets;
        this->entry_size = entry_size;
        this->begin_bits = begin_bits;
        this->done = false;
        // Cross platform way to concatenate paths, gulrak library.
        std::vector<fs::path> bucket_filenames = std::vector<fs::path>();

        for (size_t bucket_i = 0; bucket_i < num_buckets; bucket_i++) {
            this->mem_bucket_pointers.push_back(memory + bucket_i * size_per_bucket);
            this->mem_bucket_sizes.push_back(0);
            this->bucket_write_pointers.push_back(0);
            fs::path bucket_filename =
                    fs::path(tmp_dirname) /
                    fs::path(filename + ".sort_bucket_" + to_string(bucket_i) + ".tmp");
            fs::remove(bucket_filename);
            this->bucket_files.emplace_back(FileDisk(bucket_filename));
        }
        this->final_position_start = 0;
        this->final_position_end = 0;
        this->next_bucket_to_sort = 0;
        this->output_file_written = 0;
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

        uint8_t *mem_write_pointer = mem_bucket_pointers[bucket_index] + mem_write_offset;
        assert(Util::ByteAlign(entry.GetSize()) / 8 == this->entry_size);
        entry.ToBytes(mem_write_pointer);
        mem_bucket_sizes[bucket_index] += 1;
    }

    inline void Read(uint64_t position, uint8_t *buffer, uint64_t num_bytes_to_copy, bool quicksort = 0) {
        if (position < this->final_position_start) {
            throw std::string("Invalid read position.");
        }
        while (position >= this->final_position_end) {
            SortBucket(quicksort);
        }
        assert(this->final_position_end > position);
        assert(this->final_position_start >= position);

        uint64_t bytes_copied = 0;
        do {
            uint64_t bytes_to_copy = std::min(num_bytes_to_copy, this->final_position_end - position);
            memcpy(buffer, this->memory_start + (position - this->final_position_start), bytes_to_copy);
            bytes_copied += bytes_to_copy;
            if (bytes_copied < num_bytes_to_copy) {
                SortBucket(quicksort);
            }
        } while (bytes_copied < num_bytes_to_copy);
        assert (bytes_copied == num_bytes_to_copy);
    }

    inline void SortBucket(bool quicksort) {
        if (next_bucket_to_sort >= this->mem_bucket_pointers.size()) {
            throw std::string("Trying to sort bucket which does not exist.");
        }

        uint64_t bucket_i = this->next_bucket_to_sort;
        uint64_t bucket_entries = bucket_write_pointers[bucket_i] / this->entry_size;
        uint64_t entries_fit_in_memory = this->memory_size / this->entry_size;

        uint32_t entry_len_memory = this->entry_size - this->begin_bits / 8;

        std::cout << "\t\tFor quicksort, need " +
                     to_string(entry_size * bucket_entries / (1024.0 * 1024.0 * 1024.0)) + "GiB."
                  << std::endl;
        std::cout << "\t\tFor the fast uniform sort, need " +
                     to_string(
                             Util::RoundSize(bucket_entries) * entry_len_memory /
                             (1024.0 * 1024.0 * 1024.0)) +
                     "GiB."
                  << std::endl;
        std::cout << "\t\tHave "
                  << to_string(entry_size * entries_fit_in_memory / (1024.0 * 1024.0 * 1024.0)) + "GiB";

        if (bucket_entries > entries_fit_in_memory) {
            std::cout << "Not enough memory for sort in memory. Need to sort " +
                         to_string(
                                 this->bucket_write_pointers[bucket_i] /
                                 (1024.0 * 1024.0 * 1024.0)) +
                         "GiB"
                      << std::endl;
            exit(1);
        }

        // Do SortInMemory algorithm if it fits in the memory
        // (number of entries required * entry_len_memory) <= total memory available
        if (quicksort == 0 && Util::RoundSize(bucket_entries) * entry_len_memory <= this->memory_size) {
            std::cout << "\t\tDoing uniform sort" << std::endl;
            UniformSort::SortToMemory(
                    this->bucket_files[bucket_i],
                    0,
                    memory_start,
                    this->entry_size,
                    bucket_entries,
                    this->begin_bits + this->log_num_buckets);
        } else {
            // Are we in Compress phrase 1 (quicksort=1) or is it the last bucket (quicksort=2)? Perform
            // quicksort if so (SortInMemory algorithm won't always perform well), or if we don't have enough memory for
            // uniform sort
            std::cout << "\t\tDoing QS" << std::endl;
            this->bucket_files[bucket_i].Read(0, this->memory_start,
                                              bucket_entries * this->entry_size);
            QuickSort::Sort(this->memory_start, this->entry_size, bucket_entries, this->begin_bits);

        }

        // Deletes the bucket file
        this->bucket_files[bucket_i].Close();
        fs::remove(fs::path(this->bucket_files[bucket_i].GetFileName()));

        this->final_position_start = this->output_file_written;
        this->output_file_written += this->bucket_write_pointers[bucket_i];
        this->final_position_end = this->output_file_written;
        this->next_bucket_to_sort++;
    }

    inline void ChangeMemory(uint8_t *new_memory, uint64_t new_memory_size) {
        this->memory_start = new_memory;
        this->memory_size = new_memory_size;
    }

    inline uint64_t GetBytesWritten() {
        return this->output_file_written;
    }

    inline void AssertAllWritten() {
        if (!(this->next_bucket_to_sort == this->mem_bucket_pointers.size())) {
            throw std::string("Did not finish sorting and writing entries.");
        }
        uint64_t bytes_written_sum = 0;
        for (auto s : this->bucket_write_pointers) {
            bytes_written_sum += s;
        }
        if (!(bytes_written_sum == this->output_file_written)) {
            throw std::string("Did not write the correct number of entries");
        }
    }

    inline void FlushCache() {
        for (size_t bucket_i = 0; bucket_i < this->mem_bucket_pointers.size(); bucket_i++) {
            uint64_t start_write = this->bucket_write_pointers[bucket_i];
            uint64_t write_len = this->mem_bucket_sizes[bucket_i] * this->entry_size;

            // Flush each bucket to disk
            bucket_files[bucket_i].Write(
                    start_write, this->mem_bucket_pointers[bucket_i], write_len);
            this->bucket_write_pointers[bucket_i] += write_len;

            // Reset memory caches
            this->mem_bucket_pointers[bucket_i] =
                    this->memory_start + bucket_i * this->size_per_bucket;
            this->mem_bucket_sizes[bucket_i] = 0;
        }
    }

    ~LazySortManager() {
        // Close and delete files in case we exit without doing the sort
        if (!this->done) {
            for (auto &fd : this->bucket_files) {
                fd.Close();
                fs::remove(fs::path(fd.GetFileName()));
            }
            this->done = true;
        }
    }

 private:
    inline static uint64_t ExtractNum(Bits &bytes, uint32_t begin_bits, uint32_t take_bits)
    {
        return (uint64_t)(bytes.Slice(begin_bits, begin_bits + take_bits).GetValue128());
    }

    // Start of the whole memory array. This will be diveded into buckets
    uint8_t *memory_start;
    // Size of the whole memory array
    uint64_t memory_size;
    // One file for each bucket
    std::vector<FileDisk> bucket_files;
    // One output file for the result of the sort
    Disk *output_file;
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

    bool done;

    uint64_t final_position_start;
    uint64_t final_position_end;
    uint64_t next_bucket_to_sort;
    uint64_t output_file_written;
};


class SortManager {
public:
    SortManager(
            uint8_t *memory,
            uint64_t memory_size,
            uint32_t num_buckets,
            uint32_t log_num_buckets,
            uint16_t entry_size,
            const std::string &tmp_dirname,
            const std::string &filename,
            Disk *output_file,
            Disk *spare,
            uint32_t begin_bits)
    {
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
                    fs::path(tmp_dirname) /
                    fs::path(filename + ".sort_bucket_" + to_string(bucket_i) + ".tmp");
            fs::remove(bucket_filename);
            this->bucket_files.emplace_back(FileDisk(bucket_filename));
        }
    }

    inline void AddToCache(Bits &entry)
    {
        if (this->done) {
            throw std::string("Already finished.");
        }
        uint64_t bucket_index = ExtractNum(entry, this->begin_bits, this->log_num_buckets);
        uint64_t mem_write_offset = mem_bucket_sizes[bucket_index] * entry_size;
        if (mem_write_offset + entry_size > this->size_per_bucket) {
            this->FlushCache();
            mem_write_offset = 0;
        }

        uint64_t sub_bucket_index =
                ExtractNum(entry, this->begin_bits + this->log_num_buckets, this->log_num_buckets);

        this->sub_bucket_sizes[bucket_index][sub_bucket_index] += 1;

        uint8_t *mem_write_pointer = mem_bucket_pointers[bucket_index] + mem_write_offset;
        assert(Util::ByteAlign(entry.GetSize()) / 8 == this->entry_size);
        entry.ToBytes(mem_write_pointer);
        mem_bucket_sizes[bucket_index] += 1;
    }

    inline uint64_t ExecuteSort(uint8_t *sort_memory, uint64_t memory_len, bool quicksort = 0)
    {
        if (this->done) {
            throw std::string("Already finished.");
        }
        this->done = true;
        this->FlushCache();
        uint64_t output_file_written = 0;

        for (size_t bucket_i = 0; bucket_i < this->mem_bucket_pointers.size(); bucket_i++) {
            // Reads an entire bucket into memory
            std::cout << "Total bytes reading into memory: "
                      << to_string(
                              this->bucket_write_pointers[bucket_i] / (1024.0 * 1024.0 * 1024.0))
                      << " Mem size " << to_string(this->memory_size / (1024.0 * 1024.0 * 1024.0))
                      << std::endl;
            if (this->bucket_write_pointers[bucket_i] > this->memory_size) {
                std::cout << "Not enough memory for sort in memory. Need to sort " +
                             to_string(
                                     this->bucket_write_pointers[bucket_i] /
                                     (1024.0 * 1024.0 * 1024.0)) +
                             "GiB"
                          << std::endl;
            }

            // This actually sorts in memory if the entire data fits in our memory buffer (passes
            // the above check)
            Sorting::SortOnDisk(
                    this->bucket_files[bucket_i],
                    *this->output_file,
                    0,
                    output_file_written,
                    *this->spare,
                    this->entry_size,
                    this->begin_bits + this->log_num_buckets,
                    this->sub_bucket_sizes[bucket_i],
                    sort_memory,
                    memory_len,
                    quicksort);
            std::cout << "FInished executing sort" << std::endl;

            // Deletes the bucket file
            this->bucket_files[bucket_i].Close();
            fs::remove(fs::path(this->bucket_files[bucket_i].GetFileName()));

            output_file_written += this->bucket_write_pointers[bucket_i];
        }
        return output_file_written;
    }

    ~SortManager() {
        // Close and delete files in case we exit without doing the sort
        if (!this->done) {
            for (auto &fd : this->bucket_files) {
                fd.Close();
                fs::remove(fs::path(fd.GetFileName()));
            }
        }
    }

private:
    inline void FlushCache()
    {
        for (size_t bucket_i = 0; bucket_i < this->mem_bucket_pointers.size(); bucket_i++) {
            uint64_t start_write = this->bucket_write_pointers[bucket_i];
            uint64_t write_len = this->mem_bucket_sizes[bucket_i] * this->entry_size;

            // Flush each bucket to disk
            bucket_files[bucket_i].Write(
                    start_write, this->mem_bucket_pointers[bucket_i], write_len);
            this->bucket_write_pointers[bucket_i] += write_len;

            // Reset memory caches
            this->mem_bucket_pointers[bucket_i] =
                    this->memory_start + bucket_i * this->size_per_bucket;
            this->mem_bucket_sizes[bucket_i] = 0;
        }
    }

    inline static uint64_t ExtractNum(Bits &bytes, uint32_t begin_bits, uint32_t take_bits)
    {
        return (uint64_t)(bytes.Slice(begin_bits, begin_bits + take_bits).GetValue128());
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
