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

#ifndef SRC_CPP_B17SORTMANAGER_HPP_
#define SRC_CPP_B17SORTMANAGER_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "chia_filesystem.hpp"

#include "./bits.hpp"
#include "./calculate_bucket.hpp"
#include "./disk.hpp"
#include "./quicksort.hpp"
#include "./uniformsort.hpp"
#include "exceptions.hpp"

class b17SortManager {
public:
    b17SortManager(
        uint8_t *memory,
        uint64_t memory_size,
        uint32_t num_buckets,
        uint32_t log_num_buckets,
        uint16_t entry_size,
        const std::string &tmp_dirname,
        const std::string &filename,
        uint32_t begin_bits,
        uint64_t stripe_size)
    {
        this->memory_start = memory;
        this->memory_size = memory_size;
        this->size_per_bucket = memory_size / num_buckets;
        this->log_num_buckets = log_num_buckets;
        this->entry_size = entry_size;
        this->begin_bits = begin_bits;
        this->done = false;
        this->prev_bucket_buf_size =
            2 * (stripe_size + 10 * (kBC / pow(2, kExtraBits))) * entry_size;
        this->prev_bucket_buf = new uint8_t[this->prev_bucket_buf_size]();
        this->prev_bucket_position_start = 0;
        // Cross platform way to concatenate paths, gulrak library.
        std::vector<fs::path> bucket_filenames = std::vector<fs::path>();

        for (size_t bucket_i = 0; bucket_i < num_buckets; bucket_i++) {
            this->mem_bucket_pointers.push_back(memory + bucket_i * size_per_bucket);
            this->mem_bucket_sizes.push_back(0);
            this->bucket_write_pointers.push_back(0);
            std::ostringstream bucket_number_padded;
            bucket_number_padded << std::internal << std::setfill('0') << std::setw(3) << bucket_i;

            fs::path bucket_filename =
                fs::path(tmp_dirname) /
                fs::path(filename + ".sort_bucket_" + bucket_number_padded.str() + ".tmp");
            fs::remove(bucket_filename);
            this->bucket_files.push_back(FileDisk(bucket_filename));
        }
        this->final_position_start = 0;
        this->final_position_end = 0;
        this->next_bucket_to_sort = 0;
        this->entry_buf = new uint8_t[entry_size + 7]();
    }

    inline void AddToCache(const Bits &entry)
    {
        entry.ToBytes(this->entry_buf);
        return AddToCache(this->entry_buf);
    }
    inline void AddToCache(const uint8_t *entry)
    {
        if (this->done) {
            throw InvalidValueException("Already finished.");
        }
        uint64_t bucket_index =
            Util::ExtractNum(entry, this->entry_size, this->begin_bits, this->log_num_buckets);
        uint64_t mem_write_offset = mem_bucket_sizes[bucket_index] * entry_size;
        if (mem_write_offset + entry_size > this->size_per_bucket) {
            this->FlushTable(bucket_index);
            mem_write_offset = 0;
        }

        uint8_t *mem_write_pointer = mem_bucket_pointers[bucket_index] + mem_write_offset;
        memcpy(mem_write_pointer, entry, this->entry_size);
        mem_bucket_sizes[bucket_index] += 1;
    }

    inline uint8_t *ReadEntry(uint64_t position, int quicksort = 0)
    {
        if (position < this->final_position_start) {
            if (!(position >= this->prev_bucket_position_start)) {
                throw InvalidStateException("Invalid prev bucket start");
            }
            return (this->prev_bucket_buf + (position - this->prev_bucket_position_start));
        }

        while (position >= this->final_position_end) {
            SortBucket(quicksort);
        }
        if (!(this->final_position_end > position)) {
            throw InvalidValueException("Position too large");
        }
        if (!(this->final_position_start <= position)) {
            throw InvalidValueException("Position too small");
        }
        return this->memory_start + (position - this->final_position_start);
    }

    inline bool CloseToNewBucket(uint64_t position) const
    {
        if (!(position <= this->final_position_end)) {
            return this->next_bucket_to_sort < this->mem_bucket_pointers.size();
        };
        return (
            position + prev_bucket_buf_size / 2 >= this->final_position_end &&
            this->next_bucket_to_sort < this->mem_bucket_pointers.size());
    }

    inline void TriggerNewBucket(uint64_t position, bool quicksort)
    {
        if (!(position <= this->final_position_end)) {
            throw InvalidValueException("Triggering bucket too late");
        }
        if (!(position >= this->final_position_start)) {
            throw InvalidValueException("Triggering bucket too early");
        }

        // position is the first position that we need in the new array
        uint64_t cache_size = (this->final_position_end - position);
        memset(this->prev_bucket_buf, 0x00, this->prev_bucket_buf_size);
        memcpy(
            this->prev_bucket_buf,
            this->memory_start + position - this->final_position_start,
            cache_size);
        SortBucket(quicksort);
        this->prev_bucket_position_start = position;
    }

    inline void ChangeMemory(uint8_t *new_memory, uint64_t new_memory_size)
    {
        this->FlushCache();
        this->memory_start = new_memory;
        this->memory_size = new_memory_size;
        this->size_per_bucket = new_memory_size / this->mem_bucket_pointers.size();
        for (size_t bucket_i = 0; bucket_i < this->mem_bucket_pointers.size(); bucket_i++) {
            this->mem_bucket_pointers[bucket_i] = (new_memory + bucket_i * size_per_bucket);
        }
        this->final_position_start = 0;
        this->final_position_end = 0;
        this->next_bucket_to_sort = 0;
    }

    inline void FlushCache()
    {
        for (size_t bucket_i = 0; bucket_i < this->mem_bucket_pointers.size(); bucket_i++) {
            FlushTable(bucket_i);
        }
    }

    ~b17SortManager()
    {
        // Close and delete files in case we exit without doing the sort
        for (auto &fd : this->bucket_files) {
            std::string filename = fd.GetFileName();
            fd.Close();
            fs::remove(fs::path(fd.GetFileName()));
        }
        delete[] this->prev_bucket_buf;
        delete[] this->entry_buf;
    }

private:
    // Start of the whole memory array. This will be diveded into buckets
    uint8_t *memory_start;
    // Size of the whole memory array
    uint64_t memory_size;
    // One file for each bucket
    std::vector<FileDisk> bucket_files;
    // Size of each entry
    uint16_t entry_size;
    // Bucket determined by the first "log_num_buckets" bits starting at "begin_bits"
    uint32_t begin_bits;
    // Portion of memory to allocate to each bucket
    uint64_t size_per_bucket;
    // Log of the number of buckets; num bits to use to determine bucket
    uint32_t log_num_buckets;
    // One pointer to the start of each bucket memory
    std::vector<uint8_t *> mem_bucket_pointers;
    // The number of entries written to each bucket
    std::vector<uint64_t> mem_bucket_sizes;
    // The amount of data written to each disk bucket
    std::vector<uint64_t> bucket_write_pointers;
    uint64_t prev_bucket_buf_size;
    uint8_t *prev_bucket_buf;
    uint64_t prev_bucket_position_start;

    bool done;

    uint64_t final_position_start;
    uint64_t final_position_end;
    uint64_t next_bucket_to_sort;
    uint8_t *entry_buf;

    inline void FlushTable(uint16_t bucket_i)
    {
        uint64_t start_write = this->bucket_write_pointers[bucket_i];
        uint64_t write_len = this->mem_bucket_sizes[bucket_i] * this->entry_size;

        // Flush the bucket to disk
        bucket_files[bucket_i].Write(start_write, this->mem_bucket_pointers[bucket_i], write_len);
        this->bucket_write_pointers[bucket_i] += write_len;

        // Reset memory caches
        this->mem_bucket_pointers[bucket_i] = this->memory_start + bucket_i * this->size_per_bucket;
        this->mem_bucket_sizes[bucket_i] = 0;
    }

    inline void SortBucket(int quicksort)
    {
        this->done = true;
        if (next_bucket_to_sort >= this->mem_bucket_pointers.size()) {
            throw InvalidValueException("Trying to sort bucket which does not exist.");
        }
        uint64_t bucket_i = this->next_bucket_to_sort;
        uint64_t bucket_entries = bucket_write_pointers[bucket_i] / this->entry_size;
        uint64_t entries_fit_in_memory = this->memory_size / this->entry_size;

        uint32_t entry_len_memory = this->entry_size - this->begin_bits / 8;

        double have_ram = entry_size * entries_fit_in_memory / (1024.0 * 1024.0 * 1024.0);
        double qs_ram = entry_size * bucket_entries / (1024.0 * 1024.0 * 1024.0);
        double u_ram =
            Util::RoundSize(bucket_entries) * entry_len_memory / (1024.0 * 1024.0 * 1024.0);

        if (bucket_entries > entries_fit_in_memory) {
            throw InsufficientMemoryException(
                "Not enough memory for sort in memory. Need to sort " +
                std::to_string(this->bucket_write_pointers[bucket_i] / (1024.0 * 1024.0 * 1024.0)) +
                "GiB");
        }
        bool last_bucket = (bucket_i == this->mem_bucket_pointers.size() - 1) ||
                           this->bucket_write_pointers[bucket_i + 1] == 0;
        bool force_quicksort = (quicksort == 1) || (quicksort == 2 && last_bucket);
        // Do SortInMemory algorithm if it fits in the memory
        // (number of entries required * entry_len_memory) <= total memory available
        if (!force_quicksort &&
            Util::RoundSize(bucket_entries) * entry_len_memory <= this->memory_size) {
            std::cout << "\tBucket " << bucket_i << " uniform sort. Ram: " << std::fixed
                      << std::setprecision(3) << have_ram << "GiB, u_sort min: " << u_ram
                      << "GiB, qs min: " << qs_ram << "GiB." << std::endl;
            UniformSort::SortToMemory(
                this->bucket_files[bucket_i],
                0,
                memory_start,
                this->entry_size,
                bucket_entries,
                this->begin_bits + this->log_num_buckets);
        } else {
            // Are we in Compress phrase 1 (quicksort=1) or is it the last bucket (quicksort=2)?
            // Perform quicksort if so (SortInMemory algorithm won't always perform well), or if we
            // don't have enough memory for uniform sort
            std::cout << "\tBucket " << bucket_i << " QS. Ram: " << std::fixed
                      << std::setprecision(3) << have_ram << "GiB, u_sort min: " << u_ram
                      << "GiB, qs min: " << qs_ram << "GiB. force_qs: " << force_quicksort
                      << std::endl;
            this->bucket_files[bucket_i].Read(
                0, this->memory_start, bucket_entries * this->entry_size);
            QuickSort::Sort(this->memory_start, this->entry_size, bucket_entries, this->begin_bits);
        }

        // Deletes the bucket file
        std::string filename = this->bucket_files[bucket_i].GetFileName();
        this->bucket_files[bucket_i].Close();
        fs::remove(fs::path(filename));

        this->final_position_start = this->final_position_end;
        this->final_position_end += this->bucket_write_pointers[bucket_i];
        this->next_bucket_to_sort += 1;
    }
};

#endif  // SRC_CPP_FAST_SORT_ON_DISK_HPP_
