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

#ifndef SRC_CPP_PHASE2_HPP_
#define SRC_CPP_PHASE2_HPP_
#include <vector>
#include <algorithm> //std::copy
#include <thread>
#include <atomic>
#include <chrono> // this_thread::sleep_for
#include <memory> // std::make_shared

#include "disk.hpp"
#include "entry_sizes.hpp"
#include "sort_manager.hpp"
#include "bitfield.hpp"
#include "bitfield_index.hpp"
#include "progress.hpp"
#include "concurrent_queue.hpp"

struct Phase2Results
{
    Disk& disk_for_table(int const table_index)
    {
        if (table_index == 1) return table1;
        else if (table_index == 7) return table7;
        else return *output_files[table_index - 2];
    }
    FilteredDisk table1;
    BufferedDisk table7;
    std::vector<std::unique_ptr<SortManager>> output_files;
    std::vector<uint64_t> table_sizes;
};

struct SCANTHREADDATA
{
    int64_t const chunk;
    int64_t const read_index;
    std::shared_ptr<uint8_t> entry;
    SCANTHREADDATA(int64_t chunk_, int64_t read_index_, std::shared_ptr<uint8_t> entry_)
    : chunk(chunk_), read_index(read_index_), entry(entry_)
    {}
};

struct SORTTHREADDATA
{
    int64_t const chunk;
    int64_t const read_index;
    int64_t const write_counter;
    std::shared_ptr<uint8_t> entry;
    SORTTHREADDATA(int64_t chunk_, int64_t read_index_, int64_t write_counter_, std::shared_ptr<uint8_t> entry_)
    : chunk(chunk_), read_index(read_index_), write_counter(write_counter_), entry(entry_)
    {}
};

class exit_flag
{
    std::atomic<bool> exited_{false};
    public:
        void exit() { exited_.store(true, std::memory_order_release); }
        explicit operator bool() const noexcept { return exited_.load(std::memory_order_acquire); }
        bool operator !() const noexcept { return !static_cast<bool>(*this); }
};

void* ScanThread(bitfield* current_bitfield,
                 bitfield* next_bitfield,
                 uint8_t const k,
                 uint8_t const pos_offset_size,
                 int16_t const entry_size,
                 concurrent_queue<std::shared_ptr<SCANTHREADDATA> >* q,
                 exit_flag *exitflag,
                 std::mutex* nbm)
{
    while (true)
    {
        if (q->empty()) {
            if (*exitflag) {
                break;
            }else{
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
        }
        const auto od = q->pop();
        if(od == boost::none)
        {
            continue;
        }
        const auto d = od.get();
        std::shared_ptr<uint8_t> entry_chunk = d->entry;
        int64_t chunk = d->chunk;
        int64_t read_index = d->read_index;

        std::vector<uint64_t> entry_poses;
        std::vector<uint64_t> entry_offsets;
        entry_poses.reserve(chunk);
        entry_offsets.reserve(chunk);

        int64_t set_counter_of_this_thread = 0;
        for(int64_t r = 0; r < chunk; ++r){
            uint8_t const* entry = reinterpret_cast<uint8_t*>(entry_chunk.get()) + r*entry_size;
            uint64_t entry_pos_offset = 0;
            if (!current_bitfield->get(read_index+r))
            {
                // This entry should be dropped.
                continue;
            }
            entry_pos_offset = Util::SliceInt64FromBytes(entry, 0, pos_offset_size);

            uint64_t entry_pos = entry_pos_offset >> kOffsetSize;
            uint64_t entry_offset = entry_pos_offset & ((1U << kOffsetSize) - 1);
            entry_poses.emplace_back(entry_pos);
            entry_offsets.emplace_back(entry_offset);

            set_counter_of_this_thread++;
        }

        {
            std::lock_guard<std::mutex> lock(*nbm);
            for(int64_t r = 0; r < set_counter_of_this_thread; ++r){
            // mark the two matching entries as used (pos and pos+offset)
                next_bitfield->set(entry_poses[r]);
                next_bitfield->set(entry_poses[r] + entry_offsets[r]);
            }
        }
    }
    return 0;
}

void* SortThread(bitfield* current_bitfield,
                 bitfield_index const* index,
                 uint8_t const k,
                 uint8_t const pos_offset_size,
                 uint8_t const pos_offset_shift,
                 int16_t const entry_size,
                 uint8_t const write_counter_shift,
                 concurrent_queue<std::shared_ptr<SORTTHREADDATA> >* q,
                 exit_flag *exitflag,
                 SortManager* sort_manager,
                 std::mutex *smm)
{
    while(true)
    {
        if (q->empty()) {
            if (*exitflag) {
                break;
            }else{
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
        }
        const auto od = q->pop();
        if(od == boost::none)
        {
            continue;
        }
        const auto d = od.get();
        std::shared_ptr<uint8_t> entry_chunk = d->entry;
        int64_t chunk = d->chunk;
        int64_t read_index = d->read_index;
        int64_t write_counter = d->write_counter;

        auto chunk_ptr = new(std::nothrow) std::unique_ptr<uint8_t[]>[chunk];
        if (!chunk_ptr){
            std::cout << "chunkptr nullptr!" << std::endl;
            exit(1);
        }
        std::unique_ptr<std::unique_ptr<uint8_t[]>[]> bytes(chunk_ptr);

        for(int64_t r = 0; r < chunk; ++r){
            auto bytes_ptr = new(std::nothrow) uint8_t[16];
            if(!bytes_ptr){
                std::cout << "bytesptr nullptr!" << std::endl;
                exit(1);
            }
            bytes[r].reset(bytes_ptr);
        }

        int64_t writer_counter_of_this_thread = 0;
        for(int64_t r = 0; r < chunk; ++r){
            uint8_t const* entry = reinterpret_cast<uint8_t*>(entry_chunk.get()) + r*entry_size;
            uint64_t entry_pos_offset = Util::SliceInt64FromBytes(entry, 0, pos_offset_size);

            // skipping
            if (!current_bitfield->get(read_index+r)) continue;

            uint64_t entry_pos = entry_pos_offset >> kOffsetSize;
            uint64_t entry_offset = entry_pos_offset & ((1U << kOffsetSize) - 1);

            // assemble the new entry and write it to the sort manager

            // map the pos and offset to the new, compacted, positions and
            // offsets
            std::tie(entry_pos, entry_offset) = index->lookup(entry_pos, entry_offset);
            entry_pos_offset = (entry_pos << kOffsetSize) | entry_offset;

            // The new entry is slightly different. Metadata is dropped, to
            // save space, and the counter of the entry is written (sort_key). We
            // use this instead of (y + pos + offset) since its smaller.
            uint128_t new_entry = static_cast<uint128_t>(write_counter + writer_counter_of_this_thread) << write_counter_shift;
            new_entry |= (uint128_t)entry_pos_offset << pos_offset_shift;
            Util::IntTo16Bytes(bytes[writer_counter_of_this_thread].get(), new_entry);
            writer_counter_of_this_thread++;
        }

        {
            std::lock_guard<std::mutex> lock(*smm);
            for(int64_t r = 0; r < writer_counter_of_this_thread; ++r){
                sort_manager->AddToCache(bytes[r].get());
            }
        }
    }
    return 0;
}

// Backpropagate takes in as input, a file on which forward propagation has been done.
// The purpose of backpropagate is to eliminate any dead entries that don't contribute
// to final values in f7, to minimize disk usage. A sort on disk is applied to each table,
// so that they are sorted by position.
Phase2Results RunPhase2(
    std::vector<FileDisk> &tmp_1_disks,
    std::vector<uint64_t> table_sizes,
    uint8_t const k,
    const uint8_t *id,
    const std::string &tmp_dirname,
    const std::string &filename,
    uint64_t memory_size,
    uint32_t const num_buckets,
    uint32_t const log_num_buckets,
    uint8_t const num_threads,
    uint8_t const flags)
{
    // After pruning each table will have 0.865 * 2^k or fewer entries on
    // average
    uint8_t const pos_size = k;
    uint8_t const pos_offset_size = pos_size + kOffsetSize;
    uint8_t const write_counter_shift = 128 - k;
    uint8_t const pos_offset_shift = write_counter_shift - pos_offset_size;
    uint8_t const f7_shift = 128 - k;
    uint8_t const t7_pos_offset_shift = f7_shift - pos_offset_size;
    uint8_t const new_entry_size = EntrySizes::GetKeyPosOffsetSize(k);

    std::vector<uint64_t> new_table_sizes(8, 0);
    new_table_sizes[7] = table_sizes[7];

    // Iterates through each table, starting at 6 & 7. Each iteration, we scan
    // the current table twice. In the first scan, we:

    // 1. drop entries marked as false in the current bitfield (except table 7,
    //    where we don't drop anything, this is a special case)
    // 2. mark entries in the next_bitfield that non-dropped entries have
    //    references to

    // The second scan of the table, we update the positions and offsets to
    // reflect the entries that will be dropped in the next table.

    // At the end of the iteration, we transfer the next_bitfield to the current bitfield
    // to use it to prune the next table to scan.

    int64_t const max_table_size = *std::max_element(table_sizes.begin()
        , table_sizes.end());

    bitfield next_bitfield(max_table_size);
    bitfield current_bitfield(max_table_size);

    std::vector<std::unique_ptr<SortManager>> output_files;

    // table 1 and 7 are special. They are passed on as plain files on disk.
    // Only table 2-6 are passed on as SortManagers, to phase3
    output_files.resize(7 - 2);

    // note that we don't iterate over table_index=1. That table is special
    // since it contains different data. We'll do an extra scan of table 1 at
    // the end, just to compact it.
    for (int table_index = 7; table_index > 1; --table_index) {

        std::cout << "Backpropagating on table " << table_index << std::endl;

        Timer scan_timer;

        next_bitfield.clear();

        int64_t const table_size = table_sizes[table_index];
        int16_t const entry_size = cdiv(k + kOffsetSize + (table_index == 7 ? k : 0), 8);

        BufferedDisk disk(&tmp_1_disks[table_index], table_size * entry_size);

        // read_index is the number of entries we've processed so far (in the
        // current table) i.e. the index to the current entry. This is not used
        // for table 7

        int64_t read_cursor = 0;
        if(table_index==7){
            for (int64_t read_index = 0; read_index < table_size; ++read_index, read_cursor += entry_size)
            {
                uint8_t const* entry = disk.Read(read_cursor, entry_size);

                uint64_t entry_pos_offset = 0;
                // table 7 is special, we never drop anything, so just build
                // next_bitfield
                entry_pos_offset = Util::SliceInt64FromBytes(entry, k, pos_offset_size);

                uint64_t entry_pos = entry_pos_offset >> kOffsetSize;
                uint64_t entry_offset = entry_pos_offset & ((1U << kOffsetSize) - 1);
                // mark the two matching entries as used (pos and pos+offset)
                next_bitfield.set(entry_pos);
                next_bitfield.set(entry_pos + entry_offset);
            }
        }else{
            std::cout << "parallel scanning" << std::endl;
            std::vector<std::thread> threads;
            concurrent_queue<std::shared_ptr<SCANTHREADDATA> > q;
            std::mutex next_bitfield_mutex;
            exit_flag exitflag;

            for (int i = 0; i < num_threads; ++i) {
                threads.emplace_back(ScanThread, &current_bitfield, &next_bitfield, k, pos_offset_size, entry_size, &q, &exitflag, &next_bitfield_mutex);
            }
            std::cout << "thread created" << std::endl;

            //TODO: chunk_size should be determined by memory usage
            const int64_t chunk_size = 128*1024*1024/entry_size > entry_size ? 128*1024*1024/entry_size : entry_size;

            for (int64_t read_index = 0; read_index < table_size; read_index+=chunk_size, read_cursor += entry_size*chunk_size)
            {
                const auto chunk = std::min(table_size-read_index, chunk_size);
                uint8_t const* entry = disk.Read(read_cursor, chunk*entry_size);
                auto sp_ptr = new(std::nothrow) uint8_t[(chunk+1)*entry_size];
                if(!sp_ptr){
                    std::cout << "spptr nullptr!" << std::endl;
                    exit(1);
                }
                // std::shared_ptr<uint8_t[]> sp(sp_ptr); //C++20
                std::shared_ptr<uint8_t> sp(sp_ptr, std::default_delete<uint8_t[]>()); // shared_ptr does'nt support array in C++ 17
                std::copy(entry, entry + chunk*entry_size, sp.get());

                q.push(std::make_shared<SCANTHREADDATA>(chunk,read_index,sp));
            }
            
            exitflag.exit();
            std::cout << "finished making jobs" << std::endl;
            for (auto& t : threads) {
                t.join();
            }
        }

        std::cout << "scanned table " << table_index << std::endl;
        scan_timer.PrintElapsed("scanned time = ");

        std::cout << "sorting table " << table_index << std::endl;
        Timer sort_timer;

        // read the same table again. This time we'll output it to new files:
        // * add sort_key (just the index of the current entry)
        // * update (pos, offset) to remain valid after table_index-1 has been
        //   compacted.
        // * sort by pos
        //
        // As we have to sort two adjacent tables at the same time in phase 3,
        // we can use only a half of memory_size for SortManager. However,
        // table 1 is already sorted, so we can use all memory for sorting
        // table 2.

        auto sort_manager = std::make_unique<SortManager>(
            table_index == 2 ? memory_size : memory_size / 2,
            num_buckets,
            log_num_buckets,
            new_entry_size,
            tmp_dirname,
            filename + ".p2.t" + std::to_string(table_index),
            uint32_t(k),
            0,
            strategy_t::quicksort_last);

        // as we scan the table for the second time, we'll also need to remap
        // the positions and offsets based on the next_bitfield.
        bitfield_index const index(next_bitfield);

        read_cursor = 0;
        int64_t write_counter = 0;
        if (table_index == 7) {
            for (int64_t read_index = 0; read_index < table_size; ++read_index, read_cursor += entry_size)
            {
                uint8_t const* entry = disk.Read(read_cursor, entry_size);

                uint64_t entry_f7 = 0;
                uint64_t entry_pos_offset;
                
                // table 7 is special, we never drop anything, so just build
                // next_bitfield
                entry_f7 = Util::SliceInt64FromBytes(entry, 0, k);
                entry_pos_offset = Util::SliceInt64FromBytes(entry, k, pos_offset_size);

                uint64_t entry_pos = entry_pos_offset >> kOffsetSize;
                uint64_t entry_offset = entry_pos_offset & ((1U << kOffsetSize) - 1);

                // assemble the new entry and write it to the sort manager

                // map the pos and offset to the new, compacted, positions and
                // offsets
                std::tie(entry_pos, entry_offset) = index.lookup(entry_pos, entry_offset);
                entry_pos_offset = (entry_pos << kOffsetSize) | entry_offset;

                uint8_t bytes[16];
                // table 7 is already sorted by pos, so we just rewrite the
                // pos and offset in-place
                uint128_t new_entry = (uint128_t)entry_f7 << f7_shift;
                new_entry |= (uint128_t)entry_pos_offset << t7_pos_offset_shift;
                Util::IntTo16Bytes(bytes, new_entry);

                disk.Write(read_index * entry_size, bytes, entry_size);
                
                ++write_counter;
            }
        }else{
            std::cout << "parallel sorting" << std::endl;
            std::vector<std::thread> threads;
            concurrent_queue<std::shared_ptr<SORTTHREADDATA> > q;
            std::mutex sort_manager_mutex;
            exit_flag exitflag;

            for (int i = 0; i < num_threads; ++i) {
                threads.emplace_back(SortThread, &current_bitfield, &index, k, pos_offset_size,
                                                 pos_offset_shift, entry_size, write_counter_shift,
                                                 &q, &exitflag, sort_manager.get(), &sort_manager_mutex);
            }
            std::cout << "thread created" << std::endl;

            //TODO: chunk_size should be determined by memory usage
            const int64_t chunk_size = 128*1024*1024/entry_size > entry_size ? 128*1024*1024/entry_size : entry_size;

            for (int64_t read_index = 0; read_index < table_size; read_index+=chunk_size, read_cursor += entry_size*chunk_size)
            {
                const auto chunk = std::min(table_size-read_index, chunk_size);
                uint8_t const* entry = disk.Read(read_cursor, chunk*entry_size);
                auto sp_ptr = new(std::nothrow) uint8_t[(chunk+1)*entry_size];
                if(!sp_ptr){
                    std::cout << "spptr nullptr!" << std::endl;
                    exit(1);
                }
                //std::shared_ptr<uint8_t[]> sp(sp_ptr);
                std::shared_ptr<uint8_t> sp(sp_ptr, std::default_delete<uint8_t[]>()); // shared_ptr does'nt support array in C++ 17
                std::copy(entry, entry + chunk*entry_size, sp.get());

                q.push(std::make_shared<SORTTHREADDATA>(chunk,read_index,write_counter,sp));
                for(int64_t r = 0; r < chunk; ++r){
                    if (current_bitfield.get(read_index+r)) write_counter++;
                }
            }

            exitflag.exit();
            std::cout << "finished making jobs" << std::endl;
            for (auto& t : threads) {
                t.join();
            }
        }

        if (table_index != 7) {
            sort_manager->FlushCache();
            sort_timer.PrintElapsed("sort time = ");

            // clear disk caches
            disk.FreeMemory();
            sort_manager->FreeMemory();

            output_files[table_index - 2] = std::move(sort_manager);
            new_table_sizes[table_index] = write_counter;
        }
        current_bitfield.swap(next_bitfield);
        next_bitfield.clear();

        // The files for Table 1 and 7 are re-used, overwritten and passed on to
        // the next phase. However, table 2 through 6 are all written to sort
        // managers that are passed on to the next phase. At this point, we have
        // to delete the input files for table 2-6 to save disk space.
        // This loop doesn't cover table 1, it's handled below with the
        // FilteredDisk wrapper.
        if (table_index != 7) {
            tmp_1_disks[table_index].Truncate(0);
        }
        if (flags & SHOW_PROGRESS) {
            progress(2, 8 - table_index, 6);
        }
    }

    // lazy-compact table 1 based on current_bitfield

    int const table_index = 1;
    int64_t const table_size = table_sizes[table_index];
    int16_t const entry_size = EntrySizes::GetMaxEntrySize(k, table_index, false);

    // at this point, table 1 still needs to be compacted, based on
    // current_bitfield. Instead of compacting it right now, defer it and read
    // from it as-if it was compacted. This saves one read and one write pass
    new_table_sizes[table_index] = current_bitfield.count(0, table_size);
    BufferedDisk disk(&tmp_1_disks[table_index], table_size * entry_size);

    std::cout << "table " << table_index << " new size: " << new_table_sizes[table_index] << std::endl;

    return {
        FilteredDisk(std::move(disk), std::move(current_bitfield), entry_size)
        , BufferedDisk(&tmp_1_disks[7], new_table_sizes[7] * new_entry_size)
        , std::move(output_files)
        , std::move(new_table_sizes)
    };
}

#endif  // SRC_CPP_PHASE2_HPP
