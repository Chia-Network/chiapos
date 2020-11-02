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

#include "disk.hpp"
#include "entry_sizes.hpp"
#include "sort_manager.hpp"
#include "bitfield.hpp"
#include "bitfield_index.hpp"

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
    uint32_t const log_num_buckets)
{
    // An extra bit is used, since we may have more than 2^k entries in a table. (After pruning,
    // each table will have 0.8*2^k or fewer entries).
    uint8_t const pos_size = k;

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
        int16_t const entry_size = EntrySizes::GetMaxEntrySize(k, table_index, false);

        BufferedDisk disk(&tmp_1_disks[table_index], table_size * entry_size);

        // read_index is the number of entries we've processed so far (in the
        // current table) i.e. the index to the current entry. This is not used
        // for table 7

        int64_t read_cursor = 0;
        for (int64_t read_index = 0; read_index < table_size; ++read_index, read_cursor += entry_size)
        {
            uint8_t const* entry = disk.Read(read_cursor, entry_size);

            uint64_t entry_pos = 0;
            uint64_t entry_offset = 0;
            if (table_index == 7) {
                // table 7 is special, we never drop anything, so just build
                // next_bitfield
                entry_pos = Util::SliceInt64FromBytes(entry, k, pos_size);
                entry_offset = Util::SliceInt64FromBytes(entry, k + pos_size, kOffsetSize);
            } else {
                if (!current_bitfield.get(read_index))
                {
                    // This entry should be dropped.
                    continue;
                }
                entry_pos = Util::SliceInt64FromBytes(entry, 0, pos_size);
                entry_offset = Util::SliceInt64FromBytes(entry, pos_size, kOffsetSize);
            }

            // mark the two matching entries as used (pos and pos+offset)
            next_bitfield.set(entry_pos);
            next_bitfield.set(entry_pos + entry_offset);
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

        auto sort_manager = std::make_unique<SortManager>(
            memory_size / 2,
            num_buckets,
            log_num_buckets,
            uint16_t(entry_size),
            tmp_dirname,
            filename + ".p2.t" + std::to_string(table_index),
            uint32_t(k + 1),
            0,
            strategy_t::quicksort);

        // as we scan the table for the second time, we'll also need to remap
        // the positions and offsets based on the next_bitfield.
        bitfield_index const index(next_bitfield);

        read_cursor = 0;
        int64_t write_counter = 0;
        for (int64_t read_index = 0; read_index < table_size; ++read_index, read_cursor += entry_size)
        {
            uint8_t const* entry = disk.Read(read_cursor, entry_size);

            uint64_t entry_pos = 0;
            uint64_t entry_offset = 0;
            uint64_t entry_f7 = 0;
            if (table_index == 7) {
                // table 7 is special, we never drop anything, so just build
                // next_bitfield
                entry_f7 = Util::SliceInt64FromBytes(entry, 0, k);
                entry_pos = Util::SliceInt64FromBytes(entry, k, pos_size);
                entry_offset = Util::SliceInt64FromBytes(entry, k + pos_size, kOffsetSize);
            } else {
                // skipping
                if (!current_bitfield.get(read_index)) continue;

                entry_pos = Util::SliceInt64FromBytes(entry, 0, pos_size);
                entry_offset = Util::SliceInt64FromBytes(entry, pos_size, kOffsetSize);
            }

            // assemble the new entry and write it to the sort manager

            // map the pos and offset to the new, compacted, positions and
            // offsets
            std::tie(entry_pos, entry_offset) = index.lookup(entry_pos, entry_offset);

            if (table_index == 7) {
                // table 7 is already sorted by pos, so we just rewrite the
                // pos and offset in-place

                Bits new_entry;
                new_entry += Bits(entry_f7, k);
                new_entry += Bits(entry_pos, pos_size);
                new_entry += Bits(entry_offset, kOffsetSize);

                uint8_t bytes[20];
                assert(entry_size <= int(sizeof(bytes)));
                new_entry.ToBytes(bytes);
                disk.Write(read_index * entry_size, bytes, entry_size);
            }
            else {
                Bits new_entry;
                // The new entry is slightly different. Metadata is dropped, to
                // save space, and the counter of the entry is written (sort_key). We
                // use this instead of (y + pos + offset) since its smaller.
                new_entry += Bits(write_counter, k + 1);
                new_entry += Bits(entry_pos, pos_size);
                new_entry += Bits(entry_offset, kOffsetSize);

                assert(new_entry.GetSize() <= uint32_t(entry_size) * 8);

                // If we are not taking up all the bits, make sure they are zeroed
                if (Util::ByteAlign(new_entry.GetSize()) < uint32_t(entry_size) * 8) {
                    new_entry +=
                        Bits(0, entry_size * 8 - new_entry.GetSize());
                }

                sort_manager->AddToCache(new_entry);
            }
            ++write_counter;
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
        , BufferedDisk(&tmp_1_disks[7], new_table_sizes[7] * EntrySizes::GetMaxEntrySize(k, 7, false))
        , std::move(output_files)
        , std::move(new_table_sizes)
    };
}

#endif  // SRC_CPP_PHASE2_HPP
