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

#ifndef SRC_CPP_B17PHASE2_HPP_
#define SRC_CPP_B17PHASE2_HPP_

#include "disk.hpp"
#include "entry_sizes.hpp"
#include "b17sort_manager.hpp"

// Backpropagate takes in as input, a file on which forward propagation has been done.
// The purpose of backpropagate is to eliminate any dead entries that don't contribute
// to final values in f7, to minimize disk usage. A sort on disk is applied to each table,
// so that they are sorted by position.
std::vector<uint64_t> b17RunPhase2(
    uint8_t *memory,
    std::vector<FileDisk> &tmp_1_disks,
    std::vector<uint64_t> table_sizes,
    uint8_t k,
    const uint8_t *id,
    const std::string &tmp_dirname,
    const std::string &filename,
    uint64_t memory_size,
    uint32_t num_buckets,
    uint32_t log_num_buckets,
    const uint8_t flags)
{
    // An extra bit is used, since we may have more than 2^k entries in a table. (After pruning,
    // each table will have 0.8*2^k or less entries).
    uint8_t pos_size = k;

    std::vector<uint64_t> new_table_sizes = std::vector<uint64_t>(8, 0);
    new_table_sizes[7] = table_sizes[7];
    std::unique_ptr<b17SortManager> R_sort_manager;
    std::unique_ptr<b17SortManager> L_sort_manager;

    // Iterates through each table (with a left and right pointer), starting at 6 & 7.
    for (int table_index = 7; table_index > 1; --table_index) {
        // std::vector<std::pair<uint64_t, uint64_t> > match_positions;
        Timer table_timer;

        std::cout << "Backpropagating on table " << table_index << std::endl;

        uint16_t left_metadata_size = kVectorLens[table_index] * k;

        // The entry that we are reading (no metadata)
        uint16_t left_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index - 1, false);

        // The right entries which we read and write (the already have no metadata, since they
        // have been pruned in previous iteration)
        uint16_t right_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index, false);

        uint64_t left_reader = 0;
        uint64_t left_writer = 0;
        uint64_t right_reader = 0;
        uint64_t right_writer = 0;
        // The memory will be used like this, with most memory allocated towards the SortManager,
        // since it needs it
        // [--------------------------SM/RR-------------------------|-----------LW-------------|--RW--|--LR--]
        uint64_t sort_manager_buf_size = floor(kMemSortProportion * memory_size);
        uint64_t left_writer_buf_size = 3 * (memory_size - sort_manager_buf_size) / 4;
        uint64_t other_buf_sizes = (memory_size - sort_manager_buf_size - left_writer_buf_size) / 2;
        uint8_t *right_reader_buf = &(memory[0]);
        uint8_t *left_writer_buf = &(memory[sort_manager_buf_size]);
        uint8_t *right_writer_buf = &(memory[sort_manager_buf_size + left_writer_buf_size]);
        uint8_t *left_reader_buf =
            &(memory[sort_manager_buf_size + left_writer_buf_size + other_buf_sizes]);
        uint64_t right_reader_buf_entries = sort_manager_buf_size / right_entry_size_bytes;
        uint64_t left_writer_buf_entries = left_writer_buf_size / left_entry_size_bytes;
        uint64_t right_writer_buf_entries = other_buf_sizes / right_entry_size_bytes;
        uint64_t left_reader_buf_entries = other_buf_sizes / left_entry_size_bytes;
        uint64_t left_reader_count = 0;
        uint64_t right_reader_count = 0;
        uint64_t left_writer_count = 0;
        uint64_t right_writer_count = 0;

        if (table_index != 7) {
            R_sort_manager->ChangeMemory(memory, sort_manager_buf_size);
        }

        L_sort_manager = std::make_unique<b17SortManager>(
            left_writer_buf,
            left_writer_buf_size,
            num_buckets,
            log_num_buckets,
            left_entry_size_bytes,
            tmp_dirname,
            filename + ".p2.t" + std::to_string(table_index - 1),
            0,
            0);

        // We will divide by 2, so it must be even.
        assert(kCachedPositionsSize % 2 == 0);

        // Used positions will be used to mark which posL are present in table R, the rest will
        // be pruned
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
        // This is a map from old position to array of sort keys (one for each R entry with this
        // pos)
        uint64_t old_sort_keys[kReadMinusWrite][kMaxMatchesSingleEntry];
        // Map from old position to other positions that it matches with
        uint64_t old_offsets[kReadMinusWrite][kMaxMatchesSingleEntry];
        // Map from old position to count (number of times it appears)
        uint16_t old_counters[kReadMinusWrite];

        for (uint16_t &old_counter : old_counters) {
            old_counter = 0;
        }

        bool end_of_right_table = false;
        uint64_t current_pos = 0;  // This is the current pos that we are looking for in the L table
        uint64_t end_of_table_pos = 0;
        uint64_t greatest_pos = 0;  // This is the greatest position we have seen in R table

        // Buffers for reading and writing to disk
        uint8_t *left_entry_buf;
        uint8_t *new_left_entry_buf;
        uint8_t *right_entry_buf;
        uint8_t *right_entry_buf_SM = new uint8_t[right_entry_size_bytes];

        // Go through all right entries, and keep going since write pointer is behind read
        // pointer
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
            // Only runs this code if we are still reading the right table, or we still need to
            // read more left table entries (current_pos <= greatest_pos), otherwise, it skips
            // to the writing of the final R table entries
            if (!end_of_right_table || current_pos <= greatest_pos) {
                uint64_t entry_sort_key = 0;
                uint64_t entry_pos = 0;
                uint64_t entry_offset = 0;

                while (!end_of_right_table) {
                    if (should_read_entry) {
                        if (right_reader_count == new_table_sizes[table_index]) {
                            // Table R has ended, don't read any more (but keep writing)
                            end_of_right_table = true;
                            end_of_table_pos = current_pos;
                            break;
                        }
                        // Need to read another entry at the current position
                        if (table_index == 7) {
                            if (right_reader_count % right_reader_buf_entries == 0) {
                                uint64_t readAmt = std::min(
                                    right_reader_buf_entries * right_entry_size_bytes,
                                    (new_table_sizes[table_index] - right_reader_count) *
                                        right_entry_size_bytes);

                                tmp_1_disks[table_index].Read(
                                    right_reader, right_reader_buf, readAmt);
                                right_reader += readAmt;
                            }
                            right_entry_buf =
                                right_reader_buf + (right_reader_count % right_reader_buf_entries) *
                                                       right_entry_size_bytes;
                        } else {
                            right_entry_buf = R_sort_manager->ReadEntry(right_reader);
                            right_reader += right_entry_size_bytes;
                        }
                        right_reader_count++;

                        if (table_index == 7) {
                            // This is actually y for table 7
                            entry_sort_key = Util::SliceInt64FromBytes(right_entry_buf, 0, k);
                            entry_pos = Util::SliceInt64FromBytes(right_entry_buf, k, pos_size);
                            entry_offset = Util::SliceInt64FromBytes(
                                right_entry_buf, k + pos_size, kOffsetSize);
                        } else {
                            entry_pos = Util::SliceInt64FromBytes(right_entry_buf, 0, pos_size);
                            entry_offset =
                                Util::SliceInt64FromBytes(right_entry_buf, pos_size, kOffsetSize);
                            entry_sort_key = Util::SliceInt64FromBytes(
                                right_entry_buf, pos_size + kOffsetSize, k);
                        }
                    } else if (cached_entry_pos == current_pos) {
                        // We have a cached entry at this position
                        entry_sort_key = cached_entry_sort_key;
                        entry_pos = cached_entry_pos;
                        entry_offset = cached_entry_offset;
                    } else {
                        // The cached entry is at a later pos, so we don't read any more R
                        // entries, read more L entries instead.
                        break;
                    }

                    should_read_entry = true;  // By default, read another entry
                    if (entry_pos + entry_offset > greatest_pos) {
                        // Greatest L pos that we should look for
                        greatest_pos = entry_pos + entry_offset;
                    }

                    if (entry_pos == current_pos) {
                        // The current L position is the current R entry
                        // Marks the two matching entries as used (pos and pos+offset)
                        used_positions[entry_pos % kCachedPositionsSize] = true;
                        used_positions[(entry_pos + entry_offset) % kCachedPositionsSize] = true;

                        uint64_t old_write_pos = entry_pos % kReadMinusWrite;

                        // Stores the sort key for this R entry
                        old_sort_keys[old_write_pos][old_counters[old_write_pos]] = entry_sort_key;

                        // Stores the other matching pos for this R entry (pos6 + offset)
                        old_offsets[old_write_pos][old_counters[old_write_pos]] =
                            entry_pos + entry_offset;
                        ++old_counters[old_write_pos];
                    } else {
                        // Don't read any more right entries for now, because we haven't caught
                        // up on the left table yet
                        should_read_entry = false;
                        cached_entry_sort_key = entry_sort_key;
                        cached_entry_pos = entry_pos;
                        cached_entry_offset = entry_offset;
                        break;
                    }
                }
                // Only process left table if we still have entries - should fix read 0 issue
                if(left_reader_count < table_sizes[table_index - 1])
                {
                    // ***Reads a left entry
                    if (left_reader_count % left_reader_buf_entries == 0) {
                        uint64_t readAmt = std::min(
                            left_reader_buf_entries * left_entry_size_bytes,
                            (table_sizes[table_index - 1] - left_reader_count) * left_entry_size_bytes);
                        tmp_1_disks[table_index - 1].Read(left_reader, left_reader_buf, readAmt);
                        left_reader += readAmt;
                    }
                    left_entry_buf = left_reader_buf + (left_reader_count % left_reader_buf_entries) *
                                                       left_entry_size_bytes;
                    left_reader_count++;

                    // If this left entry is used, we rewrite it. If it's not used, we ignore it.
                    if (used_positions[current_pos % kCachedPositionsSize]) {
                        uint64_t entry_metadata;

                        if (table_index > 2) {
                            // For tables 2-6, the entry is: pos, offset
                            entry_pos = Util::SliceInt64FromBytes(left_entry_buf, 0, pos_size);
                            entry_offset =
                                Util::SliceInt64FromBytes(left_entry_buf, pos_size, kOffsetSize);
                        } else {
                            entry_metadata =
                                Util::SliceInt64FromBytes(left_entry_buf, 0, left_metadata_size);
                        }

                        new_left_entry_buf =
                            left_writer_buf +
                            (left_writer_count % left_writer_buf_entries) * left_entry_size_bytes;
                        left_writer_count++;

                        Bits new_left_entry;
                        if (table_index > 2) {
                            // The new left entry is slightly different. Metadata is dropped, to
                            // save space, and the counter of the entry is written (sort_key). We
                            // use this instead of (y + pos + offset) since its smaller.
                            new_left_entry += Bits(entry_pos, pos_size);
                            new_left_entry += Bits(entry_offset, kOffsetSize);
                            new_left_entry += Bits(left_entry_counter, k);

                            // If we are not taking up all the bits, make sure they are zeroed
                            if (Util::ByteAlign(new_left_entry.GetSize()) < left_entry_size_bytes * 8) {
                                new_left_entry +=
                                    Bits(0, left_entry_size_bytes * 8 - new_left_entry.GetSize());
                            }
                            L_sort_manager->AddToCache(new_left_entry);
                        } else {
                            // For table one entries, we don't care about sort key, only x.
                            // Also, we don't use the sort manager, since we won't sort it.
                            new_left_entry += Bits(entry_metadata, left_metadata_size);
                            new_left_entry.ToBytes(new_left_entry_buf);
                            if (left_writer_count % left_writer_buf_entries == 0) {
                                tmp_1_disks[table_index - 1].Write(
                                    left_writer,
                                    left_writer_buf,
                                    left_writer_buf_entries * left_entry_size_bytes);
                                left_writer += left_writer_buf_entries * left_entry_size_bytes;
                            }
                        }

                        // Mapped positions, so we can rewrite the R entry properly
                        new_positions[current_pos % kCachedPositionsSize] = left_entry_counter;

                        // Counter for new left entries written
                        ++left_entry_counter;
                    }
                }
            }
            // Write pointer lags behind the read pointer
            int64_t write_pointer_pos = current_pos - kReadMinusWrite + 1;

            // Only write entries for write_pointer_pos, if we are above 0, and there are
            // actually R entries for that pos.
            if (write_pointer_pos >= 0 &&
                used_positions[write_pointer_pos % kCachedPositionsSize]) {
                uint64_t new_pos = new_positions[write_pointer_pos % kCachedPositionsSize];
                Bits new_pos_bin(new_pos, pos_size);
                // There may be multiple R entries that share the write_pointer_pos, so write
                // all of them
                for (uint32_t counter = 0;
                     counter < old_counters[write_pointer_pos % kReadMinusWrite];
                     counter++) {
                    // Creates and writes the new right entry, with the cached data
                    uint64_t new_offset_pos = new_positions
                        [old_offsets[write_pointer_pos % kReadMinusWrite][counter] %
                         kCachedPositionsSize];
                    Bits new_right_entry =
                        table_index == 7
                            ? Bits(old_sort_keys[write_pointer_pos % kReadMinusWrite][counter], k)
                            : Bits(
                                  old_sort_keys[write_pointer_pos % kReadMinusWrite][counter],
                                  k);
                    new_right_entry += new_pos_bin;
                    // match_positions.push_back(std::make_pair(new_pos, new_offset_pos));
                    new_right_entry.AppendValue(new_offset_pos - new_pos, kOffsetSize);

                    // Calculate right entry pointer for output
                    right_entry_buf =
                        right_writer_buf +
                        (right_writer_count % right_writer_buf_entries) * right_entry_size_bytes;
                    right_writer_count++;

                    if (Util::ByteAlign(new_right_entry.GetSize()) < right_entry_size_bytes * 8) {
                        memset(right_entry_buf, 0, right_entry_size_bytes);
                    }
                    new_right_entry.ToBytes(right_entry_buf);
                    // Check for write out to disk
                    if (right_writer_count % right_writer_buf_entries == 0) {
                        tmp_1_disks[table_index].Write(
                            right_writer,
                            right_writer_buf,
                            right_writer_buf_entries * right_entry_size_bytes);
                        right_writer += right_writer_buf_entries * right_entry_size_bytes;
                    }
                }
            }
            ++current_pos;
        }
        new_table_sizes[table_index - 1] = left_entry_counter;

        std::cout << "\tWrote left entries: " << left_entry_counter << std::endl;
        table_timer.PrintElapsed("Total backpropagation time::");

        tmp_1_disks[table_index].Write(
            right_writer,
            right_writer_buf,
            (right_writer_count % right_writer_buf_entries) * right_entry_size_bytes);
        right_writer += (right_writer_count % right_writer_buf_entries) * right_entry_size_bytes;

        if (table_index != 7) {
            R_sort_manager.reset();
        }

        // Truncates the right table
        tmp_1_disks[table_index].Truncate(right_writer);

        if (table_index == 2) {
            // Writes remaining entries for table1
            tmp_1_disks[table_index - 1].Write(
                left_writer,
                left_writer_buf,
                (left_writer_count % left_writer_buf_entries) * left_entry_size_bytes);
            left_writer += (left_writer_count % left_writer_buf_entries) * left_entry_size_bytes;

            // Truncates the left table
            tmp_1_disks[table_index - 1].Truncate(left_writer);
        } else {
            L_sort_manager->FlushCache();
            R_sort_manager = std::move(L_sort_manager);
        }
        delete[] right_entry_buf_SM;
        if (flags & SHOW_PROGRESS) {
            progress(2, 8 - table_index, 6);
        }
    }
    L_sort_manager.reset();
    return new_table_sizes;
}

#endif  // SRC_CPP_PHASE2_HPP
