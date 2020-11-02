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

#ifndef SRC_CPP_PHASE1_HPP_
#define SRC_CPP_PHASE1_HPP_

#ifndef _WIN32
#include <semaphore.h>
#include <unistd.h>
#endif

#include <math.h>
#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <thread>
#include <memory>

// Gulrak filesystem brings in Windows headers that cause some issues with std
#define _HAS_STD_BYTE 0
#define NOMINMAX

#include "../lib/include/filesystem.hpp"
namespace fs = ghc::filesystem;
#include "calculate_bucket.hpp"
#include "entry_sizes.hpp"
#include "exceptions.hpp"
#include "pos_constants.hpp"
#include "sort_manager.hpp"
#include "threading.hpp"
#include "util.hpp"

struct THREADDATA {
    int index;
    Sem::type* mine;
    Sem::type* theirs;
    uint64_t right_entry_size_bytes;
    uint8_t k;
    uint8_t table_index;
    uint8_t metadata_size;
    uint32_t entry_size_bytes;
    uint8_t pos_size;
    uint64_t prevtableentries;
    uint32_t compressed_entry_size_bytes;
    std::vector<FileDisk>* ptmp_1_disks;
};

struct THREADF1DATA {
    int index;
    Sem::type* mine;
    Sem::type* theirs;
    uint8_t k;
    const uint8_t* id;
};

struct GlobalData {
    uint64_t left_writer_count;
    uint64_t right_writer_count;
    uint64_t matches;
    std::unique_ptr<SortManager> L_sort_manager;
    std::unique_ptr<SortManager> R_sort_manager;
    uint64_t left_writer_buf_entries;
    uint64_t left_writer;
    uint64_t right_writer;
    uint64_t stripe_size;
    uint8_t num_threads;
};

GlobalData globals;

PlotEntry GetLeftEntry(
    uint8_t table_index,
    uint8_t* left_buf,
    uint8_t k,
    uint8_t metadata_size,
    uint8_t pos_size)
{
    PlotEntry left_entry;
    left_entry.y = 0;
    left_entry.read_posoffset = 0;
    left_entry.left_metadata = 0;
    left_entry.right_metadata = 0;

    uint32_t ysize = (table_index == 7) ? k : k + kExtraBits;

    if (table_index == 1) {
        // For table 1, we only have y and metadata
        left_entry.y = Util::SliceInt64FromBytes(left_buf, 0, k + kExtraBits);
        left_entry.left_metadata =
            Util::SliceInt64FromBytes(left_buf, k + kExtraBits, metadata_size);
    } else {
        // For tables 2-6, we we also have pos and offset. We need to read this because
        // this entry will be written again to the table without the y (and some entries
        // are dropped).
        left_entry.y = Util::SliceInt64FromBytes(left_buf, 0, ysize);
        left_entry.read_posoffset =
            Util::SliceInt64FromBytes(left_buf, ysize, pos_size + kOffsetSize);
        if (metadata_size <= 128) {
            left_entry.left_metadata =
                Util::SliceInt128FromBytes(left_buf, ysize + pos_size + kOffsetSize, metadata_size);
        } else {
            // Large metadatas that don't fit into 128 bits. (k > 32).
            left_entry.left_metadata =
                Util::SliceInt128FromBytes(left_buf, ysize + pos_size + kOffsetSize, 128);
            left_entry.right_metadata = Util::SliceInt128FromBytes(
                left_buf, ysize + pos_size + kOffsetSize + 128, metadata_size - 128);
        }
    }
    return left_entry;
}

void* phase1_thread(THREADDATA* ptd)
{
    uint64_t right_entry_size_bytes = ptd->right_entry_size_bytes;
    uint8_t k = ptd->k;
    uint8_t table_index = ptd->table_index;
    uint8_t metadata_size = ptd->metadata_size;
    uint32_t entry_size_bytes = ptd->entry_size_bytes;
    uint8_t pos_size = ptd->pos_size;
    uint64_t prevtableentries = ptd->prevtableentries;
    uint32_t compressed_entry_size_bytes = ptd->compressed_entry_size_bytes;
    std::vector<FileDisk>* ptmp_1_disks = ptd->ptmp_1_disks;

    // Streams to read and right to tables. We will have handles to two tables. We will
    // read through the left table, compute matches, and evaluate f for matching entries,
    // writing results to the right table.
    uint64_t left_buf_entries = 5000 + (uint64_t)((1.1) * (globals.stripe_size));
    uint64_t right_buf_entries = 5000 + (uint64_t)((1.1) * (globals.stripe_size));
    std::unique_ptr<uint8_t[]> right_writer_buf(new uint8_t[right_buf_entries * right_entry_size_bytes + 7]);
    std::unique_ptr<uint8_t[]> left_writer_buf(new uint8_t[left_buf_entries * compressed_entry_size_bytes]);

    FxCalculator f(k, table_index + 1);

    // Stores map of old positions to new positions (positions after dropping entries from L
    // table that did not match) Map ke
    uint16_t position_map_size = 2000;

    // Should comfortably fit 2 buckets worth of items
    std::unique_ptr<uint16_t[]> L_position_map(new uint16_t[position_map_size]);
    std::unique_ptr<uint16_t[]> R_position_map(new uint16_t[position_map_size]);

    // Start at left table pos = 0 and iterate through the whole table. Note that the left table
    // will already be sorted by y
    uint64_t totalstripes = (prevtableentries + globals.stripe_size - 1) / globals.stripe_size;
    uint64_t threadstripes = (totalstripes + globals.num_threads - 1) / globals.num_threads;

    for (uint64_t stripe = 0; stripe < threadstripes; stripe++) {
        uint64_t pos = (stripe * globals.num_threads + ptd->index) * globals.stripe_size;
        uint64_t endpos = pos + globals.stripe_size + 1;  // one y value overlap
        uint64_t left_reader = pos * entry_size_bytes;
        uint64_t left_writer_count = 0;
        uint64_t stripe_left_writer_count = 0;
        uint64_t stripe_start_correction = 0xffffffffffffffff;
        uint64_t right_writer_count = 0;
        uint64_t matches = 0;  // Total matches

        // This is a sliding window of entries, since things in bucket i can match with things in
        // bucket
        // i + 1. At the end of each bucket, we find matches between the two previous buckets.
        std::vector<PlotEntry> bucket_L;
        std::vector<PlotEntry> bucket_R;

        uint64_t bucket = 0;
        bool end_of_table = false;  // We finished all entries in the left table

        uint64_t ignorebucket = 0xffffffffffffffff;
        bool bMatch = false;
        bool bFirstStripeOvertimePair = false;
        bool bSecondStripOvertimePair = false;
        bool bThirdStripeOvertimePair = false;

        bool bStripePregamePair = false;
        bool bStripeStartPair = false;
        bool need_new_bucket = false;
        bool first_thread = ptd->index % globals.num_threads == 0;
        bool last_thread = ptd->index % globals.num_threads == globals.num_threads - 1;

        uint64_t L_position_base = 0;
        uint64_t R_position_base = 0;
        uint64_t newlpos = 0;
        uint64_t newrpos = 0;
        Bits new_left_entry(0, pos_size + kOffsetSize);
        std::vector<std::tuple<PlotEntry, PlotEntry, std::pair<Bits, Bits>>>
            current_entries_to_write;
        std::vector<std::tuple<PlotEntry, PlotEntry, std::pair<Bits, Bits>>>
            future_entries_to_write;
        std::vector<std::pair<uint16_t, uint16_t>> match_indexes;
        std::vector<PlotEntry*> not_dropped;  // Pointers are stored to avoid copying entries

        if (pos == 0) {
            bMatch = true;
            bStripePregamePair = true;
            bStripeStartPair = true;
            stripe_left_writer_count = 0;
            stripe_start_correction = 0;
        }

        Sem::Wait(ptd->theirs);
        need_new_bucket = globals.L_sort_manager->CloseToNewBucket(left_reader);
        if (need_new_bucket) {
            if (!first_thread) {
                Sem::Wait(ptd->theirs);
            }
            globals.L_sort_manager->TriggerNewBucket(left_reader);
        }
        if (!last_thread) {
            // Do not post if we are the last thread, because first thread has already
            // waited for us to finish when it starts
            Sem::Post(ptd->mine);
        }

        while (pos < prevtableentries + 1) {
            PlotEntry left_entry = PlotEntry();
            if (pos >= prevtableentries) {
                end_of_table = true;
                left_entry.y = 0;
                left_entry.left_metadata = 0;
                left_entry.right_metadata = 0;
                left_entry.used = false;
            } else {
                // Reads a left entry from disk
                uint8_t* left_buf = globals.L_sort_manager->ReadEntry(left_reader);
                left_reader += entry_size_bytes;

                left_entry = GetLeftEntry(table_index, left_buf, k, metadata_size, pos_size);
            }

            // This is not the pos that was read from disk,but the position of the entry we read,
            // within L table.
            left_entry.pos = pos;
            left_entry.used = false;
            uint64_t y_bucket = left_entry.y / kBC;

            if (!bMatch) {
                if (ignorebucket == 0xffffffffffffffff) {
                    ignorebucket = y_bucket;
                } else {
                    if ((y_bucket != ignorebucket)) {
                        bucket = y_bucket;
                        bMatch = true;
                    }
                }
            }
            if (!bMatch) {
                stripe_left_writer_count++;
                R_position_base = stripe_left_writer_count;
                pos++;
                continue;
            }

            // Keep reading left entries into bucket_L and R, until we run out of things
            if (y_bucket == bucket) {
                bucket_L.emplace_back(left_entry);
            } else if (y_bucket == bucket + 1) {
                bucket_R.emplace_back(left_entry);
            } else {
                // cout << "matching! " << bucket << " and " << bucket + 1 << endl;
                // This is reached when we have finished adding stuff to bucket_R and bucket_L,
                // so now we can compare entries in both buckets to find matches. If two entries
                // match, match, the result is written to the right table. However the writing
                // happens in the next iteration of the loop, since we need to remap positions.
                if (!bucket_L.empty()) {
                    not_dropped.clear();

                    if (!bucket_R.empty()) {
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

                    // Adds L_bucket entries that are used to not_dropped. They are used if they
                    // either matched with something to the left (in the previous iteration), or
                    // matched with something in bucket_R (in this iteration).
                    for (size_t bucket_index = 0; bucket_index < bucket_L.size(); bucket_index++) {
                        PlotEntry& L_entry = bucket_L[bucket_index];
                        if (L_entry.used) {
                            not_dropped.emplace_back(&bucket_L[bucket_index]);
                        }
                    }
                    if (end_of_table) {
                        // In the last two buckets, we will not get a chance to enter the next
                        // iteration due to breaking from loop. Therefore to write the final
                        // bucket in this iteration, we have to add the R entries to the
                        // not_dropped list.
                        for (size_t bucket_index = 0; bucket_index < bucket_R.size();
                             bucket_index++) {
                            PlotEntry& R_entry = bucket_R[bucket_index];
                            if (R_entry.used) {
                                not_dropped.emplace_back(&R_entry);
                            }
                        }
                    }
                    // We keep maps from old positions to new positions. We only need two maps,
                    // one for L bucket and one for R bucket, and we cycle through them. Map
                    // keys are stored as positions % 2^10 for efficiency. Map values are stored
                    // as offsets from the base position for that bucket, for efficiency.
                    std::swap(L_position_map, R_position_map);
                    L_position_base = R_position_base;
                    R_position_base = stripe_left_writer_count;

                    for (PlotEntry*& entry : not_dropped) {
                        // The new position for this entry = the total amount of thing written
                        // to L so far. Since we only write entries in not_dropped, about 14% of
                        // entries are dropped.
                        R_position_map[entry->pos % position_map_size] =
                            stripe_left_writer_count - R_position_base;

                        if (bStripeStartPair) {
                            if (stripe_start_correction == 0xffffffffffffffff) {
                                stripe_start_correction = stripe_left_writer_count;
                            }

                            if (left_writer_count >= left_buf_entries) {
                                throw InvalidStateException("Left writer count overrun");
                            }
                            uint8_t* tmp_buf =
                                left_writer_buf.get() + left_writer_count * compressed_entry_size_bytes;

                            left_writer_count++;
                            // memset(tmp_buf, 0xff, compressed_entry_size_bytes);

                            // Rewrite left entry with just pos and offset, to reduce working space
                            Bits new_left_entry = Bits(
                                (table_index == 1) ? entry->left_metadata : entry->read_posoffset,
                                (table_index == 1) ? k : pos_size + kOffsetSize);

                            new_left_entry.ToBytes(tmp_buf);
                        }
                        stripe_left_writer_count++;
                    }

                    // Two vectors to keep track of things from previous iteration and from this
                    // iteration.
                    current_entries_to_write.swap(future_entries_to_write);
                    future_entries_to_write.clear();

                    for (auto& indeces : match_indexes) {
                        PlotEntry& L_entry = bucket_L[std::get<0>(indeces)];
                        PlotEntry& R_entry = bucket_R[std::get<1>(indeces)];

                        if (bStripeStartPair)
                            matches++;

                        // Sets the R entry to used so that we don't drop in next iteration
                        R_entry.used = true;
                        // Computes the output pair (fx, new_metadata)
                        if (metadata_size <= 128) {
                            const std::pair<Bits, Bits>& f_output = f.CalculateBucket(
                                Bits(L_entry.y, k + kExtraBits),
                                Bits(L_entry.left_metadata, metadata_size),
                                Bits(R_entry.left_metadata, metadata_size));
                            future_entries_to_write.push_back(
                                std::make_tuple(L_entry, R_entry, f_output));
                        } else {
                            // Metadata does not fit into 128 bits
                            const std::pair<Bits, Bits>& f_output = f.CalculateBucket(
                                Bits(L_entry.y, k + kExtraBits),
                                Bits(L_entry.left_metadata, 128) +
                                    Bits(L_entry.right_metadata, metadata_size - 128),
                                Bits(R_entry.left_metadata, 128) +
                                    Bits(R_entry.right_metadata, metadata_size - 128));
                            future_entries_to_write.push_back(
                                std::make_tuple(L_entry, R_entry, f_output));
                        }
                    }

                    // At this point, future_entries_to_write contains the matches of buckets L
                    // and R, and current_entries_to_write contains the matches of L and the
                    // bucket left of L. These are the ones that we will write.
                    uint16_t final_current_entry_size = current_entries_to_write.size();
                    if (end_of_table) {
                        // For the final bucket, write the future entries now as well, since we
                        // will break from loop
                        current_entries_to_write.insert(
                            current_entries_to_write.end(),
                            future_entries_to_write.begin(),
                            future_entries_to_write.end());
                    }
                    for (size_t i = 0; i < current_entries_to_write.size(); i++) {
                        const auto& entry_tuple = current_entries_to_write[i];
                        const PlotEntry& L_entry = std::get<0>(entry_tuple);
                        const PlotEntry& R_entry = std::get<1>(entry_tuple);

                        const std::pair<Bits, Bits>& f_output = std::get<2>(entry_tuple);
                        // We only need k instead of k + kExtraBits bits for the last table
                        Bits new_entry = table_index + 1 == 7 ? std::get<0>(f_output).Slice(0, k)
                                                              : std::get<0>(f_output);

                        // Maps the new positions. If we hit end of pos, we must write things in
                        // both final_entries to write and current_entries_to_write, which are
                        // in both position maps.
                        if (!end_of_table || i < final_current_entry_size) {
                            newlpos =
                                L_position_map[L_entry.pos % position_map_size] + L_position_base;
                        } else {
                            newlpos =
                                R_position_map[L_entry.pos % position_map_size] + R_position_base;
                        }
                        newrpos = R_position_map[R_entry.pos % position_map_size] + R_position_base;
                        // Position in the previous table
                        new_entry.AppendValue(newlpos, pos_size);

                        // Offset for matching entry
                        if (newrpos - newlpos > (1U << kOffsetSize) * 97 / 100) {
                            throw InvalidStateException(
                                "Offset too large: " + std::to_string(newrpos - newlpos));
                        }

                        new_entry.AppendValue(newrpos - newlpos, kOffsetSize);
                        // New metadata which will be used to compute the next f
                        new_entry += std::get<1>(f_output);

                        if (right_writer_count >= right_buf_entries) {
                            throw InvalidStateException("Left writer count overrun");
                        }

                        if (bStripeStartPair) {
                            uint8_t* right_buf =
                                right_writer_buf.get() + right_writer_count * right_entry_size_bytes;
                            new_entry.ToBytes(right_buf);
                            right_writer_count++;
                        }
                    }
                }

                if (pos >= endpos) {
                    if (!bFirstStripeOvertimePair)
                        bFirstStripeOvertimePair = true;
                    else if (!bSecondStripOvertimePair)
                        bSecondStripOvertimePair = true;
                    else if (!bThirdStripeOvertimePair)
                        bThirdStripeOvertimePair = true;
                    else {
                        break;
                    }
                } else {
                    if (!bStripePregamePair)
                        bStripePregamePair = true;
                    else if (!bStripeStartPair)
                        bStripeStartPair = true;
                }

                if (y_bucket == bucket + 2) {
                    // We saw a bucket that is 2 more than the current, so we just set L = R, and R
                    // = [entry]
                    bucket_L = bucket_R;
                    bucket_R = std::vector<PlotEntry>();
                    bucket_R.emplace_back(std::move(left_entry));
                    ++bucket;
                } else {
                    // We saw a bucket that >2 more than the current, so we just set L = [entry],
                    // and R = []
                    bucket = y_bucket;
                    bucket_L = std::vector<PlotEntry>();
                    bucket_L.emplace_back(std::move(left_entry));
                    bucket_R = std::vector<PlotEntry>();
                }
            }
            // Increase the read pointer in the left table, by one
            ++pos;
        }

        // If we needed new bucket, we already waited
        // Do not wait if we are the first thread, since we are guaranteed that everything is written
        if (!need_new_bucket && !first_thread) {
            Sem::Wait(ptd->theirs);
        }

        uint32_t ysize = (table_index + 1 == 7) ? k : k + kExtraBits;
        uint32_t startbyte = ysize / 8;
        uint32_t endbyte = (ysize + pos_size + 7) / 8 - 1;
        uint64_t shiftamt = (8 - ((ysize + pos_size) % 8)) % 8;
        uint64_t correction = (globals.left_writer_count - stripe_start_correction) << shiftamt;

        // Correct positions
        for (uint32_t i = 0; i < right_writer_count; i++) {
            uint64_t posaccum = 0;
            uint8_t* entrybuf = right_writer_buf.get() + i * right_entry_size_bytes;

            for (uint32_t j = startbyte; j <= endbyte; j++) {
                posaccum = (posaccum << 8) | (entrybuf[j]);
            }
            posaccum += correction;
            for (uint32_t j = endbyte; j >= startbyte; --j) {
                entrybuf[j] = posaccum & 0xff;
                posaccum = posaccum >> 8;
            }
        }
        if (table_index < 6) {
            for (uint64_t i = 0; i < right_writer_count; i++) {
                globals.R_sort_manager->AddToCache(right_writer_buf.get() + i * right_entry_size_bytes);
            }
        } else {
            // Writes out the right table for table 7
            (*ptmp_1_disks)[table_index + 1].Write(
                globals.right_writer,
                right_writer_buf.get(),
                right_writer_count * right_entry_size_bytes);
        }
        globals.right_writer += right_writer_count * right_entry_size_bytes;
        globals.right_writer_count += right_writer_count;

        (*ptmp_1_disks)[table_index].Write(
            globals.left_writer, left_writer_buf.get(), left_writer_count * compressed_entry_size_bytes);
        globals.left_writer += left_writer_count * compressed_entry_size_bytes;
        globals.left_writer_count += left_writer_count;

        globals.matches += matches;
        Sem::Post(ptd->mine);
    }

    return 0;
}

void* F1thread(THREADF1DATA* ptd)
{
    uint8_t k = ptd->k;
    uint32_t entry_size_bytes = 16;

    uint64_t max_value = ((uint64_t)1 << (k));

    uint64_t right_buf_entries = 1 << (kBatchSizes);

    std::unique_ptr<uint64_t[]> f1_entries(new uint64_t[(1U << kBatchSizes)]);

    F1Calculator f1(k, ptd->id);

    std::unique_ptr<uint8_t[]> right_writer_buf(new uint8_t[right_buf_entries * entry_size_bytes]);

    // Instead of computing f1(1), f1(2), etc, for each x, we compute them in batches
    // to increase CPU efficency.
    for (uint64_t lp = ptd->index; lp <= (((uint64_t)1) << (k - kBatchSizes));
         lp = lp + globals.num_threads) {  // globals.num_threads) {
        // For each pair x, y in the batch

        uint64_t right_writer_count = 0;
        uint64_t x = lp * (1 << (kBatchSizes));

        uint64_t loopcount = std::min(max_value - x, (uint64_t)1 << (kBatchSizes));

        // Instead of computing f1(1), f1(2), etc, for each x, we compute them in batches
        // to increase CPU efficency.
        f1.CalculateBuckets(x, loopcount, f1_entries.get());
        for (uint32_t i = 0; i < loopcount; i++) {
            uint8_t to_write[16];
            uint128_t entry;

            entry = (uint128_t)f1_entries[i] << (128 - kExtraBits - k);
            entry |= (uint128_t)x << (128 - kExtraBits - 2 * k);
            Util::IntTo16Bytes(to_write, entry);
            memcpy(&(right_writer_buf[i * entry_size_bytes]), to_write, 16);
            right_writer_count++;
            x++;
        }

        Sem::Wait(ptd->theirs);

        // Write it out
        for (uint32_t i = 0; i < right_writer_count; i++) {
            globals.L_sort_manager->AddToCache(&(right_writer_buf[i * entry_size_bytes]));
        }

        Sem::Post(ptd->mine);
    }

    return 0;
}

// This is Phase 1, or forward propagation. During this phase, all of the 7 tables,
// and f functions, are evaluated. The result is an intermediate plot file, that is
// several times larger than what the final file will be, but that has all of the
// proofs of space in it. First, F1 is computed, which is special since it uses
// ChaCha8, and each encryption provides multiple output values. Then, the rest of the
// f functions are computed, and a sort on disk happens for each table.
std::vector<uint64_t> RunPhase1(
    std::vector<FileDisk>& tmp_1_disks,
    uint8_t k,
    const uint8_t* id,
    std::string tmp_dirname,
    std::string filename,
    uint64_t memory_size,
    uint32_t num_buckets,
    uint32_t log_num_buckets,
    uint32_t stripe_size,
    uint8_t const num_threads)
{
    std::cout << "Computing table 1" << std::endl;
    globals.stripe_size = stripe_size;
    globals.num_threads = num_threads;
    Timer f1_start_time;
    F1Calculator f1(k, id);
    uint64_t x = 0;
    uint64_t prevtableentries = 0;

    uint32_t t1_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, 1, true);
    globals.L_sort_manager = std::make_unique<SortManager>(
        memory_size,
        num_buckets,
        log_num_buckets,
        t1_entry_size_bytes,
        tmp_dirname,
        filename + ".p1.t1",
        0,
        globals.stripe_size);

    // These are used for sorting on disk. The sort on disk code needs to know how
    // many elements are in each bucket.
    std::vector<uint64_t> table_sizes = std::vector<uint64_t>(8, 0);

    {
        // Start of parallel execution
        auto td = std::make_unique<THREADF1DATA[]>(num_threads);
        auto mutex = std::make_unique<Sem::type[]>(num_threads);

        std::vector<std::thread> threads;

        for (int i = 0; i < num_threads; i++) {
            mutex[i] = Sem::Create();
        }

        for (int i = 0; i < num_threads; i++) {
            td[i].index = i;
            td[i].mine = &mutex[i];
            td[i].theirs = &mutex[(num_threads + i - 1) % num_threads];

            td[i].k = k;
            td[i].id = id;

            threads.emplace_back(F1thread, &td[i]);
        }
        Sem::Post(&mutex[num_threads - 1]);

        for (auto& t : threads) {
            t.join();
        }

        for (int i = 0; i < num_threads; i++) {
            Sem::Destroy(mutex[i]);
        }

        // end of parallel execution
    }

    prevtableentries = 1ULL << k;
    f1_start_time.PrintElapsed("F1 complete, time:");
    globals.L_sort_manager->FlushCache();
    table_sizes[1] = x + 1;

    // Store positions to previous tables, in k bits.
    uint8_t pos_size = k;
    uint32_t right_entry_size_bytes = 0;

    // For tables 1 through 6, sort the table, calculate matches, and write
    // the next table. This is the left table index.
    for (uint8_t table_index = 1; table_index < 7; table_index++) {
        Timer table_timer;
        uint8_t metadata_size = kVectorLens[table_index + 1] * k;

        // Determines how many bytes the entries in our left and right tables will take up.
        uint32_t entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index, true);
        uint32_t compressed_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index, false);
        right_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index + 1, true);

        std::cout << "Computing table " << int{table_index + 1} << std::endl;
        // Start of parallel execution

        FxCalculator f(k, table_index + 1);  // dummy to load static table

        globals.matches = 0;
        globals.left_writer_count = 0;
        globals.right_writer_count = 0;
        globals.right_writer = 0;
        globals.left_writer = 0;

        globals.R_sort_manager = std::make_unique<SortManager>(
            memory_size,
            num_buckets,
            log_num_buckets,
            right_entry_size_bytes,
            tmp_dirname,
            filename + ".p1.t" + std::to_string(table_index + 1),
            0,
            globals.stripe_size);

        globals.L_sort_manager->TriggerNewBucket(0);

        Timer computation_pass_timer;

        auto td = std::make_unique<THREADDATA[]>(num_threads);
        auto mutex = std::make_unique<Sem::type[]>(num_threads);

        std::vector<std::thread> threads;

        for (int i = 0; i < num_threads; i++) {
            mutex[i] = Sem::Create();
        }

        for (int i = 0; i < num_threads; i++) {
            td[i].index = i;
            td[i].mine = &mutex[i];
            td[i].theirs = &mutex[(num_threads + i - 1) % num_threads];

            td[i].prevtableentries = prevtableentries;
            td[i].right_entry_size_bytes = right_entry_size_bytes;
            td[i].k = k;
            td[i].table_index = table_index;
            td[i].metadata_size = metadata_size;
            td[i].entry_size_bytes = entry_size_bytes;
            td[i].pos_size = pos_size;
            td[i].compressed_entry_size_bytes = compressed_entry_size_bytes;
            td[i].ptmp_1_disks = &tmp_1_disks;

            threads.emplace_back(phase1_thread, &td[i]);
        }
        Sem::Post(&mutex[num_threads - 1]);

        for (auto& t : threads) {
            t.join();
        }

        for (int i = 0; i < num_threads; i++) {
            Sem::Destroy(mutex[i]);
        }

        // end of parallel execution

        // Total matches found in the left table
        std::cout << "\tTotal matches: " << globals.matches << std::endl;

        table_sizes[table_index] = globals.left_writer_count;
        table_sizes[table_index + 1] = globals.right_writer_count;

        // Truncates the file after the final write position, deleting no longer useful
        // working space
        tmp_1_disks[table_index].Truncate(globals.left_writer);
        globals.L_sort_manager.reset();
        if (table_index < 6) {
            globals.R_sort_manager->FlushCache();
            globals.L_sort_manager = std::move(globals.R_sort_manager);
        } else {
            tmp_1_disks[table_index + 1].Truncate(globals.right_writer);
        }

        // Resets variables
        if (globals.matches != globals.right_writer_count) {
            throw InvalidStateException(
                "Matches do not match with number of write entries " +
                std::to_string(globals.matches) + " " + std::to_string(globals.right_writer_count));
        }

        prevtableentries = globals.right_writer_count;
        table_timer.PrintElapsed("Forward propagation table time:");
    }
    table_sizes[0] = 0;
    globals.R_sort_manager.reset();
    return table_sizes;
}

#endif  // SRC_CPP_PHASE1_HPP
