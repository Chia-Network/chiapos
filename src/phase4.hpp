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

#ifndef SRC_CPP_PHASE4_HPP_
#define SRC_CPP_PHASE4_HPP_

#include "disk.hpp"
#include "encoding.hpp"
#include "entry_sizes.hpp"
#include "phase3.hpp"
#include "pos_constants.hpp"
#include "util.hpp"
#include "progress.hpp"

// Writes the checkpoint tables. The purpose of these tables, is to store a list of ~2^k values
// of size k (the proof of space outputs from table 7), in a way where they can be looked up for
// proofs, but also efficiently. To do this, we assume table 7 is sorted by f7, and we write the
// deltas between each f7 (which will be mostly 1s and 0s), with a variable encoding scheme
// (C3). Furthermore, we create C1 checkpoints along the way.  For example, every 10,000 f7
// entries, we can have a C1 checkpoint, and a C3 delta encoded entry with 10,000 deltas.

// Since we can't store all the checkpoints in
// memory for large plots, we create checkpoints for the checkpoints (C2), that are meant to be
// stored in memory during proving. For example, every 10,000 C1 entries, we can have a C2
// entry.

// The final table format for the checkpoints will be:
// C1 (checkpoint values)
// C2 (checkpoint values into)
// C3 (deltas of f7s between C1 checkpoints)
void RunPhase4(uint8_t k, uint8_t pos_size, FileDisk &tmp2_disk, Phase3Results &res,
               const uint8_t flags, const int max_phase4_progress_updates)
{
    uint32_t P7_park_size = Util::ByteAlign((k + 1) * kEntriesPerPark) / 8;
    uint64_t number_of_p7_parks =
        ((res.final_entries_written == 0 ? 0 : res.final_entries_written - 1) / kEntriesPerPark) +
        1;

    uint64_t begin_byte_C1 = res.final_table_begin_pointers[7] + number_of_p7_parks * P7_park_size;

    uint64_t total_C1_entries = cdiv(res.final_entries_written, kCheckpoint1Interval);
    uint64_t begin_byte_C2 = begin_byte_C1 + (total_C1_entries + 1) * (Util::ByteAlign(k) / 8);
    uint64_t total_C2_entries = cdiv(total_C1_entries, kCheckpoint2Interval);
    uint64_t begin_byte_C3 = begin_byte_C2 + (total_C2_entries + 1) * (Util::ByteAlign(k) / 8);

    uint32_t size_C3 = EntrySizes::CalculateC3Size(k);
    uint64_t end_byte = begin_byte_C3 + (total_C1_entries)*size_C3;

    res.final_table_begin_pointers[8] = begin_byte_C1;
    res.final_table_begin_pointers[9] = begin_byte_C2;
    res.final_table_begin_pointers[10] = begin_byte_C3;
    res.final_table_begin_pointers[11] = end_byte;

    uint64_t plot_file_reader = 0;
    uint64_t final_file_writer_1 = begin_byte_C1;
    uint64_t final_file_writer_2 = begin_byte_C3;
    uint64_t final_file_writer_3 = res.final_table_begin_pointers[7];

    uint64_t prev_y = 0;
    std::vector<Bits> C2;
    uint64_t num_C1_entries = 0;
    std::vector<uint8_t> deltas_to_write;
    uint32_t right_entry_size_bytes = res.right_entry_size_bits / 8;

    uint8_t *right_entry_buf;
    auto C1_entry_buf = new uint8_t[Util::ByteAlign(k) / 8];
    auto C3_entry_buf = new uint8_t[size_C3];
    auto P7_entry_buf = new uint8_t[P7_park_size];

    std::cout << "\tStarting to write C1 and C3 tables" << std::endl;

    ParkBits to_write_p7;
    const int progress_update_increment = res.final_entries_written / max_phase4_progress_updates;

    // We read each table7 entry, which is sorted by f7, but we don't need f7 anymore. Instead,
    // we will just store pos6, and the deltas in table C3, and checkpoints in tables C1 and C2.
    for (uint64_t f7_position = 0; f7_position < res.final_entries_written; f7_position++) {
        right_entry_buf = res.table7_sm->ReadEntry(plot_file_reader);

        plot_file_reader += right_entry_size_bytes;
        uint64_t entry_y = Util::SliceInt64FromBytes(right_entry_buf, 0, k);
        uint64_t entry_new_pos = Util::SliceInt64FromBytes(right_entry_buf, k, pos_size);

        Bits entry_y_bits = Bits(entry_y, k);

        if (f7_position % kEntriesPerPark == 0 && f7_position > 0) {
            memset(P7_entry_buf, 0, P7_park_size);
            to_write_p7.ToBytes(P7_entry_buf);
            tmp2_disk.Write(final_file_writer_3, (P7_entry_buf), P7_park_size);
            final_file_writer_3 += P7_park_size;
            to_write_p7 = ParkBits();
        }

        to_write_p7 += ParkBits(entry_new_pos, k + 1);

        if (f7_position % kCheckpoint1Interval == 0) {
            entry_y_bits.ToBytes(C1_entry_buf);
            tmp2_disk.Write(final_file_writer_1, (C1_entry_buf), Util::ByteAlign(k) / 8);
            final_file_writer_1 += Util::ByteAlign(k) / 8;
            if (num_C1_entries > 0) {
                final_file_writer_2 = begin_byte_C3 + (num_C1_entries - 1) * size_C3;
                size_t num_bytes =
                    Encoding::ANSEncodeDeltas(deltas_to_write, kC3R, C3_entry_buf + 2) + 2;

                // We need to be careful because deltas are variable sized, and they need to fit
                assert(size_C3 * 8 > num_bytes);

                // Write the size
                Util::IntToTwoBytes(C3_entry_buf, num_bytes - 2);

                tmp2_disk.Write(final_file_writer_2, (C3_entry_buf), num_bytes);
                final_file_writer_2 += num_bytes;
            }
            prev_y = entry_y;
            if (f7_position % (kCheckpoint1Interval * kCheckpoint2Interval) == 0) {
                C2.emplace_back(std::move(entry_y_bits));
            }
            deltas_to_write.clear();
            ++num_C1_entries;
        } else {
            deltas_to_write.push_back(entry_y - prev_y);
            prev_y = entry_y;
        }
        if (flags & SHOW_PROGRESS && f7_position % progress_update_increment == 0) {
            progress(4, f7_position, res.final_entries_written);
        }
    }
    Encoding::ANSFree(kC3R);
    res.table7_sm.reset();

    // Writes the final park to disk
    memset(P7_entry_buf, 0, P7_park_size);
    to_write_p7.ToBytes(P7_entry_buf);

    tmp2_disk.Write(final_file_writer_3, (P7_entry_buf), P7_park_size);
    final_file_writer_3 += P7_park_size;

    if (!deltas_to_write.empty()) {
        size_t num_bytes = Encoding::ANSEncodeDeltas(deltas_to_write, kC3R, C3_entry_buf + 2);
        memset(C3_entry_buf + num_bytes + 2, 0, size_C3 - (num_bytes + 2));
        final_file_writer_2 = begin_byte_C3 + (num_C1_entries - 1) * size_C3;

        // Write the size
        Util::IntToTwoBytes(C3_entry_buf, num_bytes);

        tmp2_disk.Write(final_file_writer_2, (C3_entry_buf), size_C3);
        final_file_writer_2 += size_C3;
        Encoding::ANSFree(kC3R);
    }

    Bits(0, Util::ByteAlign(k)).ToBytes(C1_entry_buf);
    tmp2_disk.Write(final_file_writer_1, (C1_entry_buf), Util::ByteAlign(k) / 8);
    final_file_writer_1 += Util::ByteAlign(k) / 8;
    std::cout << "\tFinished writing C1 and C3 tables" << std::endl;
    std::cout << "\tWriting C2 table" << std::endl;

    for (Bits &C2_entry : C2) {
        C2_entry.ToBytes(C1_entry_buf);
        tmp2_disk.Write(final_file_writer_1, (C1_entry_buf), Util::ByteAlign(k) / 8);
        final_file_writer_1 += Util::ByteAlign(k) / 8;
    }
    Bits(0, Util::ByteAlign(k)).ToBytes(C1_entry_buf);
    tmp2_disk.Write(final_file_writer_1, (C1_entry_buf), Util::ByteAlign(k) / 8);
    final_file_writer_1 += Util::ByteAlign(k) / 8;
    std::cout << "\tFinished writing C2 table" << std::endl;

    delete[] C3_entry_buf;
    delete[] C1_entry_buf;
    delete[] P7_entry_buf;

    final_file_writer_1 = res.header_size - 8 * 3;
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
#endif  // SRC_CPP_PHASE4_HPP
