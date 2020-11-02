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

#ifndef SRC_CPP_PROVER_DISK_HPP_
#define SRC_CPP_PROVER_DISK_HPP_

#ifndef _WIN32
#include <unistd.h>
#endif
#include <stdio.h>

#include <algorithm>  // std::min
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "../lib/include/picosha2.hpp"
#include "calculate_bucket.hpp"
#include "encoding.hpp"
#include "entry_sizes.hpp"
#include "util.hpp"

struct plot_header {
    uint8_t magic[19];
    uint8_t id[32];
    uint8_t k;
    uint8_t fmt_desc_len[2];
    uint8_t fmt_desc[50];
};

// The DiskProver, given a correctly formatted plot file, can efficiently generate valid proofs
// of space, for a given challenge.
class DiskProver {
public:
    // The costructor opens the file, and reads the contents of the file header. The table pointers
    // will be used to find and seek to all seven tables, at the time of proving.
    explicit DiskProver(std::string filename)
    {
    }

    ~DiskProver()
    {
    }

    void GetMemo(uint8_t* buffer) { memcpy(buffer, memo, this->memo_size); }

    uint32_t GetMemoSize() const noexcept { return this->memo_size; }

    void GetId(uint8_t* buffer) { memcpy(buffer, id, kIdLen); }

    std::string GetFilename() const noexcept { return filename; }

    uint8_t GetSize() const noexcept { return k; }

    // Given a challenge, returns a quality string, which is sha256(challenge + 2 adjecent x
    // values), from the 64 value proof. Note that this is more efficient than fetching all 64 x
    // values, which are in different parts of the disk.
    std::vector<LargeBits> GetQualitiesForChallenge(const uint8_t* challenge)
    {
    }

    // Given a challenge, and an index, returns a proof of space. This assumes GetQualities was
    // called, and there are actually proofs present. The index represents which proof to fetch,
    // if there are multiple.
    LargeBits GetFullProof(const uint8_t* challenge, uint32_t index)
    {
    }

    // Reads exactly one line point (pair of two k bit back-pointers) from the given table.
    // The entry at index "position" is read. First, the park index is calculated, then
    // the park is read, and finally, entry deltas are added up to the position that we
    // are looking for.
    uint128_t ReadLinePoint(std::ifstream& disk_file, uint8_t table_index, uint64_t position)
    {
    }

    // Gets the P7 positions of the target f7 entries. Uses the C3 encoded bitmask read from disk.
    // A C3 park is a list of deltas between p7 entries, ANS encoded.
    std::vector<uint64_t> GetP7Positions(
        uint64_t curr_f7,
        uint64_t f7,
        uint64_t curr_p7_pos,
        uint8_t* bit_mask,
        uint16_t encoded_size,
        uint64_t c1_index)
    {
    }

    // Returns P7 table entries (which are positions into table P6), for a given challenge
    std::vector<uint64_t> GetP7Entries(std::ifstream& disk_file, const uint8_t* challenge)
    {
    }

    // Changes a proof of space (64 k bit x values) from plot ordering to proof ordering.
    // Proof ordering: x1..x64 s.t.
    //  f1(x1) m= f1(x2) ... f1(x63) m= f1(x64)
    //  f2(C(x1, x2)) m= f2(C(x3, x4)) ... f2(C(x61, x62)) m= f2(C(x63, x64))
    //  ...
    //  f7(C(....)) == challenge
    //
    // Plot ordering: x1..x64 s.t.
    //  f1(x1) m= f1(x2) || f1(x2) m= f1(x1) .....
    //  For all the levels up to f7
    //  AND x1 < x2, x3 < x4
    //     C(x1, x2) < C(x3, x4)
    //     For all comparisons up to f7
    //     Where a < b is defined as:  max(b) > max(a) where a and b are lists of k bit elements
    std::vector<LargeBits> ReorderProof(const std::vector<Bits>& xs_input) const
    {
    }

    // Recursive function to go through the tables on disk, backpropagating and fetching
    // all of the leaves (x values). For example, for depth=5, it fetches the position-th
    // entry in table 5, reading the two back pointers from the line point, and then
    // recursively calling GetInputs for table 4.
    std::vector<Bits> GetInputs(std::ifstream& disk_file, uint64_t position, uint8_t depth)
    {
    }
};

#endif  // SRC_CPP_PROVER_DISK_HPP_
