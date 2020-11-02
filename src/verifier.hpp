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

#ifndef SRC_CPP_VERIFIER_HPP_
#define SRC_CPP_VERIFIER_HPP_

#include <utility>
#include <vector>

#include "calculate_bucket.hpp"

class Verifier {
public:
    // Gets the quality string from a proof in proof ordering. The quality string is two
    // adjacent values, determined by the quality index (1-32), and the proof in plot
    // ordering.
    static LargeBits GetQualityString(
        uint8_t k,
        LargeBits proof,
        uint16_t quality_index,
        const uint8_t* challenge)
    {
    }

    // Validates a proof of space, and returns the quality string if the proof is valid for the
    // given k and challenge. If the proof is invalid, it returns an empty LargeBits().
    LargeBits ValidateProof(
        const uint8_t* id,
        uint8_t k,
        const uint8_t* challenge,
        const uint8_t* proof_bytes,
        uint16_t proof_size)
    {
    }

private:
    // Compares two lists of k values, a and b. a > b iff max(a) > max(b),
    // if there is a tie, the next largest value is compared.
    static bool CompareProofBits(LargeBits left, LargeBits right, uint8_t k)
    {
    }
};

#endif  // SRC_CPP_VERIFIER_HPP_
