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

#ifndef SRC_CPP_CALCULATE_BUCKET_HPP_
#define SRC_CPP_CALCULATE_BUCKET_HPP_

#include <stdint.h>

#include <algorithm>
#include <array>
#include <bitset>
#include <iostream>
#include <map>
#include <utility>
#include <vector>

#include "b3/blake3.h"
#include "bits.hpp"
#include "chacha8.h"
#include "pos_constants.hpp"
#include "util.hpp"

// ChaCha8 block size
const uint16_t kF1BlockSizeBits = 512;

// Extra bits of output from the f functions. Instead of being a function from k -> k bits,
// it's a function from k -> k + kExtraBits bits. This allows less collisions in matches.
// Refer to the paper for mathematical motivations.
const uint8_t kExtraBits = 6;

// Convenience variable
const uint8_t kExtraBitsPow = 1 << kExtraBits;

// B and C groups which constitute a bucket, or BC group. These groups determine how
// elements match with each other. Two elements must be in adjacent buckets to match.
const uint16_t kB = 119;
const uint16_t kC = 127;
const uint16_t kBC = kB * kC;

// This (times k) is the length of the metadata that must be kept for each entry. For example,
// for a tbale 4 entry, we must keep 4k additional bits for each entry, which is used to
// compute f5.
static const uint8_t kVectorLens[] = {0, 0, 1, 2, 4, 4, 3, 2};

uint16_t L_targets[2][kBC][kExtraBitsPow];
bool initialized = false;
void load_tables()
{
    for (uint8_t parity = 0; parity < 2; parity++) {
        for (uint16_t i = 0; i < kBC; i++) {
            uint16_t indJ = i / kC;
            for (uint16_t m = 0; m < kExtraBitsPow; m++) {
                uint16_t yr =
                    ((indJ + m) % kB) * kC + (((2 * m + parity) * (2 * m + parity) + i) % kC);
                L_targets[parity][i][m] = yr;
            }
        }
    }
}

// Class to evaluate F1
class F1Calculator {
public:
    F1Calculator() {}

    inline F1Calculator(uint8_t k, const uint8_t* orig_key)
    {
        uint8_t enc_key[32];
        this->k_ = k;

        // First byte is 1, the index of this table
        enc_key[0] = 1;
        memcpy(enc_key + 1, orig_key, 31);

        // Setup ChaCha8 context with zero-filled IV
        chacha8_keysetup(&this->enc_ctx_, enc_key, 256, NULL);
    }

    inline ~F1Calculator() {}

    // Disable copying
    F1Calculator(const F1Calculator&) = delete;

    // Reloading the encryption key is a no-op since encryption state is local.
    inline void ReloadKey() {}

    // Performs one evaluation of the F function on input L of k bits.
    inline Bits CalculateF(const Bits& L) const
    {
        uint16_t num_output_bits = k_;
        uint16_t block_size_bits = kF1BlockSizeBits;

        // Calculates the counter that will be used to get ChaCha8 keystream.
        // Since k < block_size_bits, we can fit several k bit blocks into one
        // ChaCha8 block.
        uint128_t counter_bit = L.GetValue() * (uint128_t)num_output_bits;
        uint64_t counter = counter_bit / block_size_bits;

        // How many bits are before L, in the current block
        uint32_t bits_before_L = counter_bit % block_size_bits;

        // How many bits of L are in the current block (the rest are in the next block)
        const uint16_t bits_of_L =
            std::min((uint16_t)(block_size_bits - bits_before_L), num_output_bits);

        // True if L is divided into two blocks, and therefore 2 ChaCha8
        // keystream blocks will be generated.
        const bool spans_two_blocks = bits_of_L < num_output_bits;

        uint8_t ciphertext_bytes[kF1BlockSizeBits / 8];
        Bits output_bits;

        // This counter is used to initialize words 12 and 13 of ChaCha8
        // initial state (4x4 matrix of 32-bit words). This is similar to
        // encrypting plaintext at a given offset, but we have no
        // plaintext, so no XORing at the end.
        chacha8_get_keystream(&this->enc_ctx_, counter, 1, ciphertext_bytes);
        Bits ciphertext0(ciphertext_bytes, block_size_bits / 8, block_size_bits);

        if (spans_two_blocks) {
            // Performs another encryption if necessary
            ++counter;
            chacha8_get_keystream(&this->enc_ctx_, counter, 1, ciphertext_bytes);
            Bits ciphertext1(ciphertext_bytes, block_size_bits / 8, block_size_bits);
            output_bits = ciphertext0.Slice(bits_before_L) +
                          ciphertext1.Slice(0, num_output_bits - bits_of_L);
        } else {
            output_bits = ciphertext0.Slice(bits_before_L, bits_before_L + num_output_bits);
        }

        // Adds the first few bits of L to the end of the output, production k + kExtraBits of
        // output
        Bits extra_data = L.Slice(0, kExtraBits);
        if (extra_data.GetSize() < kExtraBits) {
            extra_data += Bits(0, kExtraBits - extra_data.GetSize());
        }
        return output_bits + extra_data;
    }

    // Returns an evaluation of F1(L), and the metadata (L) that must be stored to evaluate F2.
    inline std::pair<Bits, Bits> CalculateBucket(const Bits& L) const
    {
        return std::make_pair(CalculateF(L), L);
    }

    // Returns an evaluation of F1(L), and the metadata (L) that must be stored to evaluate F2,
    // for 'number_of_evaluations' adjacent inputs.
    inline std::vector<std::pair<Bits, Bits>> CalculateBuckets(
        const Bits& start_L,
        uint64_t number_of_evaluations)
    {
        uint16_t num_output_bits = k_;
        uint16_t block_size_bits = kF1BlockSizeBits;

        uint64_t two_to_the_k = (uint64_t)1 << k_;
        if (start_L.GetValue() + number_of_evaluations > two_to_the_k) {
            throw "Evaluation out of range";
        }
        // Counter for the first input
        uint64_t counter = (start_L.GetValue() * (uint128_t)num_output_bits) / block_size_bits;
        // Counter for the last input
        uint64_t counter_end =
            ((start_L.GetValue() + (uint128_t)number_of_evaluations + 1) * num_output_bits) /
            block_size_bits;

        std::vector<Bits> blocks;
        uint64_t L = (counter * block_size_bits) / num_output_bits;
        uint8_t ciphertext_bytes[kF1BlockSizeBits / 8];

        // Evaluates ChaCha8 for each block
        while (counter <= counter_end) {
            chacha8_get_keystream(&this->enc_ctx_, counter, 1, ciphertext_bytes);
            Bits ciphertext(ciphertext_bytes, block_size_bits / 8, block_size_bits);
            blocks.push_back(std::move(ciphertext));
            ++counter;
        }

        std::vector<std::pair<Bits, Bits>> results;
        uint64_t block_number = 0;
        uint16_t start_bit = (start_L.GetValue() * (uint128_t)num_output_bits) % block_size_bits;

        // For each of the inputs, grabs the correct slice from the encrypted data.
        for (L = start_L.GetValue(); L < start_L.GetValue() + number_of_evaluations; L++) {
            Bits L_bits = Bits(L, k_);

            // Takes the first kExtraBits bits from the input, and adds zeroes if it's not enough
            Bits extra_data = L_bits.Slice(0, kExtraBits);
            if (extra_data.GetSize() < kExtraBits) {
                extra_data = extra_data + Bits(0, kExtraBits - extra_data.GetSize());
            }

            if (start_bit + num_output_bits < block_size_bits) {
                // Everything can be sliced from the current block
                results.push_back(std::make_pair(
                    blocks[block_number].Slice(start_bit, start_bit + num_output_bits) + extra_data,
                    L_bits));
            } else {
                // Must move forward one block
                Bits left = blocks[block_number].Slice(start_bit);
                Bits right = blocks[block_number + 1].Slice(
                    0, num_output_bits - (block_size_bits - start_bit));
                results.push_back(std::make_pair(left + right + extra_data, L_bits));
                ++block_number;
            }
            // Start bit of the output slice in the current block
            start_bit = (start_bit + num_output_bits) % block_size_bits;
        }
        return results;
    }

private:
    // Size of the plot
    uint8_t k_;

    // ChaCha8 context
    struct chacha8_ctx enc_ctx_;
};

struct rmap_item {
    uint16_t count : 4;
    uint16_t pos : 12;
};

// Class to evaluate F2 .. F7.
class FxCalculator {
public:
    FxCalculator() {}

    inline FxCalculator(uint8_t k, uint8_t table_index)
    {
        this->k_ = k;
        this->table_index_ = table_index;

        this->rmap.resize(kBC);
        if (!initialized) {
            initialized = true;
            load_tables();
        }
    }

    inline ~FxCalculator() {}

    // Disable copying
    FxCalculator(const FxCalculator&) = delete;

    inline void ReloadKey() {}

    // Performs one evaluation of the f function.
    inline std::pair<Bits, Bits> CalculateFC(const Bits& L, const Bits& R, const Bits& y1) const
    {
        Bits input;
        uint8_t input_bytes[64];
        uint8_t hash_bytes[32];
        blake3_hasher hasher;
        uint64_t f;
        Bits c;

        if (table_index_ < 4) {
            c = L + R;
            input = y1 + c;
        } else {
            input = y1 + L + R;
        }

        input.ToBytes(input_bytes);

        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, input_bytes, CDIV(input.GetSize(), 8));
        blake3_hasher_finalize(&hasher, hash_bytes, sizeof(hash_bytes));

        f = Util::EightBytesToInt(hash_bytes) >> (64 - (k_ + kExtraBits));

        if (table_index_ < 4) {
            // c is already computed
        } else if (table_index_ < 7) {
            uint8_t len = kVectorLens[table_index_ + 1];
            uint8_t start_byte = (k_ + kExtraBits) / 8;
            uint8_t end_bit = k_ + kExtraBits + k_ * len;
            uint8_t end_byte = CDIV(end_bit, 8);

            // TODO: proper support for partial bytes in Bits ctor
            c = Bits(hash_bytes + start_byte, end_byte - start_byte, (end_byte - start_byte) * 8);

            c = c.Slice((k_ + kExtraBits) % 8, end_bit - start_byte * 8);
        }

        return std::make_pair(Bits(f, k_ + kExtraBits), c);
    }

    // Returns an evaluation of F_i(L), and the metadata (L) that must be stored to evaluate F_i+1.
    inline std::pair<Bits, Bits> CalculateBucket(
        const Bits& y1,
        const Bits& y2,
        const Bits& L,
        const Bits& R)
    {
        return CalculateFC(L, R, y1);
    }

    // Given two buckets with entries (y values), computes which y values match, and returns a list
    // of the pairs of indeces into bucket_L and bucket_R. Indeces l and r match iff:
    //   let  yl = bucket_L[l].y,  yr = bucket_R[r].y
    //
    //   For any 0 <= m < kExtraBitsPow:
    //   yl / kBC + 1 = yR / kBC   AND
    //   (yr % kBC) / kC - (yl % kBC) / kC = m   (mod kB)  AND
    //   (yr % kBC) % kC - (yl % kBC) % kC = (2m + (yl/kBC) % 2)^2   (mod kC)
    //
    // Instead of doing the naive algorithm, which is an O(kExtraBitsPow * N^2) comparisons on
    // bucket length, we can store all the R values and lookup each of our 32 candidates to see if
    // any R value matches. This function can be further optimized by removing the inner loop, and
    // being more careful with memory allocation.
    inline std::vector<std::pair<uint16_t, uint16_t>> FindMatches(
        const std::vector<PlotEntry>& bucket_L,
        const std::vector<PlotEntry>& bucket_R)
    {
        std::vector<std::pair<uint16_t, uint16_t>> matches;
        uint16_t parity = (bucket_L[0].y / kBC) % 2;

        for (size_t i = 0; i < rmap_clean.size(); i++) {
            uint16_t yl = rmap_clean[i];
            this->rmap[yl].count = 0;
        }
        rmap_clean.clear();

        uint64_t remove = (bucket_R[0].y / kBC) * kBC;
        for (size_t pos_R = 0; pos_R < bucket_R.size(); pos_R++) {
            uint64_t r_y = bucket_R[pos_R].y - remove;

            if (!rmap[r_y].count) {
                rmap[r_y].pos = pos_R;
            }
            rmap[r_y].count++;
            rmap_clean.push_back(r_y);
        }

        uint64_t remove_y = remove - kBC;
        for (size_t pos_L = 0; pos_L < bucket_L.size(); pos_L++) {
            uint64_t r = bucket_L[pos_L].y - remove_y;
            for (uint8_t i = 0; i < kExtraBitsPow; i++) {
                uint16_t r_target = L_targets[parity][r][i];
                for (size_t j = 0; j < rmap[r_target].count; j++) {
                    matches.push_back(std::make_pair(pos_L, rmap[r_target].pos + j));
                }
            }
        }

        return matches;
    }

private:
    uint8_t k_;
    uint8_t table_index_;
    std::vector<struct rmap_item> rmap;
    std::vector<uint16_t> rmap_clean;
};

#endif  // SRC_CPP_CALCULATE_BUCKET_HPP_
