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

#include <stdio.h>

#include <set>

#include <catch2/catch.hpp>

#include "../lib/include/picosha2.hpp"
#include "calculate_bucket.hpp"
#include "disk.hpp"
#include "plotter_disk.hpp"
#include "prover_disk.hpp"
#include "serialize.hpp"
#include "sort_manager.hpp"
#include "verifier.hpp"

using namespace std;

uint8_t plot_id_1[] = {35,  2,   52,  4,  51, 55,  23,  84, 91, 10, 111, 12,  13,  222, 151, 16,
                       228, 211, 254, 45, 92, 198, 204, 10, 9,  10, 11,  129, 139, 171, 15,  23};

uint8_t plot_id_3[] = {5,   104, 52,  4,  51, 55,  23,  84, 91, 10, 111, 12,  13,  222, 151, 16,
                       228, 211, 254, 45, 92, 198, 204, 10, 9,  10, 11,  129, 139, 171, 15,  23};

vector<unsigned char> intToBytes(uint32_t paramInt, uint32_t numBytes)
{
    vector<unsigned char> arrayOfByte(numBytes, 0);
    for (uint32_t i = 0; paramInt > 0; i++) {
        arrayOfByte[numBytes - i - 1] = paramInt & 0xff;
        paramInt >>= 8;
    }
    return arrayOfByte;
}

static uint128_t to_uint128(uint64_t hi, uint64_t lo) { return (uint128_t)hi << 64 | lo; }

TEST_CASE("SliceInt64FromBytes 1 bit")
{
    const uint8_t bytes[9 + 7] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9};

    // since we interpret the first 64 bits (8 bytes) as big endian, the
    // first byte is 0x01
    CHECK(Util::SliceInt64FromBytes(bytes, 0, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 1, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 2, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 3, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 4, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 5, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 6, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 7, 1) == 1);

    // the second byte is 0x2
    CHECK(Util::SliceInt64FromBytes(bytes, 8, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 9, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 10, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 11, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 12, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 13, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 14, 1) == 1);
    CHECK(Util::SliceInt64FromBytes(bytes, 15, 1) == 0);

    // the third byte is 0x3
    CHECK(Util::SliceInt64FromBytes(bytes, 16, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 17, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 18, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 19, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 20, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 21, 1) == 0);
    CHECK(Util::SliceInt64FromBytes(bytes, 22, 1) == 1);
    CHECK(Util::SliceInt64FromBytes(bytes, 23, 1) == 1);
}

TEST_CASE("SliceInt64FromBytes 8 bits")
{
    const uint8_t bytes[9 + 7] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9};

    // since we interpret the first 64 bits (8 bytes) as big endian, the
    // first byte is 0x01
    CHECK(Util::SliceInt64FromBytes(bytes, 0, 8) == 0b00000001);
    CHECK(Util::SliceInt64FromBytes(bytes, 1, 8) == 0b00000010);
    CHECK(Util::SliceInt64FromBytes(bytes, 2, 8) == 0b00000100);
    CHECK(Util::SliceInt64FromBytes(bytes, 3, 8) == 0b00001000);
    CHECK(Util::SliceInt64FromBytes(bytes, 4, 8) == 0b00010000);
    CHECK(Util::SliceInt64FromBytes(bytes, 5, 8) == 0b00100000);
    CHECK(Util::SliceInt64FromBytes(bytes, 6, 8) == 0b01000000);
    CHECK(Util::SliceInt64FromBytes(bytes, 7, 8) == 0b10000001);

    CHECK(Util::SliceInt64FromBytes(bytes,  8, 8) == 0b00000010);
    CHECK(Util::SliceInt64FromBytes(bytes,  9, 8) == 0b00000100);
    CHECK(Util::SliceInt64FromBytes(bytes, 10, 8) == 0b00001000);
    CHECK(Util::SliceInt64FromBytes(bytes, 11, 8) == 0b00010000);
    CHECK(Util::SliceInt64FromBytes(bytes, 12, 8) == 0b00100000);
    CHECK(Util::SliceInt64FromBytes(bytes, 13, 8) == 0b01000000);
    CHECK(Util::SliceInt64FromBytes(bytes, 14, 8) == 0b10000000);
    CHECK(Util::SliceInt64FromBytes(bytes, 15, 8) == 0b00000001);

    CHECK(Util::SliceInt64FromBytes(bytes, 16, 8) == 0b00000011);
    CHECK(Util::SliceInt64FromBytes(bytes, 17, 8) == 0b00000110);
    CHECK(Util::SliceInt64FromBytes(bytes, 18, 8) == 0b00001100);
    CHECK(Util::SliceInt64FromBytes(bytes, 19, 8) == 0b00011000);
}

TEST_CASE("SliceInt64FromBytes 24 bits")
{
    const uint8_t bytes[9 + 7] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9};

    // since we interpret the first 64 bits (8 bytes) as big endian, the
    // first byte is 0x01
    CHECK(Util::SliceInt64FromBytes(bytes, 0, 24) == 0b00000001'00000010'00000011);
    CHECK(Util::SliceInt64FromBytes(bytes, 1, 24) == 0b0000001'00000010'00000011'0);
    CHECK(Util::SliceInt64FromBytes(bytes, 2, 24) == 0b000001'00000010'00000011'00);
    CHECK(Util::SliceInt64FromBytes(bytes, 3, 24) == 0b00001'00000010'00000011'000);
    CHECK(Util::SliceInt64FromBytes(bytes, 4, 24) == 0b0001'00000010'00000011'0000);
    CHECK(Util::SliceInt64FromBytes(bytes, 5, 24) == 0b001'00000010'00000011'00000);
    CHECK(Util::SliceInt64FromBytes(bytes, 6, 24) == 0b01'00000010'00000011'000001);
    CHECK(Util::SliceInt64FromBytes(bytes, 7, 24) == 0b1'00000010'00000011'0000010);
}

TEST_CASE("SliceInt64FromBytesFull")
{
    const uint8_t bytes[9 + 7] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9};

    // since we interpret the first 64 bits (8 bytes) as big endian, the
    // first byte is 0x01
    CHECK(Util::SliceInt64FromBytesFull(bytes, 0, 64) == 0x0102030405060708ull);
    CHECK(Util::SliceInt64FromBytesFull(bytes, 1, 64) == 0x0102030405060708ull << 1);
    CHECK(Util::SliceInt64FromBytesFull(bytes, 2, 64) == 0x0102030405060708ull << 2);
    CHECK(Util::SliceInt64FromBytesFull(bytes, 3, 64) == 0x0102030405060708ull << 3);
    CHECK(Util::SliceInt64FromBytesFull(bytes, 4, 64) == 0x1020304050607080ull);
    CHECK(Util::SliceInt64FromBytesFull(bytes, 5, 64) == ((0x1020304050607080ull << 1) | 0b1));
    CHECK(Util::SliceInt64FromBytesFull(bytes, 6, 64) == ((0x1020304050607080ull << 2) | 0b10));
    CHECK(Util::SliceInt64FromBytesFull(bytes, 7, 64) == ((0x1020304050607080ull << 3) | 0b100));
    CHECK(Util::SliceInt64FromBytesFull(bytes, 8, 64) == 0x0203040506070809ull);
}
TEST_CASE("Util")
{
    SECTION("Increment and decrement")
    {
        uint8_t bytes[3 + 7] = {45, 172, 225};
        REQUIRE(Util::SliceInt64FromBytes(bytes, 2, 19) == 374172);
        uint8_t bytes2[1 + 7] = {213};
        REQUIRE(Util::SliceInt64FromBytes(bytes2, 1, 5) == 21);
        uint8_t bytes3[17 + 7] = {1, 2, 3, 4, 5, 6, 7, 255, 255, 10, 11, 12, 13, 14, 15, 16, 255};
        uint128_t int3 = to_uint128(0x01020304050607ff, 0xff0a0b0c0d0e0f10);
        REQUIRE(Util::SliceInt64FromBytes(bytes3, 64, 64) == (uint64_t)int3);
        REQUIRE(Util::SliceInt64FromBytes(bytes3, 0, 60) == (uint64_t)(int3 >> 68));
        REQUIRE(Util::SliceInt128FromBytes(bytes3, 0, 60) == int3 >> 68);
        REQUIRE(Util::SliceInt128FromBytes(bytes3, 7, 64) == int3 >> 57);
        REQUIRE(Util::SliceInt128FromBytes(bytes3, 7, 72) == int3 >> 49);
        REQUIRE(Util::SliceInt128FromBytes(bytes3, 0, 128) == int3);
        REQUIRE(Util::SliceInt128FromBytes(bytes3, 3, 125) == int3);
        REQUIRE(Util::SliceInt128FromBytes(bytes3, 2, 125) == int3 >> 1);
        REQUIRE(Util::SliceInt128FromBytes(bytes3, 0, 120) == int3 >> 8);
        REQUIRE(Util::SliceInt128FromBytes(bytes3, 3, 127) == (int3 << 2 | 3));
    }
}

TEST_CASE("Bits")
{
    SECTION("Slicing and manipulating")
    {
        Bits g = Bits(13271, 15);
        cout << "G: " << g << endl;
        cout << "G Slice: " << g.Slice(4, 9) << endl;
        cout << "G Slice: " << g.Slice(0, 9) << endl;
        cout << "G Slice: " << g.Slice(9, 10) << endl;
        cout << "G Slice: " << g.Slice(9, 15) << endl;
        cout << "G Slice: " << g.Slice(9, 9) << endl;
        REQUIRE(g.Slice(9, 9) == Bits());

        uint8_t bytes[2];
        g.ToBytes(bytes);
        cout << "bytes: " << static_cast<int>(bytes[0]) << " " << static_cast<int>(bytes[1])
             << endl;
        cout << "Back to Bits: " << Bits(bytes, 2, 16) << endl;

        Bits(256, 9).ToBytes(bytes);
        cout << "bytes: " << static_cast<int>(bytes[0]) << " " << static_cast<int>(bytes[1])
             << endl;
        cout << "Back to Bits: " << Bits(bytes, 2, 16) << endl;

        cout << Bits(640, 11) << endl;
        Bits(640, 11).ToBytes(bytes);
        cout << "bytes: " << static_cast<int>(bytes[0]) << " " << static_cast<int>(bytes[1])
             << endl;

        Bits h = Bits(bytes, 2, 16);
        Bits i = Bits(bytes, 2, 17);
        cout << "H: " << h << endl;
        cout << "I: " << i << endl;

        cout << "G: " << g << endl;
        cout << "size: " << g.GetSize() << endl;

        Bits shifted = (g << 150);

        REQUIRE(shifted.GetSize() == 15);
        REQUIRE(shifted.ToString() == "000000000000000");

        Bits large = Bits(13271, 200);
        REQUIRE(large == ((large << 160)) >> 160);
        REQUIRE((large << 160).GetSize() == 200);

        Bits l = Bits(123287490 & ((1U << 20) - 1), 20);
        l = l + Bits(0, 5);

        Bits m = Bits(5, 3);
        uint8_t buf[1];
        m.ToBytes(buf);
        REQUIRE(buf[0] == (5 << 5));
    }
    SECTION("Park Bits")
    {
        uint32_t const num_bytes = 16000;
        uint8_t buf[num_bytes];
        uint8_t buf_2[num_bytes];
        Util::GetRandomBytes(buf, num_bytes);
        ParkBits my_bits = ParkBits(buf, num_bytes, num_bytes * 8);
        my_bits.ToBytes(buf_2);
        for (uint32_t i = 0; i < num_bytes; i++) {
            REQUIRE(buf[i] == buf_2[i]);
        }
    }

    SECTION("Large Bits")
    {
        uint32_t const num_bytes = 200000;
        uint8_t buf[num_bytes];
        uint8_t buf_2[num_bytes];
        Util::GetRandomBytes(buf, num_bytes);
        LargeBits my_bits = LargeBits(buf, num_bytes, num_bytes * 8);
        my_bits.ToBytes(buf_2);
        for (uint32_t i = 0; i < num_bytes; i++) {
            REQUIRE(buf[i] == buf_2[i]);
        }
    }
}

bool CheckMatch(int64_t yl, int64_t yr)
{
    int64_t bl = yl / kBC;
    int64_t br = yr / kBC;
    if (bl + 1 != br)
        return false;  // Buckets don't match
    for (int64_t m = 0; m < kExtraBitsPow; m++) {
        if ((((yr % kBC) / kC - ((yl % kBC) / kC)) - m) % kB == 0) {
            int64_t c_diff = 2 * m + bl % 2;
            c_diff *= c_diff;

            if ((((yr % kBC) % kC - ((yl % kBC) % kC)) - c_diff) % kC == 0) {
                return true;
            }
        }
    }
    return false;
}

// Get next set in the Cartesian product of k ranges of [0, n - 1], similar to
// k nested 'for' loops from 0 to n - 1
static int CartProdNext(uint8_t* items, uint8_t n, uint8_t k, bool init)
{
    uint8_t i;

    if (init) {
        memset(items, 0, k);
        return 0;
    }

    items[0]++;
    for (i = 0; i < k; i++) {
        if (items[i] == n) {
            items[i] = 0;
            if (i == k - 1) {
                return -1;
            }
            items[i + 1]++;
        } else {
            break;
        }
    }

    return 0;
}

static int sq(int n) { return n * n; }

static bool Have4Cycles(uint32_t extraBits, int B, int C)
{
    uint8_t m[4];
    bool init = true;

    while (!CartProdNext(m, 1 << extraBits, 4, init)) {
        uint8_t r1 = m[0], r2 = m[1], s1 = m[2], s2 = m[3];

        init = false;
        if (r1 != s1 && (r1 << extraBits) + r2 != (s2 << extraBits) + s1 &&
            (r1 - s1 + r2 - s2) % B == 0) {
            uint8_t p[2];
            bool initp = true;

            while (!CartProdNext(p, 2, 2, initp)) {
                uint8_t p1 = p[0], p2 = p[1];
                int lhs = sq(2 * r1 + p1) - sq(2 * s1 + p1) + sq(2 * r2 + p2) - sq(2 * s2 + p2);

                initp = false;
                if (lhs % C == 0) {
                    fprintf(stderr, "%d %d %d %d %d %d\n", r1, r2, s1, s2, p1, p2);
                    return true;
                }
            }
        }
    }

    return false;
}

TEST_CASE("Matching function")
{
    SECTION("Cycles") { REQUIRE(!Have4Cycles(kExtraBits, kB, kC)); }
}

void VerifyFC(uint8_t t, uint8_t k, uint64_t L, uint64_t R, uint64_t y1, uint64_t y, uint64_t c)
{
    uint8_t sizes[] = {1, 2, 4, 4, 3, 2};
    uint8_t size = sizes[t - 2];
    FxCalculator fcalc(k, t);

    std::pair<Bits, Bits> res = fcalc.CalculateBucket(
        Bits(y1, k + kExtraBits), Bits(L, k * size), Bits(R, k * size));
    REQUIRE(res.first.GetValue() == y);
    if (c) {
        REQUIRE(res.second.GetValue() == c);
    }
}

TEST_CASE("F functions")
{
    SECTION("F1")
    {
        uint8_t test_k = 35;
        uint8_t test_key[] = {0, 2, 3, 4,  5, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                              1, 2, 3, 41, 5, 6, 7, 8, 9, 10, 11, 12, 13, 11, 15, 16};
        F1Calculator f1(test_k, test_key);

        Bits L = Bits(525, test_k);
        pair<Bits, Bits> result1 = f1.CalculateBucket(L);
        Bits L2 = Bits(526, test_k);
        pair<Bits, Bits> result2 = f1.CalculateBucket(L2);
        Bits L3 = Bits(625, test_k);
        pair<Bits, Bits> result3 = f1.CalculateBucket(L3);

        uint64_t results[256];
        f1.CalculateBuckets(L.GetValue(), 101, results);
        REQUIRE(result1.first.GetValue() == results[0]);
        REQUIRE(result2.first.GetValue() == results[1]);
        REQUIRE(result3.first.GetValue() == results[100]);

        uint32_t max_batch = 1 << kBatchSizes;
        test_k = 32;
        F1Calculator f1_2(test_k, test_key);
        L = Bits(192837491, test_k);
        result1 = f1_2.CalculateBucket(L);
        L2 = Bits(192837491 + 1, test_k);
        result2 = f1_2.CalculateBucket(L2);
        L3 = Bits(192837491 + 2, test_k);
        result3 = f1_2.CalculateBucket(L3);
        Bits L4 = Bits(192837491 + max_batch - 1, test_k);
        pair<Bits, Bits> result4 = f1_2.CalculateBucket(L4);

        f1_2.CalculateBuckets(L.GetValue(), max_batch, results);
        REQUIRE(result1.first.GetValue() == results[0]);
        REQUIRE(result2.first.GetValue() == results[1]);
        REQUIRE(result3.first.GetValue() == results[2]);
        REQUIRE(result4.first.GetValue() == results[max_batch - 1]);
    }

    SECTION("F2")
    {
        uint8_t test_key_2[] = {20,  2,  5,  4,   51, 52,  23,  84,  91, 10, 111,
                                12,  13, 24, 151, 16, 228, 211, 254, 45, 92, 198,
                                204, 10, 9,  10,  11, 129, 139, 171, 15, 18};
        map<uint64_t, vector<pair<Bits, Bits>>> buckets;

        uint8_t const k = 12;
        uint64_t num_buckets = (1ULL << (k + kExtraBits)) / kBC + 1;
        uint64_t x = 0;

        F1Calculator f1(k, test_key_2);
        for (uint32_t j = 0; j < (1ULL << (k - 4)) + 1; j++) {
            uint64_t y[1 << 4];

            f1.CalculateBuckets(x, 1U << 4, y);
            for (int i = 0; i < 1 << 4; i++) {
                uint64_t bucket = y[i] / kBC;
                if (buckets.find(bucket) == buckets.end()) {
                    buckets[bucket] = vector<std::pair<Bits, Bits>>();
                }
                buckets[bucket].push_back(std::make_pair(Bits(y[i], k + kExtraBits), Bits(x, k)));
                if (x + 1 > (1ULL << k) - 1) {
                    break;
                }
                ++x;
            }
            if (x + 1 > (1ULL << k) - 1) {
                break;
            }
        }

        FxCalculator f2(k, 2);
        int total_matches = 0;

        for (auto kv : buckets) {
            if (kv.first == num_buckets - 1) {
                continue;
            }
            auto bucket_elements_2 = buckets[kv.first + 1];
            vector<PlotEntry> left_bucket;
            vector<PlotEntry> right_bucket;
            for (auto yx1 : kv.second) {
                PlotEntry e;
                e.y = get<0>(yx1).GetValue();
                left_bucket.push_back(e);
            }
            for (auto yx2 : buckets[kv.first + 1]) {
                PlotEntry e;
                e.y = get<0>(yx2).GetValue();
                right_bucket.push_back(e);
            }
            sort(
                left_bucket.begin(),
                left_bucket.end(),
                [](const PlotEntry& a, const PlotEntry& b) -> bool { return a.y > b.y; });
            sort(
                right_bucket.begin(),
                right_bucket.end(),
                [](const PlotEntry& a, const PlotEntry& b) -> bool { return a.y > b.y; });

            uint16_t idx_L[10000];
            uint16_t idx_R[10000];

            int32_t idx_count = f2.FindMatches(left_bucket, right_bucket, idx_L, idx_R);
            for(int32_t i=0; i < idx_count; i++) {
                REQUIRE(CheckMatch(left_bucket[idx_L[i]].y, right_bucket[idx_R[i]].y));
            }
            total_matches += idx_count;
        }
        REQUIRE(total_matches > (1 << k) / 2);
        REQUIRE(total_matches < (1 << k) * 2);
    }

    SECTION("Fx")
    {
        VerifyFC(2, 16, 0x44cb, 0x204f, 0x20a61a, 0x2af546, 0x44cb204f);
        VerifyFC(2, 16, 0x3c5f, 0xfda9, 0x3988ec, 0x15293b, 0x3c5ffda9);
        VerifyFC(3, 16, 0x35bf992d, 0x7ce42c82, 0x31e541, 0xf73b3, 0x35bf992d7ce42c82);
        VerifyFC(3, 16, 0x7204e52d, 0xf1fd42a2, 0x28a188, 0x3fb0b5, 0x7204e52df1fd42a2);
        VerifyFC(
            4, 16, 0x5b6e6e307d4bedc, 0x8a9a021ea648a7dd, 0x30cb4c, 0x11ad5, 0xd4bd0b144fc26138);
        VerifyFC(
            4, 16, 0xb9d179e06c0fd4f5, 0xf06d3fef701966a0, 0x1dd5b6, 0xe69a2, 0xd02115f512009d4d);
        VerifyFC(5, 16, 0xc2cd789a380208a9, 0x19999e3fa46d6753, 0x25f01e, 0x1f22bd, 0xabe423040a33);
        VerifyFC(5, 16, 0xbe3edc0a1ef2a4f0, 0x4da98f1d3099fdf5, 0x3feb18, 0x31501e, 0x7300a3a03ac5);
        VerifyFC(6, 16, 0xc965815a47c5, 0xf5e008d6af57, 0x1f121a, 0x1cabbe, 0xc8cc6947);
        VerifyFC(6, 16, 0xd420677f6cbd, 0x5894aa2ca1af, 0x2efde9, 0xc2121, 0x421bb8ec);
        VerifyFC(7, 16, 0x5fec898f, 0x82283d15, 0x14f410, 0x24c3c2, 0x0);
        VerifyFC(7, 16, 0x64ac5db9, 0x7923986, 0x590fd, 0x1c74a2, 0x0);
    }
}

TEST_CASE("(De)Serialization")
{
    Serializer serializer;
    Deserializer deserializer(serializer.Data());
    SECTION("Basics")
    {
        serializer.Reset();
        for (uint8_t i = 0; i < 0xFF; ++i) {
            serializer << i;
        }
        uint8_t n{0}, n_deserialized;
        do{
            deserializer >> n_deserialized;
            REQUIRE(n_deserialized == n++);
        } while (!deserializer.End());
        serializer.Reset();
        deserializer.Reset();
        std::vector<float> vecFloat;
        REQUIRE_THROWS(deserializer >> vecFloat); // Unexpected vector, leads to read out of bounds
        float f = 0.123, f_deserialized = 0;
        double d = 1.23, d_deserialized = 0;
        int64_t i = -123, i_deserialized = 0;
        uint64_t u = 123, u_deserialized = 0;
        serializer << f;
        deserializer >> f_deserialized;
        REQUIRE(f_deserialized == f);
        serializer << d;
        deserializer >> d_deserialized;
        REQUIRE(d_deserialized == d);
        serializer << i;
        deserializer >> i_deserialized;
        REQUIRE(i_deserialized == i);
        serializer << u;
        deserializer >> u_deserialized;
        REQUIRE(u_deserialized == u);
        REQUIRE(deserializer.End());
        deserializer.Reset();
        deserializer >> f_deserialized;
        deserializer >> d_deserialized;
        deserializer >> i_deserialized;
        deserializer >> u_deserialized;
        REQUIRE(f_deserialized == f);
        REQUIRE(d_deserialized == d);
        REQUIRE(i_deserialized == i);
        REQUIRE(u_deserialized == u);
        REQUIRE(deserializer.End());
        REQUIRE_THROWS(deserializer >> f_deserialized); // Read out of bounds
    }
    SECTION("vector")
    {
        std::vector<uint64_t> vec1{1,2,3}, vec2;
        std::vector<std::vector<uint64_t>> vec3{{1}, {2,3}, {4,5,6}}, vec4;
        serializer << vec1;
        serializer << vec2;
        serializer << vec3;
        serializer << vec4;
        std::vector<uint64_t> vec1_deserialized, vec2_deserialized;
        std::vector<std::vector<uint64_t>> vec3_deserialized, vec4_deserialized;
        deserializer >> vec1_deserialized;
        deserializer >> vec2_deserialized;
        deserializer >> vec3_deserialized;
        deserializer >> vec4_deserialized;
        REQUIRE(vec1_deserialized == vec1);
        REQUIRE(vec2_deserialized == vec2);
        REQUIRE(vec3_deserialized == vec3);
        REQUIRE(vec4_deserialized == vec4);
        REQUIRE(deserializer.End());
    }
    SECTION("string")
    {
        std::string str1 = "123", str2;
        serializer << str1;
        serializer << str2;
        std::string str1_deserialized, str2_deserialized;
        deserializer >> str1_deserialized;
        deserializer >> str2_deserialized;
        REQUIRE(str1_deserialized == str1);
        REQUIRE(str2_deserialized == str2);
        REQUIRE(deserializer.End());
    }
    SECTION("DiskProver")
    {
        std::string filename = "prover_test.plot";
        DiskPlotter plotter = DiskPlotter();
        uint8_t memo[5] = {1, 2, 3, 4, 5};
        plotter.CreatePlotDisk(
            ".", ".", ".", filename, 18, memo, 5, plot_id_1, 32, 11, 0, 4000, 2);
        DiskProver prover1(filename);
        std::vector<uint8_t> vecBytes = prover1.ToBytes();
        DiskProver prover2(vecBytes);
        REQUIRE(prover1.GetFilename() == prover2.GetFilename());
        REQUIRE(prover1.GetSize() == prover2.GetSize());
        REQUIRE(prover1.GetId() == prover2.GetId());
        REQUIRE(prover1.GetMemo() == prover2.GetMemo());
        vector<unsigned char> hash_input = intToBytes(0, 4);
        vector<unsigned char> hash(picosha2::k_digest_size);
        picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());
        vector<LargeBits> qualities1 = prover1.GetQualitiesForChallenge(hash.data());
        LargeBits proof1 = prover1.GetFullProof(hash.data(), 0);
        vector<LargeBits> qualities2 = prover2.GetQualitiesForChallenge(hash.data());
        LargeBits proof2 = prover2.GetFullProof(hash.data(), 0);
        REQUIRE(qualities1 == qualities2);
        REQUIRE(proof1 == proof2);
        vecBytes[0] = 0x02; // Change version
        REQUIRE_THROWS(DiskProver(vecBytes)); // Invalid version
        REQUIRE(remove(filename.c_str()) == 0);
    }
}

void HexToBytes(const string& hex, uint8_t* result)
{
    for (unsigned int i = 0; i < hex.length(); i += 2) {
        string byteString = hex.substr(i, 2);
        uint8_t byte = (uint8_t)strtol(byteString.c_str(), NULL, 16);
        result[i / 2] = byte;
    }
}

void TestProofOfSpace(
    std::string filename,
    uint32_t iterations,
    uint8_t k,
    uint8_t* plot_id,
    uint32_t num_proofs)
{
    DiskProver prover(filename);
    uint8_t* proof_data = new uint8_t[8 * k];
    uint32_t success = 0;
    // Tries an edge case challenge with many 1s in the front, and ensures there is no segfault
    vector<unsigned char> hash(picosha2::k_digest_size);
    HexToBytes("fffffa2b647d4651c500076d7df4c6f352936cf293bd79c591a7b08e43d6adfb", hash.data());
    prover.GetQualitiesForChallenge(hash.data());

    for (uint32_t i = 0; i < iterations; i++) {
        vector<unsigned char> hash_input = intToBytes(i, 4);
        vector<unsigned char> hash(picosha2::k_digest_size);
        picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());
        vector<LargeBits> qualities = prover.GetQualitiesForChallenge(hash.data());
        Verifier verifier = Verifier();
            
        for (uint32_t index = 0; index < qualities.size(); index++) {
            LargeBits proof = prover.GetFullProof(hash.data(), index);
            proof.ToBytes(proof_data);

            LargeBits quality = verifier.ValidateProof(plot_id, k, hash.data(), proof_data, k * 8);
            REQUIRE(quality.GetSize() == 256);
            REQUIRE(quality == qualities[index]);
            success += 1;

            // Tests invalid proof
            proof_data[0] = (proof_data[0] + 1) % 256;
            LargeBits quality_2 =
                verifier.ValidateProof(plot_id, k, hash.data(), proof_data, k * 8);
            REQUIRE(quality_2.GetSize() == 0);
        }
    }
    std::cout << "Success: " << success << "/" << iterations << " "
              << (100 * ((double)success / (double)iterations)) << "%" << std::endl;
    REQUIRE(success == num_proofs);
    REQUIRE(success > 0.5 * iterations);
    REQUIRE(success < 1.5 * iterations);
    delete[] proof_data;
}

void PlotAndTestProofOfSpace(
    std::string filename,
    uint32_t iterations,
    uint8_t k,
    uint8_t* plot_id,
    uint32_t buffer,
    uint32_t num_proofs,
    uint32_t stripe_size,
    uint8_t num_threads)
{
    DiskPlotter plotter = DiskPlotter();
    uint8_t memo[5] = {1, 2, 3, 4, 5};
    plotter.CreatePlotDisk(
        ".", ".", ".", filename, k, memo, 5, plot_id, 32, buffer, 0, stripe_size, num_threads);
    TestProofOfSpace(filename, iterations, k, plot_id, num_proofs);
    REQUIRE(remove(filename.c_str()) == 0);
}

TEST_CASE("Plotting")
{
    SECTION("Disk plot k18")
    {
        PlotAndTestProofOfSpace("cpp-test-plot.dat", 100, 18, plot_id_1, 11, 95, 4000, 2);
    }
    SECTION("Disk plot k19")
    {
        PlotAndTestProofOfSpace("cpp-test-plot.dat", 100, 19, plot_id_1, 100, 71, 8192, 2);
    }
    SECTION("Disk plot k19 single-thread")
    {
        PlotAndTestProofOfSpace("cpp-test-plot.dat", 100, 19, plot_id_1, 100, 71, 8192, 1);
    }
    SECTION("Disk plot k20")
    {
        PlotAndTestProofOfSpace("cpp-test-plot.dat", 500, 20, plot_id_3, 100, 469, 16000, 2);
    }
    SECTION("Disk plot k21")
    {
        PlotAndTestProofOfSpace("cpp-test-plot.dat", 5000, 21, plot_id_3, 100, 4945, 8192, 4);
    }
    // SECTION("Disk plot k24") { PlotAndTestProofOfSpace("cpp-test-plot.dat", 100, 24, plot_id_3,
    // 100, 107); }
}

TEST_CASE("Invalid plot")
{
    SECTION("File gets deleted")
    {
        string filename = "invalid-plot.dat";
        {
            DiskPlotter plotter = DiskPlotter();
            uint8_t memo[5] = {1, 2, 3, 4, 5};
            uint8_t k = 20;
            plotter.CreatePlotDisk(".", ".", ".", filename, k, memo, 5, plot_id_1, 32, 200, 32, 8192, 2);
            DiskProver prover(filename);
            uint8_t* proof_data = new uint8_t[8 * k];
            uint8_t challenge[32];
            size_t i;
            memset(challenge, 155, 32);
            vector<LargeBits> qualities;
            for (i = 0; i < 50; i++) {
                qualities = prover.GetQualitiesForChallenge(challenge);
                if (qualities.size())
                    break;
                challenge[0]++;
            }
            Verifier verifier = Verifier();
            REQUIRE(qualities.size() > 0);
            for (uint32_t index = 0; index < qualities.size(); index++) {
                LargeBits proof = prover.GetFullProof(challenge, index);
                proof.ToBytes(proof_data);
                LargeBits quality =
                    verifier.ValidateProof(plot_id_1, k, challenge, proof_data, k * 8);
                REQUIRE(quality == qualities[index]);
            }
            delete[] proof_data;
        }
        REQUIRE(remove(filename.c_str()) == 0);
        REQUIRE_THROWS_WITH([&]() { DiskProver p(filename); }(), "Invalid file " + filename);
    }
}

TEST_CASE("Sort on disk")
{
    SECTION("ExtractNum")
    {
        for (int i = 0; i < 15 * 8 - 5; i++) {
            uint8_t buf[15 + 7];
            Bits((uint128_t)27 << i, 15 * 8).ToBytes(buf);

            REQUIRE(Util::ExtractNum(buf, 15, 15 * 8 - 4 - i, 3) == 5);
        }
        uint8_t buf[16 + 7];
        Bits((uint128_t)27 << 5, 128).ToBytes(buf);
        REQUIRE(Util::ExtractNum(buf, 16, 100, 200) == 864);
    }

    SECTION("MemCmpBits")
    {
        uint8_t left[3];
        left[0] = 12;
        left[1] = 10;
        left[2] = 100;

        uint8_t right[3];
        right[0] = 12;
        right[1] = 10;
        right[2] = 100;

        REQUIRE(Util::MemCmpBits(left, right, 3, 0) == 0);
        REQUIRE(Util::MemCmpBits(left, right, 3, 10) == 0);

        right[1] = 11;
        REQUIRE(Util::MemCmpBits(left, right, 3, 0) < 0);
        REQUIRE(Util::MemCmpBits(left, right, 3, 16) == 0);

        right[1] = 9;
        REQUIRE(Util::MemCmpBits(left, right, 3, 0) > 0);

        right[1] = 10;

        // Last bit differs
        right[2] = 101;
        REQUIRE(Util::MemCmpBits(left, right, 3, 0) < 0);
    }

    SECTION("Quicksort")
    {
        uint32_t const iters = 100;
        vector<string> hashes;
        uint8_t* hashes_bytes = new uint8_t[iters * 16];
        memset(hashes_bytes, 0, iters * 16);

        srand(0);
        for (uint32_t i = 0; i < iters; i++) {
            // reverting to rand()
            string to_insert = std::to_string(rand());
            while (to_insert.length() < 16) {
                to_insert += "0";
            }
            hashes.push_back(to_insert);
            memcpy(hashes_bytes + i * 16, to_insert.data(), to_insert.length());
        }
        sort(hashes.begin(), hashes.end());
        QuickSort::Sort(hashes_bytes, 16, iters, 0);

        for (uint32_t i = 0; i < iters; i++) {
            std::string str(reinterpret_cast<char*>(hashes_bytes) + i * 16, 16);
            REQUIRE(str.compare(hashes[i]) == 0);
        }
        delete[] hashes_bytes;
    }

    SECTION("File disk")
    {
        FileDisk d = FileDisk("test_file.bin");
        uint8_t buf[5] = {1, 2, 3, 5, 7};
        d.Write(250, buf, 5);

        uint8_t read_buf[5];
        d.Read(250, read_buf, 5);

        REQUIRE(memcmp(buf, read_buf, 5) == 0);
        remove("test_file.bin");
    }

    SECTION("Lazy Sort Manager QS")
    {
        uint32_t iters = 250000;
        uint32_t const size = 32;
        vector<Bits> input;
        const uint32_t memory_len = 1000000;
        SortManager manager(memory_len, 16, 4, size, ".", "test-files", 0, 1);
        int total_written_1 = 0;
        for (uint32_t i = 0; i < iters; i++) {
            vector<unsigned char> hash_input = intToBytes(i, 4);
            vector<unsigned char> hash(picosha2::k_digest_size);
            picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());
            total_written_1 += size;
            Bits to_write = Bits(hash.data(), size, size * 8);
            input.emplace_back(to_write);
            manager.AddToCache(to_write);
        }
        manager.FlushCache();
        uint8_t buf[size];
        sort(input.begin(), input.end());
        uint8_t* buf3;
        for (uint32_t i = 0; i < iters; i++) {
            buf3 = manager.ReadEntry(i * size);
            input[i].ToBytes(buf);
            REQUIRE(memcmp(buf, buf3, size) == 0);
        }
    }

    SECTION("Lazy Sort Manager uniform sort")
    {
        uint32_t iters = 120000;
        uint32_t const size = 32;
        vector<Bits> input;
        const uint32_t memory_len = 1000000;
        SortManager manager(memory_len, 16, 4, size, ".", "test-files", 0, 1);
        int total_written_1 = 0;
        for (uint32_t i = 0; i < iters; i++) {
            vector<unsigned char> hash_input = intToBytes(i, 4);
            vector<unsigned char> hash(picosha2::k_digest_size);
            picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());
            total_written_1 += size;
            Bits to_write = Bits(hash.data(), size, size * 8);
            input.emplace_back(to_write);
            manager.AddToCache(to_write);
        }
        manager.FlushCache();
        uint8_t buf[size];
        sort(input.begin(), input.end());
        uint8_t* buf3;
        for (uint32_t i = 0; i < iters; i++) {
            buf3 = manager.ReadEntry(i * size);
            input[i].ToBytes(buf);
            REQUIRE(memcmp(buf, buf3, size) == 0);
        }
    }

    SECTION("Sort in Memory")
    {
        uint32_t iters = 100000;
        uint32_t const size = 32;
        vector<Bits> input;
        uint32_t begin = 1000;
        FileDisk disk("test_file.bin");

        for (uint32_t i = 0; i < iters; i++) {
            vector<unsigned char> hash_input = intToBytes(i, 4);
            vector<unsigned char> hash(picosha2::k_digest_size);
            picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());
            hash[0] = hash[1] = 0;
            disk.Write(begin + i * size, hash.data(), size);
            input.emplace_back(Bits(hash.data(), size, size * 8));
        }

        const uint32_t memory_len = Util::RoundSize(iters) * size;
        auto memory = std::make_unique<uint8_t[]>(memory_len);
        UniformSort::SortToMemory(disk, begin, memory.get(), size, iters, 16);

        sort(input.begin(), input.end());
        uint8_t buf[size];
        for (uint32_t i = 0; i < iters; i++) {
            input[i].ToBytes(buf);
            REQUIRE(memcmp(buf, memory.get() + i * size, size) == 0);
        }
    }
}

TEST_CASE("bitfield-simple")
{
    bitfield b(4);
    CHECK(!b.get(0));
    CHECK(!b.get(1));
    CHECK(!b.get(2));
    CHECK(!b.get(3));

    b.set(0);
    CHECK(b.get(0));
    CHECK(!b.get(1));
    CHECK(!b.get(2));
    CHECK(!b.get(3));

    b.set(1);
    CHECK(b.get(0));
    CHECK(b.get(1));
    CHECK(!b.get(2));
    CHECK(!b.get(3));

    b.set(3);
    CHECK(b.get(0));
    CHECK(b.get(1));
    CHECK(!b.get(2));
    CHECK(b.get(3));
}

TEST_CASE("bitfield-count")
{
    bitfield b(512);

    for (int i = 0; i < 512; ++i) {
        CHECK(b.count(0, 512) == i);
        CHECK(!b.get(i));
        b.set(i);
        CHECK(b.get(i));
    }
    CHECK(b.count(0, 512) == 512);
}

TEST_CASE("bitfield-count-unaligned")
{
    bitfield b(512);

    for (int i = 0; i < 512; ++i) {
        b.set(i);
    }

    for (int i = 0; i < 512; ++i) {
        CHECK(b.count(0, i) == i);
    }
}

TEST_CASE("bitfield_index-simple")
{
    bitfield b(64);
    b.set(0);
    b.set(1);
    b.set(3);
    bitfield_index const idx(b);
    CHECK(idx.lookup(0, 0) == std::pair<uint64_t, uint64_t>{0,0});
    CHECK(idx.lookup(0, 1) == std::pair<uint64_t, uint64_t>{0,1});

    CHECK(idx.lookup(0, 3) == std::pair<uint64_t, uint64_t>{0,2});

    CHECK(idx.lookup(1, 0) == std::pair<uint64_t, uint64_t>{1,0});
    CHECK(idx.lookup(1, 2) == std::pair<uint64_t, uint64_t>{1,1});
    CHECK(idx.lookup(3, 0) == std::pair<uint64_t, uint64_t>{2,0});
}

TEST_CASE("bitfield_index-use index")
{
    bitfield b(1048576);
    CHECK(b.size() == 1048576);
    b.set(1048576 - 3);
    b.set(1048576 - 2);
    b.set(1048576 - 1);
    bitfield_index const idx(b);
    CHECK(idx.lookup(1048576 - 3, 1) == std::pair<uint64_t, uint64_t>{0,1});
    CHECK(idx.lookup(1048576 - 2, 1) == std::pair<uint64_t, uint64_t>{1,1});
}

TEST_CASE("bitfield_index edge-cases")
{
    bitfield b(1048576);
    CHECK(b.size() == 1048576);
    b.set(0);
    b.set(bitfield_index::kIndexBucket);
    b.set(bitfield_index::kIndexBucket * 2);
    b.set(1048576 - 1);
    bitfield_index const idx(b);
    CHECK(idx.lookup(0, 0) == std::pair<uint64_t, uint64_t>{0,0});
    CHECK(idx.lookup(0, bitfield_index::kIndexBucket) == std::pair<uint64_t, uint64_t>{0,1});
    CHECK(idx.lookup(0, bitfield_index::kIndexBucket * 2) == std::pair<uint64_t, uint64_t>{0,2});
    CHECK(idx.lookup(0, 1048576 - 1) == std::pair<uint64_t, uint64_t>{0,3});

    CHECK(idx.lookup(bitfield_index::kIndexBucket, 0) == std::pair<uint64_t, uint64_t>{1,0});
    CHECK(idx.lookup(bitfield_index::kIndexBucket, bitfield_index::kIndexBucket) == std::pair<uint64_t, uint64_t>{1,1});
    CHECK(idx.lookup(bitfield_index::kIndexBucket, 1048576 - 1 - bitfield_index::kIndexBucket)
        == std::pair<uint64_t, uint64_t>{1,2});

    CHECK(idx.lookup(bitfield_index::kIndexBucket * 2, 1048576 - 1 - bitfield_index::kIndexBucket * 2)
        == std::pair<uint64_t, uint64_t>{2,1});
    CHECK(idx.lookup(1048576 - 1, 0) == std::pair<uint64_t, uint64_t>{3,0});
}

void test_bitfield_size(int const size)
{
    bitfield b(size);
    b.set(0);
    b.set(size - 1);
    bitfield_index const idx(b);
    CHECK(idx.lookup(0, 0) == std::pair<uint64_t, uint64_t>{0,0});
    CHECK(idx.lookup(0, size - 1) == std::pair<uint64_t, uint64_t>{0,1});
    CHECK(idx.lookup(size - 1, 0) == std::pair<uint64_t, uint64_t>{1,0});
}

TEST_CASE("bitfield_index edge-sizes")
{
    test_bitfield_size(bitfield_index::kIndexBucket - 1);
    test_bitfield_size(bitfield_index::kIndexBucket);
    test_bitfield_size(bitfield_index::kIndexBucket + 1);
}

namespace {

constexpr int num_test_entries = 2000000;

void write_disk_file(FileDisk& df)
{
    std::uint32_t val = 0;
    for (int i = 0; i < num_test_entries; ++i) {
        df.Write(i * 4, reinterpret_cast<std::uint8_t const*>(&val), 4);
        ++val;
    }
}

}

TEST_CASE("FileDisk")
{
    FileDisk d = FileDisk("test_file.bin");
    write_disk_file(d);

    std::uint32_t val = 0;
    for (uint32_t i = 0; i < num_test_entries; ++i) {
        d.Read(i * 4, reinterpret_cast<std::uint8_t*>(&val), 4);
        REQUIRE(i == val);
    }

    for (uint32_t i = num_test_entries - 1; i > 0; --i) {
        d.Read(i * 4, reinterpret_cast<std::uint8_t*>(&val), 4);
        CHECK(i == val);
    }

    remove("test_file.bin");
}

TEST_CASE("BufferedDisk")
{
    FileDisk d = FileDisk("test_file.bin");
    write_disk_file(d);

    BufferedDisk bd(&d, num_test_entries * 4);

    for (uint32_t i = 0; i < num_test_entries; ++i) {
        auto const val = *reinterpret_cast<std::uint32_t const*>(bd.Read(i * 4, 4));
        CHECK(i == val);
    }

    // don't go all the way down to 0, every backwards read cursor movement will
    // print a warning
    for (uint32_t i = num_test_entries - 1; i > num_test_entries / 2 + 200; --i) {
        auto const val = *reinterpret_cast<std::uint32_t const*>(bd.Read(i * 4, 4));
        CHECK(i == val);
    }

    remove("test_file.bin");
}

TEST_CASE("DiskProver")
{
    SECTION("Move constructor")
    {
        std::string filename = "prover_test.plot";
        DiskPlotter plotter = DiskPlotter();
        std::vector<uint8_t> memo{1, 2, 3};
        plotter.CreatePlotDisk(
            ".", ".", ".", filename, 18, memo.data(),
            memo.size(), plot_id_1, 32, 11, 0,
            4000, 2);
        DiskProver prover1(filename);
        auto* p1_filename_ptr = prover1.GetFilename().data();
        auto* p1_memo_ptr = prover1.GetMemo().data();
        auto* p1_id_ptr = prover1.GetId().data();
        auto* p1_table_begin_pointers_ptr = prover1.GetTableBeginPointers().data();
        auto* p1_C2_ptr = prover1.GetC2().data();
        DiskProver prover2(std::move(prover1));
        REQUIRE(prover2.GetFilename().data() == p1_filename_ptr);
        REQUIRE(prover2.GetMemo().data() == p1_memo_ptr);
        REQUIRE(prover2.GetId().data() == p1_id_ptr);
        REQUIRE(prover2.GetTableBeginPointers().data() == p1_table_begin_pointers_ptr);
        REQUIRE(prover2.GetC2().data() == p1_C2_ptr);
        REQUIRE(prover1.GetFilename().empty());
        REQUIRE(prover1.GetMemo().empty());
        REQUIRE(prover1.GetId().empty());
        REQUIRE(prover1.GetSize() == prover1.GetSize());
        REQUIRE(prover1.GetTableBeginPointers().empty());
        REQUIRE(prover1.GetC2().empty());
    }
}

TEST_CASE("FilteredDisk")
{
    FileDisk d = FileDisk("test_file.bin");
    write_disk_file(d);

    SECTION("filter even")
    {
        BufferedDisk bd(&d, num_test_entries * 4);
        // filter every other entry (starting with 0)
        bitfield filter(num_test_entries);
        for (int i = 0; i < num_test_entries; ++i) {
            if ((i & 1) == 1) filter.set(i);
        }
        FilteredDisk fd(std::move(bd), std::move(filter), 4);

        for (uint32_t i = 0; i < num_test_entries / 2 - 1; ++i) {
            auto const val = *reinterpret_cast<std::uint32_t const*>(fd.Read(i * 4, 4));
            CHECK((i * 2) + 1 == val);
        }

        // don't go all the way down to 0, every backwards read cursor movement will
        // print a warning
        for (uint32_t i = num_test_entries / 2 - 1; i > num_test_entries / 2 + 200; --i) {
            auto const val = *reinterpret_cast<std::uint32_t const*>(fd.Read(i * 4, 4));
            CHECK((i * 2) + 1 == val);
        }
    }

    SECTION("filter odd")
    {
        BufferedDisk bd(&d, num_test_entries * 4);
        // filter every other entry (starting with 0)
        bitfield filter(num_test_entries);
        for (int i = 0; i < num_test_entries; ++i) {
            if ((i & 1) == 0) filter.set(i);
        }
        FilteredDisk fd(std::move(bd), std::move(filter), 4);

        for (uint32_t i = 0; i < num_test_entries / 2 - 1; ++i) {
            auto const val = *reinterpret_cast<std::uint32_t const*>(fd.Read(i * 4, 4));
            CHECK((i * 2) == val);
        }

        // don't go all the way down to 0, every backwards read cursor movement will
        // print a warning
        for (uint32_t i = num_test_entries / 2 - 1; i > num_test_entries / 2 + 200; --i) {
            auto const val = *reinterpret_cast<std::uint32_t const*>(fd.Read(i * 4, 4));
            CHECK((i * 2) == val);
        }
    }
/*
    SECTION("empty bitfield")
    {
        BufferedDisk bd(&d, num_test_entries * 4);
        bitfield filter(num_test_entries);
        FilteredDisk fd(std::move(bd), std::move(filter), 4);
    }
*/
    remove("test_file.bin");
}
