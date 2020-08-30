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

#include <set>
#include <stdio.h>
#include "../lib/include/catch.hpp"
#include "../lib/include/picosha2.hpp"

#include "calculate_bucket.hpp"
#include "plotter_disk.hpp"
#include "sort_on_disk.hpp"
#include "prover_disk.hpp"
#include "verifier.hpp"
#include "encoding.hpp"

using namespace std;

uint8_t plot_id_1[] = {35, 2, 52, 4, 51, 55, 23, 84, 91, 10, 111, 12, 13,
                       222, 151, 16, 228, 211, 254, 45, 92, 198, 204, 10, 9, 10,
                       11, 129, 139, 171, 15, 23};

uint8_t plot_id_3[] = {5, 104, 52, 4, 51, 55, 23, 84, 91, 10, 111, 12, 13,
                       222, 151, 16, 228, 211, 254, 45, 92, 198, 204, 10, 9, 10,
                       11, 129, 139, 171, 15, 23};

vector<unsigned char> intToBytes(uint32_t paramInt, uint32_t numBytes) {
    vector<unsigned char> arrayOfByte(numBytes, 0);
    for (uint32_t i = 0; paramInt > 0; i++) {
        arrayOfByte[numBytes - i - 1] = paramInt & 0xff;
        paramInt >>= 8;
    }
    return arrayOfByte;
}

static uint128_t to_uint128(uint64_t hi, uint64_t lo)
{
    return (uint128_t)hi << 64 | lo;
}

TEST_CASE("Util") {
    SECTION("Increment and decrement") {
        uint8_t bytes[3] = {45, 172, 225};
        REQUIRE(Util::SliceInt64FromBytes(bytes, 2, 19) == 374172);
        uint8_t bytes2[1] = {213};
        REQUIRE(Util::SliceInt64FromBytes(bytes2, 1, 5) == 21);
        uint8_t bytes3[17] = {1, 2, 3, 4, 5, 6, 7, 255, 255, 10, 11, 12, 13, 14, 15, 16, 255};
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

TEST_CASE("Bits") {
    SECTION("Increment and decrement") {
        Bits a = Bits(5, 3);
        Bits b = Bits(2, 10);
        cout << "A is: " << a << endl;
        cout << "B is: " << b << endl;

        ++b;
        ++b;
        ++b;
        ++b;

        cout << "B is: " << b << endl;

        ++a;
        cout << "A is: " << a << endl;
        ++a;
        cout << "A is: " << a << endl;

        cout << a + b + a << endl;
        --a;
        cout << "A is: " << a << endl;
        Bits c = a++;
        cout << "C is: " << c << endl;
        cout << "A is: " << a << endl;
        Bits d = a--;
        cout << "D is: " << d << endl;
        cout << "A is: " << a << endl;

        Bits e;
        Bits f = Bits(3, 5);
        cout << e + f + e + d << endl;
    }

    SECTION("Slicing and manipulating") {
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
        cout << "bytes: " << static_cast<int>(bytes[0]) << " " << static_cast<int>(bytes[1]) << endl;
        cout << "Back to Bits: " << Bits(bytes, 2, 16) << endl;

        Bits(256, 9).ToBytes(bytes);
        cout << "bytes: " << static_cast<int>(bytes[0]) << " " << static_cast<int>(bytes[1]) << endl;
        cout << "Back to Bits: " << Bits(bytes, 2, 16) << endl;

        cout << Bits(640, 11) << endl;
        Bits(640, 11).ToBytes(bytes);
        cout << "bytes: " << static_cast<int>(bytes[0]) << " " << static_cast<int>(bytes[1]) << endl;

        Bits h = Bits(bytes, 2, 16);
        Bits i = Bits(bytes, 2, 17);
        cout << "H: " << h << endl;
        cout << "I: " << i << endl;

        Bits j = Bits(11, 5);
        Bits k1 = Bits(7, 5);

        cout << "j" << j << endl;
        cout << "k" << k1 << endl;
        cout << "Xor:" << (j ^ k1) << endl;

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

        uint64_t a_hi = 0x97ef8e98bce1bb4ULL, a_lo = 0x6924069578d89abeULL;
        uint64_t b_hi = 0xe1dcbd9c33572c8ULL, b_lo = 0x8a2b75bbdbb73f73ULL;
        uint8_t ab_len = 124;
        Bits a(to_uint128(a_hi, a_lo), ab_len);
        Bits b(to_uint128(b_hi, b_lo), ab_len);

        uint128_t sum = to_uint128(0x79cc4c34f038e7cULL, 0xf34f7c51548fda31ULL);
        uint128_t sum_res = a.Add(b).GetValue128();
        cout << "sum_res: " << sum_res << endl;
        REQUIRE(sum_res == sum);

        uint128_t xor_res = to_uint128(0x763333048fb697cULL, 0xe30f732ea36fa5cdULL);
        REQUIRE((a ^ b).GetValue128() == xor_res);

        uint128_t r1 = to_uint128(0x2fdf1d3179c3768ULL, 0xd2480d2af1b1357dULL);
        uint128_t r15 = to_uint128(0x5ece19ab9644515ULL, 0xbaddeddb9fb9f0eeULL);
        uint128_t r60 = to_uint128(0x8a2b75bbdbb73f7ULL, 0x3e1dcbd9c33572c8ULL);
        uint128_t r63 = to_uint128(0x515baddeddb9fb9ULL, 0xf0ee5ece19ab9644ULL);
        uint128_t r1_res = a.Rotl(1).GetValue128();
        uint128_t r15_res = b.Rotl(15).GetValue128();
        uint128_t r60_res = b.Rotl(60).GetValue128();
        uint128_t r63_res = b.Rotl(63).GetValue128();
        cout << "r1_res: " << r1_res << endl;
        REQUIRE(r1_res == r1);
        cout << "r15_res: " << r15_res << endl;
        REQUIRE(r15_res == r15);
        cout << "r60_res: " << r60_res << endl;
        REQUIRE(r60_res == r60);
        cout << "r63_res: " << r63_res << endl;
        REQUIRE(r63_res == r63);

        REQUIRE(a.Trunc(60) == Bits(0x924069578d89abeULL, 60));
        REQUIRE(b.Trunc(99) == Bits(to_uint128(0x1c33572c8ULL, 0x8a2b75bbdbb73f73ULL), 99));
    }
    SECTION("Park Bits") {
        uint32_t num_bytes = 16000;
        uint8_t* buf = new uint8_t[num_bytes];
        uint8_t* buf_2 = new uint8_t[num_bytes];
        Util::GetRandomBytes(buf, num_bytes);
        ParkBits my_bits = ParkBits(buf, num_bytes, num_bytes*8);
        my_bits.ToBytes(buf_2);
        for (uint32_t i = 0; i < num_bytes; i++) {
            REQUIRE(buf[i] == buf_2[i]);
        }
        delete[] buf;
        delete[] buf_2;
    }

    SECTION("Large Bits") {
        uint32_t num_bytes = 200000;
        uint8_t* buf = new uint8_t[num_bytes];
        uint8_t* buf_2 = new uint8_t[num_bytes];
        Util::GetRandomBytes(buf, num_bytes);
        LargeBits my_bits = LargeBits(buf, num_bytes, num_bytes*8);
        my_bits.ToBytes(buf_2);
        for (uint32_t i = 0; i < num_bytes; i++) {
            REQUIRE(buf[i] == buf_2[i]);
        }
        delete[] buf;
        delete[] buf_2;
    }
}

class FakeDisk : public Disk {
 public:
    explicit FakeDisk(uint32_t size) : s(size, 'a') {
        f_ = std::stringstream(s, std::ios_base::in | std::ios_base::out);
    }

    ~FakeDisk() {

    }

    void Read(uint64_t begin, uint8_t* memcache, uint64_t length) override {
        f_.seekg(begin);
        f_.read(reinterpret_cast<char*>(memcache), length);
    }

    void Write(uint64_t begin, const uint8_t* memcache, uint64_t length) override {
        f_.seekp(begin);
        f_.write(reinterpret_cast<const char*>(memcache), length);
    }
    void Truncate(uint64_t new_size) override {
        if (new_size <= s.size()) {
            s = s.substr(0, new_size);
        } else {
            s = s + std::string(new_size - s.size(), 0);
        }
    }

 private:
    std::string s;
    std::stringstream f_;
};

bool CheckMatch(int64_t yl, int64_t yr) {
    int64_t bl = yl / kBC;
    int64_t br = yr / kBC;
    if (bl + 1 != br) return false;  // Buckets don't match
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
static int CartProdNext(uint8_t *items, uint8_t n, uint8_t k, bool init) {
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

static int sq(int n) {
    return n * n;
}

static bool Have4Cycles(uint32_t extraBits, int B, int C) {
    uint8_t m[4];
    bool init = true;

    while (!CartProdNext(m, 1 << extraBits, 4, init)) {
        uint8_t r1 = m[0], r2 = m[1], s1 = m[2], s2 = m[3];

        init = false;
        if (r1 != s1 && (r1 << extraBits) + r2 != (s2 << extraBits) + s1 &&
            (r1 - s1 + r2 - s2) % B == 0) {
            uint8_t p[2];
            bool initp = true;

            while(!CartProdNext(p, 2, 2, initp)) {
                uint8_t p1 = p[0], p2 = p[1];
                int lhs = sq(2*r1 + p1) - sq(2*s1 + p1) + sq(2*r2 + p2) - sq(2*s2 + p2);

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

TEST_CASE("Matching function") {
    SECTION("Cycles") {
        REQUIRE(!Have4Cycles(kExtraBits, kB, kC));
    }
}

void VerifyFC(uint8_t t, uint8_t k, uint64_t L, uint64_t R, uint64_t y1, uint64_t y, uint64_t c)
{
    uint8_t sizes[] = { 1, 2, 4, 4, 3, 2 };
    uint8_t size = sizes[t - 2];
    FxCalculator fcalc(k, t);

    std::pair<Bits, Bits> res = fcalc.CalculateBucket(Bits(y1, k + kExtraBits), Bits(), Bits(L, k * size), Bits(R, k * size));
    REQUIRE(res.first.GetValue() == y);
    if (c) {
        REQUIRE(res.second.GetValue() == c);
    }
}

TEST_CASE("F functions") {
    SECTION("F1") {
        uint8_t test_k = 35;
        uint8_t test_key[] = {0, 2, 3, 4, 5, 5, 7, 8, 9, 10, 11, 12, 13,
                        14, 15, 16, 1, 2, 3, 41, 5, 6, 7, 8, 9, 10,
                        11, 12, 13, 11, 15, 16};
        F1Calculator f1(test_k, test_key);

        Bits L = Bits(525, test_k);
        pair<Bits, Bits> result1 = f1.CalculateBucket(L);
        Bits L2 = Bits(526, test_k);
        pair<Bits, Bits> result2 = f1.CalculateBucket(L2);
        Bits L3 = Bits(625, test_k);
        pair<Bits, Bits> result3 = f1.CalculateBucket(L3);

        vector<pair<Bits, Bits>> results = f1.CalculateBuckets(L, 101);
        REQUIRE(result1 == results[0]);
        REQUIRE(result2 == results[1]);
        REQUIRE(result3 == results[100]);

        test_k = 32;
        F1Calculator f1_2(test_k, test_key);
        L = Bits(192837491, test_k);
        result1 = f1_2.CalculateBucket(L);
        L2 = Bits(192837491 + 1, test_k);
        result2 = f1_2.CalculateBucket(L2);
        L3 = Bits(192837491 + 2, test_k);
        result3 = f1_2.CalculateBucket(L3);
        Bits L4 = Bits(192837491 + 490, test_k);
        pair<Bits, Bits> result4 = f1_2.CalculateBucket(L4);

        results = f1_2.CalculateBuckets(L, 491);
        REQUIRE(result1 == results[0]);
        REQUIRE(result2 == results[1]);
        REQUIRE(result3 == results[2]);
        REQUIRE(result4 == results[490]);
    }

    SECTION("F2") {
        uint8_t test_key_2[] = {20, 2, 5, 4, 51, 52, 23, 84, 91, 10, 111, 12, 13,
                            24, 151, 16, 228, 211, 254, 45, 92, 198, 204, 10, 9, 10,
                            11, 129, 139, 171, 15, 18};
        map<uint64_t, vector<pair<Bits, Bits>>> buckets;

        uint8_t k = 12;
        uint64_t num_buckets = (1ULL << (k + kExtraBits)) / kBC + 1;
        Bits x = Bits(0, k);

        F1Calculator f1(k, test_key_2);
        for (uint32_t j=0; j < (1ULL << (k-4)) + 1; j++) {
            for (auto pair : f1.CalculateBuckets(x, 1U << 4)) {
                uint64_t bucket = std::get<0>(pair).GetValue() / kBC;
                if (buckets.find(bucket) == buckets.end()) {
                    buckets[bucket] = vector<std::pair<Bits, Bits>>();
                }
                buckets[bucket].push_back(pair);
                if (x.GetValue() + 1 > (1ULL << k) - 1) {
                    break;
                }
                ++x;
            }
            if (x.GetValue() + 1 > (1ULL << k) - 1) {
                break;
            }
        }

        FxCalculator f2(k, 2);
        int total_matches = 0;

        for (auto kv : buckets) {
            if (kv.first == num_buckets- 1) {
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
            sort(left_bucket.begin(), left_bucket.end(), [](const PlotEntry & a, const PlotEntry & b) -> bool {
                return a.y > b.y;
            });
            sort(right_bucket.begin(), right_bucket.end(), [](const PlotEntry & a, const PlotEntry & b) -> bool {
                return a.y > b.y;
            });

            vector<pair<uint16_t, uint16_t> > matches = f2.FindMatches(left_bucket, right_bucket);
            for (auto match : matches) {
                REQUIRE(CheckMatch(left_bucket[match.first].y, right_bucket[match.second].y));
            }
            total_matches += matches.size();
        }
        REQUIRE(total_matches > (1 << k) / 2);
        REQUIRE(total_matches < (1 << k) * 2);
    }

    SECTION("Fx") {
        VerifyFC(2, 16, 0x44cb, 0x204f, 0x20a61a, 0x2af546, 0x44cb204f);
        VerifyFC(2, 16, 0x3c5f, 0xfda9, 0x3988ec, 0x15293b, 0x3c5ffda9);
        VerifyFC(3, 16, 0x35bf992d, 0x7ce42c82, 0x31e541, 0xf73b3, 0x35bf992d7ce42c82);
        VerifyFC(3, 16, 0x7204e52d, 0xf1fd42a2, 0x28a188, 0x3fb0b5, 0x7204e52df1fd42a2);
        VerifyFC(4, 16, 0x5b6e6e307d4bedc, 0x8a9a021ea648a7dd, 0x30cb4c, 0x11ad5, 0xd4bd0b144fc26138);
        VerifyFC(4, 16, 0xb9d179e06c0fd4f5, 0xf06d3fef701966a0, 0x1dd5b6, 0xe69a2, 0xd02115f512009d4d);
        VerifyFC(5, 16, 0xc2cd789a380208a9, 0x19999e3fa46d6753, 0x25f01e, 0x1f22bd, 0xabe423040a33);
        VerifyFC(5, 16, 0xbe3edc0a1ef2a4f0, 0x4da98f1d3099fdf5, 0x3feb18, 0x31501e, 0x7300a3a03ac5);
        VerifyFC(6, 16, 0xc965815a47c5, 0xf5e008d6af57, 0x1f121a, 0x1cabbe, 0xc8cc6947);
        VerifyFC(6, 16, 0xd420677f6cbd, 0x5894aa2ca1af, 0x2efde9, 0xc2121, 0x421bb8ec);
        VerifyFC(7, 16, 0x5fec898f, 0x82283d15, 0x14f410, 0x24c3c2, 0x0);
        VerifyFC(7, 16, 0x64ac5db9, 0x7923986, 0x590fd, 0x1c74a2, 0x0);
    }
}

void HexToBytes(const string& hex, uint8_t* result) {
    for (unsigned int i = 0; i < hex.length(); i += 2) {
        string byteString = hex.substr(i, 2);
        uint8_t byte = (uint8_t) strtol(byteString.c_str(), NULL, 16);
        result[i/2] = byte;
  }
}


void TestProofOfSpace(std::string filename, uint32_t iterations, uint8_t k, uint8_t* plot_id) {
        DiskProver prover(filename);
        uint8_t* proof_data = new uint8_t[8 * k];
        uint32_t success = 0;
        for (uint32_t i = 0; i < iterations; i++) {
            vector<unsigned char> hash_input = intToBytes(i, 4);
            vector<unsigned char> hash(picosha2::k_digest_size);
            picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());
            vector<LargeBits> qualities = prover.GetQualitiesForChallenge(hash.data());
            Verifier verifier = Verifier();
            for (uint32_t index = 0; index < qualities.size(); index++) {
                LargeBits proof = prover.GetFullProof(hash.data(), index);
                proof.ToBytes(proof_data);

                LargeBits quality = verifier.ValidateProof(plot_id, k, hash.data(), proof_data, k*8);
                REQUIRE(quality.GetSize() == 256);
                REQUIRE(quality == qualities[index]);
                success += 1;

                // Tests invalid proof
                proof_data[0] = (proof_data[0] + 1) % 256;
                LargeBits quality_2 = verifier.ValidateProof(plot_id, k, hash.data(), proof_data, k*8);
                REQUIRE(quality_2.GetSize() == 0);
            }
        }
        std::cout << "Success: " << success << "/" << iterations << " " << (100* ((double)success/(double)iterations))
                                 << "%" << std::endl;
        REQUIRE(success > 0);
        REQUIRE(success < iterations);
        delete[] proof_data;
}


void PlotAndTestProofOfSpace(std::string filename, uint32_t iterations, uint8_t k, uint8_t* plot_id) {
        DiskPlotter plotter = DiskPlotter();
        uint8_t memo[5] = {1, 2, 3, 4, 5};
        plotter.CreatePlotDisk(".", ".", ".", filename, k, memo, 5, plot_id, 32);
        TestProofOfSpace(filename, iterations, k, plot_id);
        REQUIRE(remove(filename.c_str()) == 0);
}


TEST_CASE("Plotting") {
    SECTION("Disk plot 1") {
        PlotAndTestProofOfSpace("cpp-test-plot.dat", 100, 16, plot_id_1);
    }
    SECTION("Disk plot 2") {
        PlotAndTestProofOfSpace("cpp-test-plot.dat", 500, 17, plot_id_3);
    }
    SECTION("Disk plot 3") {
        PlotAndTestProofOfSpace("cpp-test-plot.dat", 5000, 21, plot_id_3);
    }
}

TEST_CASE("Invalid plot") {
    SECTION("File gets deleted") {
        string filename = "invalid-plot.dat";
        {
        DiskPlotter plotter = DiskPlotter();
        uint8_t memo[5] = {1, 2, 3, 4, 5};
        uint8_t k = 22;
        plotter.CreatePlotDisk(".", ".", ".", filename, k, memo, 5, plot_id_1, 32);
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
            LargeBits quality = verifier.ValidateProof(plot_id_1, k, challenge, proof_data, k*8);
            REQUIRE(quality == qualities[index]);
        }
        delete[] proof_data;
        }
        REQUIRE(remove(filename.c_str()) == 0);
        REQUIRE_THROWS_WITH([&](){
            DiskProver p(filename);
        }(), "Invalid file " + filename);
    }
}

TEST_CASE("Sort on disk") {
    SECTION("ExtractNum") {
        for (int i=0; i < 15*8 - 5; i++) {
            uint8_t buf[15];
            Bits((uint128_t)27 << i, 15*8).ToBytes(buf);

            REQUIRE(SortOnDiskUtils::ExtractNum(buf, 15, 15*8 - 4 - i, 3) == 5);
        }
        uint8_t buf[16];
        Bits((uint128_t)27 << 5, 128).ToBytes(buf);
        REQUIRE(SortOnDiskUtils::ExtractNum(buf, 16, 100, 200) == 864);
    }

    SECTION("MemCmpBits") {
        uint8_t left[3];
        left[0] = 12;
        left[1] = 10;
        left[2] = 100;

        uint8_t right[3];
        right[0] = 12;
        right[1] = 10;
        right[2] = 100;

        REQUIRE(SortOnDiskUtils::MemCmpBits(left, right, 3, 0) == 0);
        REQUIRE(SortOnDiskUtils::MemCmpBits(left, right, 3, 10) == 0);

        right[1] = 11;
        REQUIRE(SortOnDiskUtils::MemCmpBits(left, right, 3, 0) < 0);
        REQUIRE(SortOnDiskUtils::MemCmpBits(left, right, 3, 16) == 0);

        right[1] = 9;
        REQUIRE(SortOnDiskUtils::MemCmpBits(left, right, 3, 0) > 0);

        right[1] = 10;

        // Last bit differs
        right[2] = 101;
        REQUIRE(SortOnDiskUtils::MemCmpBits(left, right, 3, 0) < 0);
    }

    SECTION("Quicksort") {
        uint32_t iters = 100;
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
        Sorting::QuickSort(hashes_bytes, 16, iters, 0);

        for (uint32_t i = 0; i < iters; i++) {
            std::string str(reinterpret_cast<char*>(hashes_bytes) + i * 16, 16);
            REQUIRE(str.compare(hashes[i]) == 0);
        }
        delete[] hashes_bytes;
    }

    SECTION("Fake disk") {
        FakeDisk d = FakeDisk(10000);
        uint8_t buf[5] = {1, 2, 3, 5, 7};
        d.Write(250, buf, 5);

        uint8_t read_buf[5];
        d.Read(250, read_buf, 5);

        REQUIRE(memcmp(buf, read_buf, 5) == 0);
    }

    SECTION("File disk") {
        FileDisk d = FileDisk("test_file.bin");
        uint8_t buf[5] = {1, 2, 3, 5, 7};
        d.Write(250, buf, 5);

        uint8_t read_buf[5];
        d.Read(250, read_buf, 5);

        REQUIRE(memcmp(buf, read_buf, 5) == 0);
        remove("test_file.bin");
    }

    SECTION("Bucket store") {
        uint32_t iters = 10000;
        uint32_t size = 16;
        vector<Bits> input;

        for (uint32_t i = 0; i < iters; i++) {
            uint8_t rand_arr[size];
            for (uint32_t i = 0; i < size; i++) {
                rand_arr[i] = rand() % 256;
            }
            input.push_back(Bits(rand_arr, size, size*8));
        }

        set<Bits> iset(input.begin(), input.end());
        uint32_t input_index = 0;
        vector<Bits> output;

        uint8_t* mem = new uint8_t[iters];
        BucketStore bs = BucketStore(mem, iters, 16, 0, 4, 5);
        bs.Audit();

        uint8_t buf[size];
        while (true) {
            while (!bs.IsFull() && input_index != input.size()) {
                input[input_index].ToBytes(buf);
                bs.Store(buf, size);
                bs.Audit();
                input_index += 1;
            }
            uint32_t m = bs.MaxBucket();
            uint64_t final_size;
            uint64_t* bucket_handle = bs.BucketHandle(m, 1000000, final_size);
            uint32_t entry_size = size / 8;
            uint8_t last_size = (size * 8) % 64;
            if (last_size == 0)
                last_size = 64;
            for (uint32_t i = 0; i < final_size; i += entry_size) {
                Sorting::EntryToBytes(bucket_handle, i, i + entry_size, last_size, buf);
                Bits x(buf, size, size*8);
                REQUIRE(iset.find(x) != iset.end());
                REQUIRE(SortOnDiskUtils::ExtractNum((uint8_t*)buf, size, 0, 4) == m);
                output.push_back(x);
            }
            if (bs.IsEmpty()) {
                delete[] bucket_handle;
                break;
            }
            delete[] bucket_handle;
        }
        sort(input.begin(), input.end());
        sort(output.begin(), output.end());

        set<Bits> output_set(output.begin(), output.end());
        REQUIRE(output_set.size() == output.size());
        for (uint32_t i = 0; i < output.size(); i++) {
            REQUIRE(input[i] == output[i]);
        }

        delete[] mem;
    }

    SECTION("Sort on disk") {
        uint32_t iters = 100000;
        uint32_t size = 32;
        vector<Bits> input;
        uint32_t begin = 1000;
        FakeDisk disk = FakeDisk(5000000);
        FakeDisk spare = FakeDisk(5000000);

        for (uint32_t i = 0; i < iters; i ++) {
            vector<unsigned char> hash_input = intToBytes(i, 4);
            vector<unsigned char> hash(picosha2::k_digest_size);
            picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());

            disk.Write(begin + i * size, hash.data(), size);
            input.emplace_back(Bits(hash.data(), size, size*8));
        }

        vector<uint64_t> bucket_sizes(16, 0);
        uint8_t buf[size];
        for (Bits& x : input) {
            x.ToBytes(buf);
            bucket_sizes[SortOnDiskUtils::ExtractNum(buf, size, 0, 4)] += 1;
        }

        const uint32_t memory_len = 100000;
        uint8_t* memory = new uint8_t[memory_len];
        Sorting::SortOnDisk(disk, begin, spare, size, 0, bucket_sizes, memory, memory_len);


        sort(input.begin(), input.end());

        uint8_t buf2[size];
        for (uint32_t i = 0; i < iters; i++) {
            disk.Read(begin + i * size, buf2, size);
            input[i].ToBytes(buf);
            REQUIRE(memcmp(buf, buf2, size) == 0);
        }

        delete[] memory;
    }

    SECTION("Sort in Memory") {
        uint32_t iters = 100000;
        uint32_t size = 32;
        vector<Bits> input;
        uint32_t begin = 1000;
        FakeDisk disk = FakeDisk(5000000);

        for (uint32_t i = 0; i < iters; i ++) {
            vector<unsigned char> hash_input = intToBytes(i, 4);
            vector<unsigned char> hash(picosha2::k_digest_size);
            picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());
            hash[0] = hash[1] = 0;
            disk.Write(begin + i * size, hash.data(), size);
            input.emplace_back(Bits(hash.data(), size, size*8));
        }

        const uint32_t memory_len = SortOnDiskUtils::RoundSize(iters) * 30;
        uint8_t* memory = new uint8_t[memory_len];
        Sorting::SortInMemory(disk, begin, memory, size, iters, 16);

        sort(input.begin(), input.end());
        uint8_t buf[size];
        uint8_t buf2[size];
        for (uint32_t i = 0; i < iters; i++) {
            disk.Read(begin + i * size, buf2, size);
            input[i].ToBytes(buf);
            REQUIRE(memcmp(buf, buf2, size) == 0);
        }

        delete[] memory;
    }

}
