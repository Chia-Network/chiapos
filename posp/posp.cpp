#include "posp.h"

#include <iostream>

#include "../src/plotter_disk.hpp"
#include "../src/prover_disk.hpp"
#include "../src/verifier.hpp"

using std::cout;
using std::endl;
using std::string;

string Strip0x(const string &hex)
{
    if (hex.size() > 1 && (hex.substr(0, 2) == "0x" || hex.substr(0, 2) == "0X")) {
        return hex.substr(2);
    }
    return hex;
}

void HexToBytes(const string &hex, uint8_t *result)
{
    for (uint32_t i = 0; i < hex.length(); i += 2) {
        string byteString = hex.substr(i, 2);
        uint8_t byte = (uint8_t)strtol(byteString.c_str(), NULL, 16);
        result[i / 2] = byte;
    }
}

void ValidateProof()
{
    string id = Strip0x("022fb42c08c12de3a6af053880199806532e79515f94e83461612101f9412f9e");
    string proof = Strip0x(
        "0x99550b233d022598b09d4c8a7b057986f6775d80973a905f5a6251d628d186430cb4464b8c70ecc77101bd4d"
        "50ef2c016cc78682a13c4b796835431edeb2231a282229c9e7322614d10193b1b87daaac0e21af5b5acc9f73b7"
        "ddd1da2a46294a2073f2e2fc99d57f3278ea1fc0f527499267aaa3980f730cb2ea7aacc1fa3f460acca1254f92"
        "791612e6e9ab9c3aed5aea172d7056b03bbfdf5861372d5c0ceb09e109485412376e");
    string challenge =
        Strip0x("0x4000000000000000000000000000000000000000000000000000000000000000");
    if (id.size() != 64) {
        cout << "Invalid ID, should be 32 bytes" << endl;
        exit(1);
    }
    if (challenge.size() != 64) {
        cout << "Invalid challenge, should be 32 bytes" << endl;
        exit(1);
    }
    if (proof.size() % 16) {
        cout << "Invalid proof, should be a multiple of 8 bytes" << endl;
        exit(1);
    }
    uint8_t k = proof.size() / 16;
    cout << "Verifying proof=" << proof << " for challenge=" << challenge
         << " and k=" << static_cast<int>(k) << endl
         << endl;
    uint8_t id_bytes[32];
    uint8_t challenge_bytes[32];
    uint8_t *proof_bytes = new uint8_t[proof.size() / 2];
    HexToBytes(id, id_bytes);
    HexToBytes(challenge, challenge_bytes);
    HexToBytes(proof, proof_bytes);

    Verifier verifier = Verifier();
    LargeBits quality = verifier.ValidateProof(id_bytes, k, challenge_bytes, proof_bytes, k * 8);

    std::cout << "called!" << quality << std::endl;
}