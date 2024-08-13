#include "wrapper.h"

extern "C" {
    ByteArray validate_proof(const uint8_t* seed, uint8_t k, const uint8_t* challenge, const uint8_t* proof, uint16_t proof_len) {
        Verifier v;
        auto quality = v.ValidateProof(seed, k, challenge, proof, proof_len);
        if (quality.GetSize() == 0) {
            return ByteArray { nullptr, 0 };
        }
        uint8_t *quality_buf = new uint8_t[32];
        quality.ToBytes(quality_buf);
        delete[] quality_buf;
        return ByteArray { quality_buf, 32 };
    }

    void delete_byte_array(ByteArray array) {
        delete[] array.data;
    }
}
