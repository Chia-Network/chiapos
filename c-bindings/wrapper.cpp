#include "wrapper.h"
#include "verifier.hpp"

extern "C" {
    bool validate_proof(uint8_t* seed, uint8_t k, uint8_t* challenge, uint8_t* proof, uint16_t proof_len, uint8_t* quality_buf) {
        Verifier v;
        auto quality = v.ValidateProof(seed, k, challenge, proof, proof_len);
        if (quality.GetSize() == 0) {
            return false;
        }
        quality.ToBytes(quality_buf);
        return true;
    }
}
