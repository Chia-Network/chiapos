#include "wrapper.h"
#include "verifier.hpp"

extern "C" {
    bool validate_proof(const uint8_t* plot_id, uint8_t k, const uint8_t* challenge, const uint8_t* proof, uint16_t proof_len, uint8_t* quality_buf) {
        Verifier v;
        auto quality = v.ValidateProof(plot_id, k, challenge, proof, proof_len);
        if (quality.GetSize() != 256) {
            return false;
        }
        quality.ToBytes(quality_buf);
        return true;
    }
}
