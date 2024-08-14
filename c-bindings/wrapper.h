#include "picosha2.hpp"

extern "C" {
    bool validate_proof(uint8_t* seed, uint8_t k, uint8_t* challenge, uint8_t* proof, uint16_t proof_len, uint8_t* quality_buf);
}
