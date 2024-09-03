#include <stdint.h>
#include "picosha2.hpp"

extern "C" {
    bool validate_proof(const uint8_t* plot_id, uint8_t k, const uint8_t* challenge, const uint8_t* proof, uint16_t proof_len, uint8_t* quality_buf);
}
