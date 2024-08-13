#include "picosha2.hpp"
#include "../src/verifier.hpp"

extern "C" {
    typedef struct {
        uint8_t* data;
        size_t length;
    } ByteArray;

    ByteArray validate_proof(const uint8_t* seed, uint8_t k, const uint8_t* challenge, const uint8_t* proof, uint16_t proof_len);

    void delete_byte_array(ByteArray array);
}
