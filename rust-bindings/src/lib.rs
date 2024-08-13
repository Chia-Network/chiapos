#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(non_upper_case_globals)]

extern crate link_cplusplus;

mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub fn validate_proof(
    seed: [u8; 32],
    k: u8,
    challenge: [u8; 32],
    proof: [u8; 32],
) -> Option<Vec<u8>> {
    unsafe {
        let array = bindings::validate_proof(
            seed.as_ptr(),
            k,
            challenge.as_ptr(),
            proof.as_ptr(),
            proof
                .len()
                .try_into()
                .expect("proof must be less than 2^16 bytes long"),
        );

        if array.data.is_null() {
            None
        } else {
            let data = std::slice::from_raw_parts(array.data, array.length);
            let result = data.to_vec();
            bindings::delete_byte_array(array);
            Some(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_proof() {
        assert_eq!(validate_proof([0; 32], 0, [0; 32], [0; 32]), Some(vec![]));
    }
}
