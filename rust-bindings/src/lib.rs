#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(non_upper_case_globals)]

extern crate link_cplusplus;

mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub fn validate_proof(
    seed: &[u8; 32],
    k: u8,
    challenge: &[u8; 32],
    proof: &[u8],
    quality: &mut [u8; 32],
) -> bool {
    let Some(proof_len) = proof.len().try_into().ok() else {
        return false;
    };

    unsafe {
        bindings::validate_proof(
            seed.as_ptr(),
            k,
            challenge.as_ptr(),
            proof.as_ptr(),
            proof_len,
            quality.as_mut_ptr(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_proof() {
        let mut quality = [0; 32];
        assert!(!validate_proof(&[0; 32], 32, &[0; 32], &[], &mut quality));
        assert_eq!(quality, [0; 32]);
    }

    #[test]
    fn test_min_k_size() {
        let mut quality = [0; 32];
        assert!(!validate_proof(&[0; 32], 0, &[0; 32], &[0], &mut quality));
        assert_eq!(quality, [0; 32]);
    }

    #[test]
    fn test_max_k_size() {
        let mut quality = [0; 32];
        assert!(!validate_proof(&[0; 32], 100, &[0; 32], &[0], &mut quality));
        assert_eq!(quality, [0; 32]);
    }

    #[test]
    fn test_wrong_proof_length() {
        let mut quality = [0; 32];
        assert!(!validate_proof(
            &[0; 32],
            32,
            &[0; 32],
            &[0; 1000],
            &mut quality
        ));
        assert_eq!(quality, [0; 32]);
    }

    #[test]
    fn test_bad_proof() {
        let mut quality = [0; 32];
        assert!(!validate_proof(
            &[0; 32],
            32,
            &[0; 32],
            &[0; 32 * 8],
            &mut quality
        ));
        assert_eq!(quality, [0; 32]);
    }
}
