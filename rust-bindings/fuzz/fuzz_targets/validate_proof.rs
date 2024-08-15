#![no_main]

use chiapos::validate_proof;
use libfuzzer_sys::{
    arbitrary::{Arbitrary, Unstructured},
    fuzz_target,
};

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let seed = u.arbitrary().unwrap();
    let k = u.arbitrary().unwrap();
    let challenge = u.arbitrary().unwrap();
    let proof = Vec::arbitrary(&mut u).unwrap();
    let mut quality = [0; 32];
    if !validate_proof(&seed, k, &challenge, &proof, &mut quality) {
        assert_eq!(quality, [0; 32]);
    }
});
