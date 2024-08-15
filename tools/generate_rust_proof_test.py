from chiapos import DiskPlotter, DiskProver, Verifier
from hashlib import sha256
from pathlib import Path
import os

plot_seed = bytes.fromhex(
    "05683404333717545b0a6f0c0dde9710e4d3fe2d5cc6cc0a090a0b818bab0f17"
)

pl = DiskPlotter()
pl.create_plot_disk(
    ".",
    ".",
    ".",
    "myplot.dat",
    21,
    bytes([1, 2, 3, 4, 5]),
    plot_seed,
    300,
    32,
    8192,
    8,
    False,
)
pl = None
pr = DiskProver(str(Path("myplot.dat")))

path = "rust-bindings/test_proofs.txt"
os.remove(path)

v = Verifier()
for i in range(100):
    challenge = sha256(i.to_bytes(4, "big")).digest()
    for index, quality in enumerate(pr.get_qualities_for_challenge(challenge)):
        proof = pr.get_full_proof(challenge, index)
        assert len(proof) == 8 * pr.get_size()
        with open(path, "a+") as f:
            f.write(
                f"{plot_seed.hex()}, {pr.get_size()}, {challenge.hex()}, {proof.hex()}, {quality.hex()}\n"
            )
        computed_quality = v.validate_proof(plot_seed, pr.get_size(), challenge, proof)
        assert computed_quality == quality
