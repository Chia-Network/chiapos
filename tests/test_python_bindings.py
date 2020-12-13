import unittest
from chiapos import DiskProver, DiskPlotter, Verifier
from hashlib import sha256
from pathlib import Path


class TestPythonBindings(unittest.TestCase):
    def test_k_21(self):
        challenge: bytes = bytes([i for i in range(0, 32)])

        plot_seed: bytes = bytes(
            [
                5,
                104,
                52,
                4,
                51,
                55,
                23,
                84,
                91,
                10,
                111,
                12,
                13,
                222,
                151,
                16,
                228,
                211,
                254,
                45,
                92,
                198,
                204,
                10,
                9,
                10,
                11,
                129,
                139,
                171,
                15,
                23,
            ]
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

        total_proofs: int = 0
        iterations: int = 5000

        v = Verifier()
        for i in range(iterations):
            if i % 100 == 0:
                print(i)
            challenge = sha256(i.to_bytes(4, "big")).digest()
            for index, quality in enumerate(pr.get_qualities_for_challenge(challenge)):
                proof = pr.get_full_proof(challenge, index)
                assert len(proof) == 8 * pr.get_size()
                computed_quality = v.validate_proof(
                    plot_seed, pr.get_size(), challenge, proof
                )
                assert computed_quality == quality
                total_proofs += 1

        print(
            f"total proofs {total_proofs} out of {iterations}\
            {total_proofs / iterations}"
        )
        assert total_proofs > 4000
        assert total_proofs < 6000
        pr = None
        sha256_plot_hash = sha256()
        with open("myplot.dat", "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_plot_hash.update(byte_block)
            plot_hash = str(sha256_plot_hash.hexdigest())
        assert (
            plot_hash
            == "80e32f560f3a4347760d6baae8d16fbaf484948088bff05c51bdcc24b7bc40d9"
        )
        print(f"\nPlotfile asserted sha256: {plot_hash}\n")
        Path("myplot.dat").unlink()


if __name__ == "__main__":
    unittest.main()
