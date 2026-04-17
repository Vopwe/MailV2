"""
Customer-facing helper — print the host fingerprint for this machine.

Run on the target VPS after install, send output to vendor to receive a license:

    python -m licensing.fingerprint
"""
from licensing.validator import compute_host_fingerprint

if __name__ == "__main__":
    fp = compute_host_fingerprint()
    print(fp)
