"""
License issuer CLI — VENDOR USE ONLY. Do not ship to customers.

Generates a signed license file for a specific VPS fingerprint.

Usage:
    # One-time: generate signing key pair (store private key offline, commit public key)
    python -m licensing.issue keygen --out licensing/

    # Issue a license for a customer
    python -m licensing.issue sign \\
        --signing-key ~/.graphenmail/signing_key.pem \\
        --customer "acme-corp" \\
        --host-fingerprint "<sha256 hex from customer>" \\
        --expires 2027-04-17 \\
        --features ai_urls,ip_rotation \\
        > license.key

The customer runs `python -m licensing.fingerprint` on their VPS to get
the host fingerprint and sends it to you.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import secrets
import sys
from datetime import date
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def cmd_keygen(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    sk = Ed25519PrivateKey.generate()
    pk = sk.public_key()

    priv_pem = sk.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_pem = pk.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    priv_path = out_dir / "signing_key.pem"
    pub_path = out_dir / "public_key.pem"

    priv_path.write_bytes(priv_pem)
    pub_path.write_bytes(pub_pem)
    try:
        os.chmod(priv_path, 0o600)
    except (OSError, NotImplementedError):
        pass

    print(f"Private key → {priv_path} (KEEP SECRET, do not commit)")
    print(f"Public key  → {pub_path} (commit to repo, bundled with app)")
    return 0


def cmd_sign(args: argparse.Namespace) -> int:
    key_path = Path(args.signing_key)
    if not key_path.exists():
        print(f"Signing key not found: {key_path}", file=sys.stderr)
        return 2

    sk_data = key_path.read_bytes()
    sk = serialization.load_pem_private_key(sk_data, password=None)
    if not isinstance(sk, Ed25519PrivateKey):
        print("Signing key is not ed25519.", file=sys.stderr)
        return 2

    features = [f.strip() for f in args.features.split(",") if f.strip()] if args.features else []
    payload = {
        "customer": args.customer,
        "host_fingerprint": args.host_fingerprint.strip().lower(),
        "issued_at": date.today().isoformat(),
        "expires_at": args.expires,  # None for perpetual
        "features": features,
        "nonce": secrets.token_hex(8),
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = sk.sign(payload_bytes)

    license_text = f"{_b64url(payload_bytes)}.{_b64url(sig)}"
    print(license_text)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="licensing.issue", description="GraphenMail license issuer (vendor use only)")
    sub = p.add_subparsers(dest="cmd", required=True)

    kg = sub.add_parser("keygen", help="Generate signing key pair")
    kg.add_argument("--out", default="licensing/", help="Output directory")
    kg.set_defaults(func=cmd_keygen)

    sg = sub.add_parser("sign", help="Sign a license for a customer")
    sg.add_argument("--signing-key", required=True, help="Path to ed25519 private key PEM")
    sg.add_argument("--customer", required=True, help="Customer name / company")
    sg.add_argument("--host-fingerprint", required=True, help="SHA256 hex from customer VPS")
    sg.add_argument("--expires", default=None, help="Expiry date ISO (YYYY-MM-DD) or omit for perpetual")
    sg.add_argument("--features", default="", help="Comma-separated feature flags")
    sg.set_defaults(func=cmd_sign)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
