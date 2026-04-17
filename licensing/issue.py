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
import calendar
import json
import os
import secrets
import sys
from datetime import date, timedelta
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _add_months(start: date, months: int) -> date:
    if months < 0:
        raise ValueError("Months must be zero or greater.")
    month_index = start.month - 1 + months
    year = start.year + month_index // 12
    month = month_index % 12 + 1
    day = min(start.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def resolve_expiry(
    *,
    expires: str | None = None,
    days: int | None = None,
    months: int | None = None,
    perpetual: bool = False,
    today: date | None = None,
) -> str | None:
    supplied = sum(
        1 for value in (
            expires,
            days if days is not None else None,
            months if months is not None else None,
            True if perpetual else None,
        ) if value not in (None, "", False)
    )
    if supplied > 1:
        raise ValueError("Choose only one expiry option.")
    if perpetual:
        return None
    if expires:
        date.fromisoformat(expires)
        return expires

    base = today or date.today()
    if days is not None:
        if days < 1:
            raise ValueError("Days must be at least 1.")
        return (base + timedelta(days=days)).isoformat()
    if months is not None:
        if months < 1:
            raise ValueError("Months must be at least 1.")
        return _add_months(base, months).isoformat()
    return None


def load_signing_key(path: str | Path) -> Ed25519PrivateKey:
    key_path = Path(path)
    if not key_path.exists():
        raise FileNotFoundError(f"Signing key not found: {key_path}")

    sk_data = key_path.read_bytes()
    sk = serialization.load_pem_private_key(sk_data, password=None)
    if not isinstance(sk, Ed25519PrivateKey):
        raise ValueError("Signing key is not ed25519.")
    return sk


def generate_license_text(
    *,
    signing_key: Ed25519PrivateKey,
    customer: str,
    host_fingerprint: str,
    expires_at: str | None,
    features: list[str] | tuple[str, ...] | None = None,
    issued_at: date | None = None,
) -> str:
    payload = {
        "customer": customer,
        "host_fingerprint": host_fingerprint.strip().lower(),
        "issued_at": (issued_at or date.today()).isoformat(),
        "expires_at": expires_at,
        "features": list(features or ()),
        "nonce": secrets.token_hex(8),
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = signing_key.sign(payload_bytes)
    return f"{_b64url(payload_bytes)}.{_b64url(sig)}"


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
    try:
        sk = load_signing_key(args.signing_key)
        expires_at = resolve_expiry(
            expires=args.expires,
            days=args.days,
            months=args.months,
            perpetual=args.perpetual,
        )
        features = [f.strip() for f in args.features.split(",") if f.strip()] if args.features else []
        license_text = generate_license_text(
            signing_key=sk,
            customer=args.customer,
            host_fingerprint=args.host_fingerprint,
            expires_at=expires_at,
            features=features,
        )
    except (FileNotFoundError, ValueError) as e:
        print(str(e), file=sys.stderr)
        return 2

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
    expiry_group = sg.add_mutually_exclusive_group()
    expiry_group.add_argument("--expires", default=None, help="Expiry date ISO (YYYY-MM-DD)")
    expiry_group.add_argument("--days", type=int, default=None, help="Expire N days from today")
    expiry_group.add_argument("--months", type=int, default=None, help="Expire N calendar months from today")
    expiry_group.add_argument("--perpetual", action="store_true", help="Never expires")
    sg.add_argument("--features", default="", help="Comma-separated feature flags")
    sg.set_defaults(func=cmd_sign)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
