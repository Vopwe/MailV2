"""
License validator — offline ed25519 signature verification.

License file format (single line):
    <base64url_payload>.<base64url_signature>

Payload is a UTF-8 JSON object:
    {
        "customer": "acme-corp",
        "host_fingerprint": "<sha256 hex of hostname + primary MAC>",
        "issued_at": "2026-04-17",           # ISO date
        "expires_at": "2027-04-17" | null,   # ISO date or null for perpetual
        "features": ["ai_urls", "ip_rotation"],
        "nonce": "<random 8 bytes hex>"
    }

Signature is ed25519 over the raw payload bytes (before base64).

Public key is embedded in `public_key.pem` and verified against every boot.
Private key is never distributed — lives only on the vendor machine.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

logger = logging.getLogger(__name__)

# ─── Paths ────────────────────────────────────────────────────────────
_LICENSING_DIR = Path(__file__).resolve().parent
PUBLIC_KEY_PATH = _LICENSING_DIR / "public_key.pem"


def license_path() -> Path:
    """Default /etc/graphenmail/license.key; overridable via LICENSE_PATH env."""
    override = os.getenv("LICENSE_PATH")
    if override:
        return Path(override)
    # On Windows dev boxes fall back to repo root; on Linux use /etc.
    if os.name == "nt":
        return _LICENSING_DIR.parent / "license.key"
    return Path("/etc/graphenmail/license.key")


# ─── Errors & state ───────────────────────────────────────────────────

class LicenseError(Exception):
    """Raised when license validation fails. Message is user-facing."""


@dataclass
class LicenseState:
    valid: bool
    customer: str = ""
    issued_at: str = ""
    expires_at: str | None = None
    features: tuple[str, ...] = ()
    error: str = ""
    host_fingerprint: str = ""

    def to_dict(self) -> dict:
        return {
            "valid": self.valid,
            "customer": self.customer,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "features": list(self.features),
            "error": self.error,
            "host_fingerprint": self.host_fingerprint,
        }


# ─── Host fingerprint ─────────────────────────────────────────────────

def _primary_mac() -> str:
    """Return the primary MAC address as 12 hex chars. Stable per VPS."""
    mac_int = uuid.getnode()
    return f"{mac_int:012x}"


def compute_host_fingerprint() -> str:
    """
    SHA256(hostname + primary_mac) — stable per machine, not tied to external IP.
    Vendor uses this to bind a license to a specific VPS.
    """
    host = socket.gethostname().lower().strip()
    mac = _primary_mac()
    return hashlib.sha256(f"{host}|{mac}".encode()).hexdigest()


# ─── Public key loading ───────────────────────────────────────────────

_public_key: Ed25519PublicKey | None = None


def _load_public_key() -> Ed25519PublicKey | None:
    global _public_key
    if _public_key is not None:
        return _public_key
    if not PUBLIC_KEY_PATH.exists():
        return None
    try:
        data = PUBLIC_KEY_PATH.read_bytes()
        key = serialization.load_pem_public_key(data)
        if not isinstance(key, Ed25519PublicKey):
            logger.error("License public key is not ed25519.")
            return None
        _public_key = key
        return _public_key
    except Exception as e:
        logger.error(f"Failed to load license public key: {e}")
        return None


# ─── Core validation ──────────────────────────────────────────────────

def _b64decode(s: str) -> bytes:
    # Accept both urlsafe and standard base64, with or without padding.
    s = s.strip()
    pad = "=" * (-len(s) % 4)
    try:
        return base64.urlsafe_b64decode(s + pad)
    except Exception:
        return base64.b64decode(s + pad)


def load_license() -> str | None:
    """Read raw license text from disk. Returns None if not present."""
    path = license_path()
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception as e:
        logger.error(f"Cannot read license file {path}: {e}")
        return None


def install_license(text: str) -> None:
    """Write license text to disk with 0600 permissions."""
    path = license_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass  # Windows dev boxes


# ─── Cache ────────────────────────────────────────────────────────────

_CACHE_TTL = 600  # 10 minutes
_cache: tuple[float, LicenseState] | None = None

# ─── Master admin key ─────────────────────────────────────────────────
# Plaintext bypass key for the product owner. If the license file
# contains exactly this string (or the env var is set to it), the
# validator short-circuits to a valid perpetual all-features state.
# Not tied to a machine — works on every install.
MASTER_ADMIN_KEY = "GRAPHENMAIL-MASTER-ADMIN-2026"


def _cached_state() -> LicenseState | None:
    global _cache
    if _cache is None:
        return None
    ts, state = _cache
    if time.time() - ts > _CACHE_TTL:
        return None
    return state


def invalidate_cache() -> None:
    global _cache
    _cache = None


# ─── Entry point ──────────────────────────────────────────────────────

def validate(force: bool = False) -> LicenseState:
    """
    Validate the license on disk. Returns a LicenseState.
    Uses a 10-minute cache unless force=True.
    """
    global _cache

    fp = compute_host_fingerprint()

    if not force:
        cached = _cached_state()
        if cached is not None:
            return cached

    state = _do_validate(fp)
    _cache = (time.time(), state)
    return state


def _do_validate(fp: str) -> LicenseState:
    raw = load_license()
    env_key = os.getenv("GRAPHENMAIL_MASTER_KEY", "").strip()
    if env_key == MASTER_ADMIN_KEY or (raw and raw.strip() == MASTER_ADMIN_KEY):
        return LicenseState(
            valid=True,
            host_fingerprint=fp,
            customer="admin-master",
            issued_at=date.today().isoformat(),
            expires_at=None,
            features=("ai_urls", "ip_rotation"),
        )

    pub_key = _load_public_key()
    if pub_key is None:
        return LicenseState(
            valid=False,
            host_fingerprint=fp,
            error=(
                "Vendor public key missing (licensing/public_key.pem). "
                "Contact support."
            ),
        )

    if not raw:
        return LicenseState(
            valid=False,
            host_fingerprint=fp,
            error="No license installed.",
        )

    if "." not in raw:
        return LicenseState(
            valid=False,
            host_fingerprint=fp,
            error="License file is malformed (missing signature separator).",
        )

    payload_b64, sig_b64 = raw.split(".", 1)
    try:
        payload_bytes = _b64decode(payload_b64)
        sig_bytes = _b64decode(sig_b64)
    except Exception:
        return LicenseState(
            valid=False,
            host_fingerprint=fp,
            error="License file base64 decoding failed.",
        )

    try:
        pub_key.verify(sig_bytes, payload_bytes)
    except InvalidSignature:
        return LicenseState(
            valid=False,
            host_fingerprint=fp,
            error="License signature invalid — likely tampered or wrong vendor.",
        )
    except Exception as e:
        return LicenseState(
            valid=False,
            host_fingerprint=fp,
            error=f"Signature verification error: {e}",
        )

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        return LicenseState(
            valid=False,
            host_fingerprint=fp,
            error="License payload is not valid JSON.",
        )

    customer = str(payload.get("customer", "")).strip()
    host_fp_claim = str(payload.get("host_fingerprint", "")).strip().lower()
    issued_at = str(payload.get("issued_at", "")).strip()
    expires_at = payload.get("expires_at")
    features = tuple(payload.get("features", []) or [])

    # Wildcard "*" is an admin/master key — binds to no specific machine.
    # Vendor-only: only issue these to yourself for support/demo.
    if host_fp_claim == "*":
        pass
    elif host_fp_claim != fp:
        return LicenseState(
            valid=False,
            host_fingerprint=fp,
            customer=customer,
            error=(
                "License is bound to a different machine. "
                f"Expected fingerprint: {host_fp_claim[:16]}… got {fp[:16]}…"
            ),
        )

    if expires_at:
        try:
            exp = date.fromisoformat(str(expires_at))
        except ValueError:
            return LicenseState(
                valid=False,
                host_fingerprint=fp,
                customer=customer,
                error=f"License has malformed expires_at: {expires_at!r}",
            )
        if exp < date.today():
            return LicenseState(
                valid=False,
                host_fingerprint=fp,
                customer=customer,
                issued_at=issued_at,
                expires_at=str(expires_at),
                features=features,
                error=f"License expired on {exp.isoformat()}.",
            )

    return LicenseState(
        valid=True,
        host_fingerprint=fp,
        customer=customer,
        issued_at=issued_at,
        expires_at=str(expires_at) if expires_at else None,
        features=features,
    )
