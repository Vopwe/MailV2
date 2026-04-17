"""
GraphenMail license module.

Offline ed25519-signed licenses bound to the installation VPS.
The app validates every boot; no phone-home, no license server needed.
"""
from licensing.validator import (
    LicenseError,
    LicenseState,
    compute_host_fingerprint,
    install_license,
    load_license,
    validate,
)

__all__ = [
    "LicenseError",
    "LicenseState",
    "compute_host_fingerprint",
    "install_license",
    "load_license",
    "validate",
]
