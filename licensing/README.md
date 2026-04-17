# GraphenMail License Module

## One-time vendor setup

1. Generate your signing key pair (keep private key OFFLINE):

   ```bash
   python -m licensing.issue keygen --out licensing/
   ```

   This writes:
   - `licensing/signing_key.pem` — **PRIVATE. Do not commit. Do not ship.**
   - `licensing/public_key.pem` — commit to repo, bundled with every install.

2. Move `signing_key.pem` to a secure offline location (hardware key / password manager).

3. Delete it from the repo working copy. Keep `public_key.pem` in the repo.

## Issuing a license to a customer

1. Customer runs on their VPS:

   ```bash
   python -m licensing.fingerprint
   ```

   They send the 64-char hex output to you.

2. You sign a license file bound to that fingerprint:

   ```bash
   python -m licensing.issue sign \
     --signing-key ~/.graphenmail/signing_key.pem \
     --customer "Acme Corp" \
     --host-fingerprint "abc123..." \
     --expires 2027-04-17 \
     --features ai_urls,ip_rotation \
     > acme.license.key
   ```

3. Send `acme.license.key` to the customer. They paste it into Settings → License, or run:

   ```bash
   sudo cp acme.license.key /etc/graphenmail/license.key
   sudo chmod 600 /etc/graphenmail/license.key
   sudo systemctl restart graphenmail
   ```

## File format

A license is one line:

    <base64url_payload>.<base64url_signature>

Payload is ed25519-signed JSON; signature verifies with the embedded
`public_key.pem`. Validation is offline — no network calls.

## Cache

The app caches a validated license for 10 minutes in-process to avoid
re-reading the file on every request. Restart the service after replacing
the license file, or hit Settings → License → "Re-validate".
