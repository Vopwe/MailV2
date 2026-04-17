# GraphenMail License Module

## One-time vendor setup

1. Generate your signing key pair and keep the private key offline:

   ```bash
   python -m licensing.issue keygen --out licensing/
   ```

   This writes:
   - `licensing/signing_key.pem` - private, never commit or ship
   - `licensing/public_key.pem` - public, commit and bundle with the app

2. Move `signing_key.pem` to a secure location outside the repo.

3. Optional: on your own admin install, set `GM_LICENSE_SIGNING_KEY_PATH=/secure/path/signing_key.pem`
   to enable the admin-only License Lab UI at `/admin/licenses`.

## Issue a customer-bound license

1. Customer runs:

   ```bash
   python -m licensing.fingerprint
   ```

2. Customer sends you the 64-char fingerprint.

3. You sign the license:

   ```bash
   python -m licensing.issue sign \
     --signing-key ~/.graphenmail/signing_key.pem \
     --customer "Acme Corp" \
     --host-fingerprint "abc123..." \
     --months 1 \
     --features ai_urls,ip_rotation \
     > acme.license.key
   ```

Expiry options:
- `--days 30` for 30 days
- `--months 1` for one calendar month
- `--expires 2027-04-17` for a fixed date
- `--perpetual` or no expiry flag for forever

## Issue your own admin/support license

Use wildcard fingerprint `*` to make one that works on any machine:

```bash
python -m licensing.issue sign \
  --signing-key ~/.graphenmail/signing_key.pem \
  --customer "GraphenMail Admin" \
  --host-fingerprint "*" \
  --perpetual \
  --features ai_urls,ip_rotation \
  > admin.license.key
```

## Install a license

Customer pastes the key into Settings -> License, or:

```bash
sudo cp acme.license.key /etc/graphenmail/license.key
sudo chmod 600 /etc/graphenmail/license.key
sudo systemctl restart graphenmail
```

## File format

One line:

    <base64url_payload>.<base64url_signature>

Payload is signed JSON. Validation is fully offline using bundled `public_key.pem`.
Wildcard `host_fingerprint: "*"` is valid for vendor/admin/support licenses.
Plaintext master keys are not supported.

## Cache

The app caches a validated license for 10 minutes. Restart the service after
replacing the file, or use Settings -> License -> Re-validate.
