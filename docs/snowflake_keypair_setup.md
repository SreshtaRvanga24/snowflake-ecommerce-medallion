# Snowflake Key-Pair Authentication Setup

The pipeline (Airflow and dbt) connects to Snowflake as the service user `PIPELINE_SVC`
using key-pair authentication instead of a password.

> **Never commit the private key (`rsa_key.p8`).** It works like a password.
> Keys live outside the repo in `~/.snowflake/keys/`, and `.gitignore` blocks `*.p8`, `*.pem` and `*.key` as a second safeguard.

## 1. Generate the key pair

```bash
mkdir -p ~/.snowflake/keys && cd ~/.snowflake/keys

# Private key (unencrypted PKCS#8)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Public key derived from it
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Restrict access to the private key
chmod 600 rsa_key.p8

ls -la   # expect rsa_key.p8 and rsa_key.pub
```

## 2. Register the public key in Snowflake

Print the public key:

```bash
cat ~/.snowflake/keys/rsa_key.pub
```

Copy the text **between** `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----`
(not those two lines). In Snowsight, as `SECURITYADMIN`, run section 5 of
`snowflake/setup/01_account_setup.sql` with the key pasted in:

```sql
CREATE USER IF NOT EXISTS PIPELINE_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = LOADER_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  RSA_PUBLIC_KEY = '<paste_public_key_here>';
```

For a user that already exists, use:

```sql
ALTER USER PIPELINE_SVC SET RSA_PUBLIC_KEY = '<paste_public_key_here>';
```

Keep the placeholder in the committed SQL file. The real key is pasted only in Snowsight.

## 3. Verify the fingerprint

In Snowsight:

```sql
DESC USER PIPELINE_SVC;   -- note the value of RSA_PUBLIC_KEY_FP
```

Locally:

```bash
openssl rsa -pubin -in ~/.snowflake/keys/rsa_key.pub -outform DER \
  | openssl dgst -sha256 -binary | openssl enc -base64
```

The local output must match the part after `SHA256:` in `RSA_PUBLIC_KEY_FP`.

## 4. Point the project at the key

In `.env` (gitignored), use the **full path**, because `~` is not expanded by every tool:

```bash
SNOWFLAKE_USER=PIPELINE_SVC
SNOWFLAKE_PRIVATE_KEY_PATH=/Users/<you>/.snowflake/keys/rsa_key.p8
```

## 5. Rotating the key

Snowflake allows two active public keys per user, so rotation needs no downtime:

```sql
ALTER USER PIPELINE_SVC SET RSA_PUBLIC_KEY_2 = '<new_public_key>';
-- switch .env to the new private key and confirm the pipeline connects, then:
ALTER USER PIPELINE_SVC UNSET RSA_PUBLIC_KEY;
```

## Before every commit

```bash
git status                                  # .env and *.p8 must not appear
git diff --cached | grep -i "BEGIN\|MII"    # must print nothing
```
