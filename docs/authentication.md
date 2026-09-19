# Authentication

The HTTP application server uses **Bearer token** authentication on `/api/v1` routes except public health endpoints.

## Roles

| Role | Typical capabilities |
|------|------------------------|
| **reader** | Read jobs, runs, results |
| **operator** | Submit scans/repairs/batch jobs, cancel permitted jobs |
| **admin** | Token create/rotate/disable |

## Token format

New tokens use the **`prqnt_`** prefix. Legacy stored token hashes remain valid when still present in the database.

## Bootstrap

On first startup, set **`PRQNT_BOOTSTRAP_ADMIN_TOKEN`** to a strong secret to create the initial administrator. The plaintext is **never** stored or logged. Do not overwrite an existing admin accidentally.

## CLI

There is no token on CLI — local `prqnt` uses your OS user and filesystem permissions. Server policy still restricts which paths and buckets the server may touch.
