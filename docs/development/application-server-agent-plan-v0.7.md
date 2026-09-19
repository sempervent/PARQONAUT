# Application server agent plan (v0.7.0)

Release-specific coordination history. Move to `docs/history/v0.7.0/` at tag.

## Interface freeze

After commit `feat: define shared application and job contracts`, record:

```text
APPLICATION_CONTRACT_SHA=134b828679b4f1e648bb8336e77fd1378bcc04a2
```

Parallel workstreams start from that SHA.

## Workstreams

| Agent | Owns |
|-------|------|
| A | `paraclete-store` generic jobs, migrations, recovery |
| B | `paraclete-service` HTTP DTOs, routes, OpenAPI |
| C | `parqonaut-cli` adapters onto `parqonaut-app` |
| D | auth, storage policy, RBAC tests |
| E | API integration, parity, restart demo |
| F | CI, OpenAPI golden, Postgres job |

Lead: contracts, integration order, release.

## Integration order

1. Generic job store
2. CLI app adapters
3. HTTP app adapters
4. Auth/security
5. Integration tests
6. CI
