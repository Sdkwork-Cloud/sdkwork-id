# Repository Guidelines

## SDKWORK Standards

Canonical SDKWORK specs path: `../sdkwork-specs/README.md`

## Project Purpose

`sdkwork-id` provides unified ID generation for SDKWork:
- **Snowflake** — ordered int64 IDs for primary keys
- **UUID v4/v5** — random or deterministic string IDs

## Code Style

- Rust 2021 edition
- No external dependencies beyond `uuid`
- All public types implement `IdGenerator` trait
- Deterministic generation via `generate_at()` for testing

## Documentation Canon

- [docs/README.md](docs/README.md)
- [docs/product/prd/PRD.md](docs/product/prd/PRD.md)
- [docs/architecture/tech/TECH_ARCHITECTURE.md](docs/architecture/tech/TECH_ARCHITECTURE.md)

