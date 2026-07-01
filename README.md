# sdkwork-id
repository-kind: foundation-dependency

SDKWork unified ID generation library.

## Features

- **Snowflake** — ordered int64 IDs for database primary keys (41-bit timestamp, 10-bit node, 12-bit sequence)
- **UUID v4** — random UUIDs for opaque identifiers
- **UUID v5** — deterministic UUIDs for reproducible IDs
- **IdGenerator trait** — strategy-agnostic interface for swapping ID strategies
- **Batch generation** — generate multiple IDs efficiently

## Usage

```rust
use sdkwork_id_core::{SnowflakeIdGenerator, UuidIdGenerator, IdGenerator, generate_batch};

// Snowflake IDs (ordered, positive i64)
let snowflake = SnowflakeIdGenerator::new(1)?;
let id = snowflake.next_id()?;

// UUID v4 (random)
let uuid = UuidIdGenerator::new("user_");
let id = uuid.next_id()?;

// Batch generation
let ids = generate_batch(&snowflake, 100)?;
```

## Integration with sdkwork-database-repository

The repository auto-generates IDs when using `with_snowflake()` or `with_uuid()`:

```rust
use sdkwork_database_repository::{impl_entity, impl_repository};
use sdkwork_database_sqlx::DatabasePool;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User { id: i64, name: String }

impl_entity!(User, "users", id, [id, name]);
impl_repository!(User);

// Create repository with auto-ID generation
let pool = DatabasePool::create_from_env("MY_APP").await?;
let repo = UserRepository::with_snowflake(pool, 1)?; // node_id=1

let user = User { id: 0, name: "Alice".into() };
let generated_id = repo.insert_entity(&user).await?;
// generated_id is the Snowflake ID
```

## Node ID Allocation

| Node ID | Service | Env Var |
|---------|---------|---------|
| 0 | knowledgebase | `SDKWORK_KNOWLEDGEBASE_SNOWFLAKE_NODE_ID` |
| 1 | user-center | hardcoded |
| 21 | claw-router admin app | hardcoded |
| 22 | claw-router admin skill | hardcoded |
| 23 | claw-router runtime | `SDKWORK_CLAW_SNOWFLAKE_NODE_ID` |
| 31 | drive | `SDKWORK_DRIVE_SNOWFLAKE_NODE_ID` |
| 41 | local-router | `SDKWORK_LR_SNOWFLAKE_NODE_ID` |

## Migration

This crate replaces `sdkwork-platform-id-service`. The API is fully compatible.

## Documentation Canon

- [docs/README.md](docs/README.md)
- [docs/product/prd/PRD.md](docs/product/prd/PRD.md)
- [docs/architecture/tech/TECH_ARCHITECTURE.md](docs/architecture/tech/TECH_ARCHITECTURE.md)

