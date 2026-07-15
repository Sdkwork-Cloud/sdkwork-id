//! SDKWork unified ID generation.
//!
//! This crate provides high-performance, reliable ID generation for SDKWork applications:
//! - **Snowflake** — deterministic, ordered i64 IDs for database primary keys
//! - **UUID** — random v4 or namespace v5 UUIDs for opaque identifiers
//!
//! ## Features
//!
//! - Thread-safe ID generation
//! - Monotonically increasing IDs (Snowflake)
//! - Configurable node IDs for distributed systems
//! - Batch generation support
//! - Clock drift tolerance
//!
//! ## Example
//!
//! ```rust
//! use sdkwork_id_core::{SnowflakeIdGenerator, UuidIdGenerator, IdGenerator};
//!
//! // Snowflake for ordered IDs
//! let snowflake = SnowflakeIdGenerator::new(1).unwrap();
//! let id = snowflake.next_id().unwrap();
//!
//! // UUID for random IDs
//! let uuid = UuidIdGenerator::new("user_");
//! let id = uuid.next_id().unwrap();
//! ```

pub mod snowflake;
pub mod uuid_gen;

pub use snowflake::{
    current_time_millis, default_snowflake_epoch_millis, default_snowflake_profile,
    max_snowflake_node_id,
};
pub use snowflake::{
    SnowflakeIdError, SnowflakeIdGenerator, SnowflakeLeaseGuard, SnowflakeProfile,
};
pub use uuid_gen::{uuid_v4, uuid_v4_with_prefix, UuidIdGenerator};

use std::fmt;

/// A strategy-agnostic trait for generating unique identifiers.
///
/// Both Snowflake and UUID generators implement this trait, allowing
/// callers to swap ID generation strategies.
pub trait IdGenerator: Send + Sync {
    /// Generate a new identifier and return it as a string.
    fn next_id(&self) -> Result<String, IdGenError>;

    /// Human-readable label for this generator (e.g. "snowflake", "uuid-v4").
    fn label(&self) -> &str;
}

/// Error type for ID generation failures.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IdGenError {
    pub message: String,
}

impl fmt::Display for IdGenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ID generation failed: {}", self.message)
    }
}

impl From<String> for IdGenError {
    fn from(s: String) -> Self {
        Self { message: s }
    }
}

impl From<&str> for IdGenError {
    fn from(s: &str) -> Self {
        Self {
            message: s.to_string(),
        }
    }
}

/// Generate a batch of unique IDs using the given generator.
///
/// # Example
///
/// ```rust
/// use sdkwork_id_core::{SnowflakeIdGenerator, generate_batch};
///
/// let gen = SnowflakeIdGenerator::new(1).unwrap();
/// let ids = generate_batch(&gen, 100).unwrap();
/// assert_eq!(ids.len(), 100);
/// ```
pub fn generate_batch(
    generator: &dyn IdGenerator,
    count: usize,
) -> Result<Vec<String>, IdGenError> {
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(generator.next_id()?);
    }
    Ok(ids)
}

/// Validate a Snowflake ID string.
///
/// Returns the decoded parts if valid: (node_id, timestamp_delta_millis, sequence).
pub fn validate_snowflake_id(id_str: &str) -> Result<(u16, u64, u16), IdGenError> {
    let id: i64 = id_str
        .parse()
        .map_err(|_| IdGenError::from("invalid Snowflake ID format"))?;
    if id <= 0 {
        return Err(IdGenError::from("Snowflake ID must be positive"));
    }

    let bits = id as u64;
    let node_id = ((bits >> 12) & 0x3FF) as u16; // 10 bits
    let timestamp_delta = bits >> 22; // 41 bits
    let sequence = (bits & 0xFFF) as u16; // 12 bits

    Ok((node_id, timestamp_delta, sequence))
}

/// Helper: convert an i64 Snowflake ID to a string.
fn i64_to_string(id: i64) -> String {
    id.to_string()
}

/// Helper: convert a UUID to a hyphenated string.
fn uuid_to_string(uuid: uuid::Uuid) -> String {
    uuid.as_hyphenated().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SnowflakeIdGenerator, UuidIdGenerator};

    #[test]
    fn generate_batch_works() {
        let gen = UuidIdGenerator::new("");
        let ids = generate_batch(&gen, 100).unwrap();
        assert_eq!(ids.len(), 100);
        // All IDs should be unique
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 100);
    }

    #[test]
    fn snowflake_batch_generates_unique_ids() {
        let gen = SnowflakeIdGenerator::new(1).unwrap();
        let ids = generate_batch(&gen, 100).unwrap();
        assert_eq!(ids.len(), 100);
        // All IDs should be unique within the same millisecond
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 100);
    }

    #[test]
    fn validate_snowflake_id_roundtrip() {
        // Use a timestamp after the default epoch (2024-01-01 = 1704067200000)
        let gen = SnowflakeIdGenerator::new(42).unwrap();
        let id = gen.generate_at(1_704_067_200_001).unwrap();
        let id_str = id.to_string();

        let (node_id, _timestamp, _sequence) = validate_snowflake_id(&id_str).unwrap();
        assert_eq!(node_id, 42);
    }
}
