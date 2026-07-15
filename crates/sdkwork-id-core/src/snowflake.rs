use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::{i64_to_string, IdGenError, IdGenerator};

/// Default epoch: 2024-01-01 00:00:00 UTC in milliseconds
const DEFAULT_EPOCH_MILLIS: u64 = 1_704_067_200_000;

/// Bit layout: 41 bits timestamp | 10 bits node | 12 bits sequence
const TIMESTAMP_BITS: u8 = 41;
const NODE_BITS: u8 = 10;
const SEQUENCE_BITS: u8 = 12;

/// Maximum values
const MAX_NODE_ID: u16 = (1 << NODE_BITS) - 1; // 1023
const MAX_SEQUENCE: u16 = (1 << SEQUENCE_BITS) - 1; // 4095

/// Bit shift constants
const NODE_SHIFT: u8 = SEQUENCE_BITS;
const TIMESTAMP_SHIFT: u8 = NODE_BITS + SEQUENCE_BITS;
const MAX_TIMESTAMP_DELTA: u64 = (1_u64 << TIMESTAMP_BITS) - 1; // ~69 years

/// Maximum allowed clock drift before error (100ms)
const MAX_CLOCK_DRIFT_MS: u64 = 100;

/// Snowflake ID generation errors
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SnowflakeIdError {
    /// The distributed node lease is no longer valid.
    LeaseUnavailable,
    /// Invalid node ID (must be 0-1023)
    InvalidNodeId { node_id: u16, max_node_id: u16 },
    /// System clock is before the epoch
    ClockBeforeEpoch { now_millis: u64, epoch_millis: u64 },
    /// Clock moved backwards (beyond drift tolerance)
    ClockMovedBackwards { last_millis: u64, now_millis: u64 },
    /// Timestamp overflow (69+ years since epoch)
    TimestampOverflow {
        delta_millis: u64,
        max_delta_millis: u64,
    },
    /// Sequence exhausted (4096 IDs in one millisecond)
    SequenceExhausted { millis: u64 },
    /// System time error
    SystemTime(String),
    /// Mutex poisoned
    StatePoisoned,
}

impl fmt::Display for SnowflakeIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeaseUnavailable => write!(f, "snowflake node lease is unavailable"),
            Self::InvalidNodeId {
                node_id,
                max_node_id,
            } => write!(f, "invalid node_id={node_id}, max={max_node_id}"),
            Self::ClockBeforeEpoch {
                now_millis,
                epoch_millis,
            } => write!(
                f,
                "clock before epoch: now={now_millis}, epoch={epoch_millis}"
            ),
            Self::ClockMovedBackwards {
                last_millis,
                now_millis,
            } => write!(
                f,
                "clock moved backwards: last={last_millis}, now={now_millis}"
            ),
            Self::TimestampOverflow {
                delta_millis,
                max_delta_millis,
            } => write!(
                f,
                "timestamp overflow: delta={delta_millis}, max={max_delta_millis}"
            ),
            Self::SequenceExhausted { millis } => {
                write!(f, "sequence exhausted at millis={millis}")
            }
            Self::SystemTime(e) => write!(f, "system time error: {e}"),
            Self::StatePoisoned => write!(f, "generator state poisoned"),
        }
    }
}

/// Thread-safe Snowflake ID generator.
///
/// Generates unique, monotonically increasing 64-bit IDs using the Snowflake algorithm.
/// - 41 bits: timestamp (milliseconds since epoch)
/// - 10 bits: node ID (0-1023)
/// - 12 bits: sequence (0-4095 per millisecond)
///
/// Supports 30+ years of operation and up to 4096 IDs per millisecond per node.
#[derive(Clone)]
pub struct SnowflakeIdGenerator {
    node_id: u16,
    epoch_millis: u64,
    state: Arc<Mutex<SnowflakeState>>,
    lease_guard: Option<Arc<SnowflakeLeaseGuard>>,
}

impl std::fmt::Debug for SnowflakeIdGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeIdGenerator")
            .field("node_id", &self.node_id)
            .field("epoch_millis", &self.epoch_millis)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy)]
struct SnowflakeState {
    last_millis: u64,
    sequence: u16,
}

impl SnowflakeIdGenerator {
    /// Create a new Snowflake ID generator with the default epoch.
    pub fn new(node_id: u16) -> Result<Self, SnowflakeIdError> {
        Self::with_epoch(node_id, DEFAULT_EPOCH_MILLIS)
    }

    /// Create a new Snowflake ID generator with a custom epoch.
    pub fn with_epoch(node_id: u16, epoch_millis: u64) -> Result<Self, SnowflakeIdError> {
        if node_id > MAX_NODE_ID {
            return Err(SnowflakeIdError::InvalidNodeId {
                node_id,
                max_node_id: MAX_NODE_ID,
            });
        }
        Ok(Self {
            node_id,
            epoch_millis,
            state: Arc::new(Mutex::new(SnowflakeState {
                last_millis: 0,
                sequence: 0,
            })),
            lease_guard: None,
        })
    }

    /// Attach a distributed lease guard to this generator.
    ///
    /// Database-backed allocators use this guard to fence ID generation when
    /// the node lease can no longer be proven valid. The check is a single
    /// relaxed atomic load on the generation hot path.
    #[must_use]
    pub fn with_lease_guard(mut self, lease_guard: Arc<SnowflakeLeaseGuard>) -> Self {
        self.lease_guard = Some(lease_guard);
        self
    }

    /// Generate a new ID using the current system time.
    pub fn generate(&self) -> Result<i64, SnowflakeIdError> {
        let now_millis = current_time_millis()?;
        self.generate_at_with_wall_clock(now_millis, now_millis)
    }

    /// Generate a new ID at a specific timestamp (useful for testing).
    pub fn generate_at(&self, now_millis: u64) -> Result<i64, SnowflakeIdError> {
        let wall_clock_millis = current_time_millis()?;
        self.generate_at_with_wall_clock(now_millis, wall_clock_millis)
    }

    fn generate_at_with_wall_clock(
        &self,
        now_millis: u64,
        wall_clock_millis: u64,
    ) -> Result<i64, SnowflakeIdError> {
        if self
            .lease_guard
            .as_ref()
            .is_some_and(|guard| !guard.allows(wall_clock_millis))
        {
            return Err(SnowflakeIdError::LeaseUnavailable);
        }
        if self.lease_guard.is_some() && now_millis.abs_diff(wall_clock_millis) > MAX_CLOCK_DRIFT_MS
        {
            return Err(SnowflakeIdError::ClockMovedBackwards {
                last_millis: wall_clock_millis,
                now_millis,
            });
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| SnowflakeIdError::StatePoisoned)?;
        self.next_id_at(now_millis, &mut state)
    }

    /// Get the node ID for this generator.
    pub fn node_id(&self) -> u16 {
        self.node_id
    }

    /// Get the epoch used by this generator.
    pub fn epoch_millis(&self) -> u64 {
        self.epoch_millis
    }

    /// Internal: generate next ID at given timestamp.
    fn next_id_at(
        &self,
        now_millis: u64,
        state: &mut SnowflakeState,
    ) -> Result<i64, SnowflakeIdError> {
        // Check clock before epoch
        if now_millis < self.epoch_millis {
            return Err(SnowflakeIdError::ClockBeforeEpoch {
                now_millis,
                epoch_millis: self.epoch_millis,
            });
        }

        // Check for excessive clock drift (beyond 100ms tolerance)
        if state.last_millis > now_millis.saturating_add(MAX_CLOCK_DRIFT_MS) {
            return Err(SnowflakeIdError::ClockMovedBackwards {
                last_millis: state.last_millis,
                now_millis,
            });
        }

        // Pin small clock rollbacks to the last logical millisecond. Never
        // move the encoded timestamp backwards or reset sequence at an older
        // timestamp, otherwise a clock recovery can duplicate an earlier ID.
        let logical_millis = state.last_millis.max(now_millis);
        if state.last_millis == logical_millis {
            if state.sequence == MAX_SEQUENCE {
                return Err(SnowflakeIdError::SequenceExhausted {
                    millis: logical_millis,
                });
            }
            state.sequence += 1;
        } else {
            state.last_millis = logical_millis;
            state.sequence = 0;
        }

        // Calculate delta from epoch
        let delta_millis = logical_millis - self.epoch_millis;
        if delta_millis > MAX_TIMESTAMP_DELTA {
            return Err(SnowflakeIdError::TimestampOverflow {
                delta_millis,
                max_delta_millis: MAX_TIMESTAMP_DELTA,
            });
        }

        // Combine bits: timestamp | node_id | sequence
        let value = (delta_millis << TIMESTAMP_SHIFT)
            | (u64::from(self.node_id) << NODE_SHIFT)
            | u64::from(state.sequence);

        Ok(value as i64)
    }
}

/// Lock-free safety guard for a database-backed Snowflake node lease.
#[derive(Debug)]
pub struct SnowflakeLeaseGuard {
    available: AtomicBool,
    valid_until_millis: AtomicU64,
    valid_until_monotonic_millis: AtomicU64,
}

impl SnowflakeLeaseGuard {
    pub fn new(valid_until_millis: u64) -> Self {
        let remaining =
            valid_until_millis.saturating_sub(current_time_millis().unwrap_or(u64::MAX));
        Self {
            available: AtomicBool::new(true),
            valid_until_millis: AtomicU64::new(valid_until_millis),
            valid_until_monotonic_millis: AtomicU64::new(
                monotonic_millis().saturating_add(remaining),
            ),
        }
    }

    /// Extend the locally enforced lease deadline after a successful fenced heartbeat.
    pub fn renew_until(&self, valid_until_millis: u64) {
        let remaining =
            valid_until_millis.saturating_sub(current_time_millis().unwrap_or(u64::MAX));
        self.valid_until_millis
            .store(valid_until_millis, Ordering::Release);
        self.valid_until_monotonic_millis.store(
            monotonic_millis().saturating_add(remaining),
            Ordering::Release,
        );
    }

    /// Extend the local deadline by a duration measured with a monotonic
    /// clock. This cannot be prolonged by system-clock rollback.
    pub fn renew_for(&self, ttl: Duration) {
        let ttl_millis = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        self.valid_until_millis.store(
            current_time_millis()
                .unwrap_or(u64::MAX)
                .saturating_add(ttl_millis),
            Ordering::Release,
        );
        self.valid_until_monotonic_millis.store(
            monotonic_millis().saturating_add(ttl_millis),
            Ordering::Release,
        );
    }

    /// Permanently fence this generator instance.
    pub fn fence(&self) {
        self.available.store(false, Ordering::Release);
    }

    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }

    pub fn allows(&self, now_millis: u64) -> bool {
        self.is_available()
            && now_millis < self.valid_until_millis.load(Ordering::Acquire)
            && monotonic_millis() < self.valid_until_monotonic_millis.load(Ordering::Acquire)
    }
}

fn monotonic_millis() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    u64::try_from(ORIGIN.get_or_init(Instant::now).elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl IdGenerator for SnowflakeIdGenerator {
    fn next_id(&self) -> Result<String, IdGenError> {
        self.generate()
            .map(i64_to_string)
            .map_err(|e| IdGenError::from(e.to_string()))
    }

    fn label(&self) -> &str {
        "snowflake"
    }
}

/// Get the default epoch in milliseconds.
pub fn default_snowflake_epoch_millis() -> u64 {
    DEFAULT_EPOCH_MILLIS
}

/// Get the default Snowflake profile.
pub fn default_snowflake_profile() -> SnowflakeProfile {
    SnowflakeProfile {
        epoch_millis: DEFAULT_EPOCH_MILLIS,
        timestamp_bits: TIMESTAMP_BITS,
        node_bits: NODE_BITS,
        sequence_bits: SEQUENCE_BITS,
        max_node_id: MAX_NODE_ID,
        max_sequence_per_millis: MAX_SEQUENCE,
        max_timestamp_delta_millis: MAX_TIMESTAMP_DELTA,
        min_required_lifetime_years: 30,
    }
}

/// Get the maximum valid node ID.
pub fn max_snowflake_node_id() -> u16 {
    MAX_NODE_ID
}

/// Get the current system time in milliseconds since Unix epoch.
pub fn current_time_millis() -> Result<u64, SnowflakeIdError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SnowflakeIdError::SystemTime(error.to_string()))?;
    Ok(duration.as_millis() as u64)
}

/// Snowflake profile describing the bit layout.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SnowflakeProfile {
    pub epoch_millis: u64,
    pub timestamp_bits: u8,
    pub node_bits: u8,
    pub sequence_bits: u8,
    pub max_node_id: u16,
    pub max_sequence_per_millis: u16,
    pub max_timestamp_delta_millis: u64,
    pub min_required_lifetime_years: u16,
}

impl SnowflakeProfile {
    /// Calculate how many years the timestamp can represent.
    pub fn lifetime_years(&self) -> f64 {
        const MILLIS_PER_MEAN_YEAR: f64 = 31_556_952_000.0;
        self.max_timestamp_delta_millis as f64 / MILLIS_PER_MEAN_YEAR
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snowflake_ids_are_positive_and_monotonic() {
        let gen = SnowflakeIdGenerator::with_epoch(7, 1_700_000_000_000).unwrap();
        let a = gen.generate_at(1_700_000_000_001).unwrap();
        let b = gen.generate_at(1_700_000_000_001).unwrap();
        let c = gen.generate_at(1_700_000_000_002).unwrap();
        assert!(a > 0);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn snowflake_rejects_invalid_node_id() {
        assert!(matches!(
            SnowflakeIdGenerator::new(max_snowflake_node_id() + 1),
            Err(SnowflakeIdError::InvalidNodeId { .. })
        ));
    }

    #[test]
    fn lease_guard_fences_generation_at_deadline() {
        let wall_clock = current_time_millis().unwrap();
        let guard = Arc::new(SnowflakeLeaseGuard::new(wall_clock.saturating_sub(1)));
        let generator = SnowflakeIdGenerator::new(1)
            .unwrap()
            .with_lease_guard(guard.clone());

        assert_eq!(
            generator.generate_at(wall_clock),
            Err(SnowflakeIdError::LeaseUnavailable)
        );

        guard.renew_until(wall_clock + 10_000);
        assert!(generator.generate_at(wall_clock).is_ok());
        guard.fence();
        assert_eq!(
            generator.generate_at(wall_clock),
            Err(SnowflakeIdError::LeaseUnavailable)
        );
    }

    #[test]
    fn lease_guard_requires_wall_and_monotonic_deadlines() {
        let wall_clock = current_time_millis().unwrap();
        let guard = SnowflakeLeaseGuard::new(wall_clock + 10_000);
        assert!(guard.allows(wall_clock));

        guard
            .valid_until_monotonic_millis
            .store(monotonic_millis(), Ordering::Release);
        assert!(!guard.allows(wall_clock));
    }

    #[test]
    fn leased_generator_rejects_historical_timestamp_override() {
        let wall_clock = current_time_millis().unwrap();
        let guard = Arc::new(SnowflakeLeaseGuard::new(wall_clock + 10_000));
        let generator = SnowflakeIdGenerator::new(1)
            .unwrap()
            .with_lease_guard(guard);

        assert!(matches!(
            generator.generate_at(wall_clock - MAX_CLOCK_DRIFT_MS - 1),
            Err(SnowflakeIdError::ClockMovedBackwards { .. })
        ));
    }

    #[test]
    fn snowflake_rejects_clock_rollback() {
        // Use timestamps after the default epoch, with a large backward jump
        let gen = SnowflakeIdGenerator::with_epoch(1, 1_700_000_000_000).unwrap();
        gen.generate_at(1_700_000_001_000).unwrap();
        // Clock moves back by 200ms (beyond 100ms tolerance)
        assert!(matches!(
            gen.generate_at(1_700_000_000_800),
            Err(SnowflakeIdError::ClockMovedBackwards { .. })
        ));
    }

    #[test]
    fn snowflake_handles_clock_drift_within_tolerance() {
        // Use timestamps after the default epoch (2024-01-01 = 1704067200000)
        let gen = SnowflakeIdGenerator::new(1).unwrap();
        let t1 = gen.generate_at(1_704_067_200_001).unwrap();
        // Clock moves back by 50ms (within tolerance), then recovers.
        let t2 = gen.generate_at(1_704_067_200_000).unwrap();
        let t3 = gen.generate_at(1_704_067_200_001).unwrap();
        assert!(t1 < t2);
        assert!(t2 < t3);
        assert_ne!(t1, t2);
        assert_ne!(t2, t3);
    }

    #[test]
    fn id_generator_trait_works() {
        let gen = SnowflakeIdGenerator::new(1).unwrap();
        match gen.next_id() {
            Ok(id) => {
                assert!(!id.is_empty());
                assert_eq!(gen.label(), "snowflake");
            }
            Err(e) => panic!("should generate id: {e}"),
        }
    }

    #[test]
    fn snowflake_profile_lifetime() {
        let profile = default_snowflake_profile();
        assert!(profile.lifetime_years() >= 30.0);
    }
}
