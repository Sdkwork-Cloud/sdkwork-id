use uuid::Uuid;

use crate::{uuid_to_string, IdGenError, IdGenerator};

/// UUID v4 random ID generator.
#[derive(Clone)]
pub struct UuidIdGenerator {
    prefix: String,
}

impl UuidIdGenerator {
    /// Create a generator with an optional prefix (e.g. "user_").
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }
}

impl IdGenerator for UuidIdGenerator {
    fn next_id(&self) -> Result<String, IdGenError> {
        Ok(format!("{}{}", self.prefix, uuid_to_string(Uuid::new_v4())))
    }
    fn label(&self) -> &str {
        "uuid-v4"
    }
}

/// UUID v5 namespace-based ID generator (deterministic).
pub struct UuidV5Generator {
    namespace: Uuid,
    prefix: String,
}

impl UuidV5Generator {
    pub fn new(namespace: Uuid, prefix: &str) -> Self {
        Self {
            namespace,
            prefix: prefix.to_string(),
        }
    }

    pub fn from_namespace_str(namespace: &str, prefix: &str) -> Result<Self, uuid::Error> {
        let ns = Uuid::parse_str(namespace)?;
        Ok(Self {
            namespace: ns,
            prefix: prefix.to_string(),
        })
    }

    pub fn generate_for(&self, name: &str) -> Result<String, uuid::Error> {
        let id = Uuid::new_v5(&self.namespace, name.as_bytes());
        Ok(format!("{}{}", self.prefix, id.as_hyphenated()))
    }
}

impl IdGenerator for UuidV5Generator {
    fn next_id(&self) -> Result<String, IdGenError> {
        // Fallback: use v4 when no name is provided
        Ok(format!("{}{}", self.prefix, uuid_to_string(Uuid::new_v4())))
    }
    fn label(&self) -> &str {
        "uuid-v5"
    }
}

/// Convenience: generate a UUID v4 string.
pub fn uuid_v4() -> String {
    Uuid::new_v4().as_hyphenated().to_string()
}

/// Convenience: generate a UUID v4 string with prefix.
pub fn uuid_v4_with_prefix(prefix: &str) -> String {
    format!("{}{}", prefix, uuid_v4())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_v4_is_unique() {
        let a = uuid_v4();
        let b = uuid_v4();
        assert_ne!(a, b);
    }

    #[test]
    fn uuid_v4_with_prefix_format() {
        let id = uuid_v4_with_prefix("user_");
        assert!(id.starts_with("user_"));
    }

    #[test]
    fn uuid_id_generator_trait() {
        let gen = UuidIdGenerator::new("test_");
        match gen.next_id() {
            Ok(id) => {
                assert!(id.starts_with("test_"));
                assert_eq!(gen.label(), "uuid-v4");
            }
            Err(e) => panic!("should generate id: {e}"),
        }
    }

    #[test]
    fn uuid_v5_deterministic() {
        let gen =
            UuidV5Generator::from_namespace_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "user_")
                .unwrap();
        let a = gen.generate_for("alice").unwrap();
        let b = gen.generate_for("alice").unwrap();
        let c = gen.generate_for("bob").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
