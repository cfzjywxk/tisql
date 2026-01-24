use crate::transaction::IsolationLevel;

/// Session configuration
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub default_schema: String,
    pub isolation_level: IsolationLevel,
    pub autocommit: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            default_schema: "default".to_string(),
            isolation_level: IsolationLevel::default(),
            autocommit: true,
        }
    }
}

/// Session state
pub struct Session {
    pub id: u64,
    pub config: SessionConfig,
}

impl Session {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            config: SessionConfig::default(),
        }
    }
}
