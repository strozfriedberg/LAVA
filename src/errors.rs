use thiserror::Error;

pub type Result<T> = std::result::Result<T, LavaError>;
use std::fmt;

#[derive(PartialEq, Debug, Clone)]
pub enum LavaErrorLevel {
    Critical, // This is going to mean you stop processing, anythign else will get propogated at the end, but it will still attempt to keep doing other processsing
    High,
    Medium,
    Low,
    Info,
}
impl fmt::Display for LavaErrorLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LavaErrorLevel::Critical => write!(f, "CRITICAL"),
            LavaErrorLevel::High => write!(f, "HIGH"),
            LavaErrorLevel::Medium => write!(f, "MEDIUM"),
            LavaErrorLevel::Low => write!(f, "LOW"),
            LavaErrorLevel::Info => write!(f, "INFO"),
        }
    }
}

#[derive(Debug, Error, Clone)]
#[error("{reason}")]
pub struct LavaError {
    pub reason: String,
    pub level: LavaErrorLevel,
}

impl LavaError {
    pub fn new(reason: impl Into<String>, level: LavaErrorLevel) -> Self {
        Self {
            reason: reason.into(),
            level: level,
        }
    }
}
