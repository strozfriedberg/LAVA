use thiserror::Error;

pub type Result<T> = std::result::Result<T, LavaError>;

#[derive(Debug, Clone, Error)]
pub enum PhaseError {
    #[error("Metadata Retreival Error: {0}")]
    MetaDataRetieval(String),
    #[error("Timestamp Discovery Error: {0}")]
    TimeDiscovery(String),
    #[error("Timestamp Order Error: {0}")]
    TimeDirection(String),
    #[error("File Streaming Error: {0}")]
    FileStreaming(String),
    #[error("Formatting Error: {0}")]
    Formatting(String),
}

#[derive(PartialEq, Debug, Clone)]
pub enum LavaErrorLevel {
    Critical, // This is going to mean you stop processing, anythign else will get propogated at the end, but it will still attempt to keep doing other processsing
    High,
    Medium,
    Low,
    Info
}

#[derive(Debug, Error, Clone)]
#[error("{reason}")]
pub struct LavaError {
    pub reason: String,
    pub level: LavaErrorLevel
}

impl LavaError {
    pub fn new(reason: impl Into<String>, level: LavaErrorLevel ) -> Self {
        Self {
            reason: reason.into(),
            level: level
        }
    }
}
