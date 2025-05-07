use thiserror::Error;

pub type Result<T> = std::result::Result<T, LogCheckError>;

#[derive(Debug, Clone, Error)]
pub enum PhaseError {
    #[error("Output Dir Setup Error: {0}")]
    OutputDirSetup(String),
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

#[derive(Debug, Error)]
#[error("{reason}")]
pub struct LogCheckError {
    pub reason: String,
}

impl LogCheckError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}
