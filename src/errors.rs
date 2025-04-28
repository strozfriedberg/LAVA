use thiserror::Error;

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
} // Should prob actually use this for the different stages of processing, Metadata extraction error, File Error, etc

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