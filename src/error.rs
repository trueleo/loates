#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    TerminationError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    GenericError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Error {
    pub fn is_termination_err(&self) -> bool {
        matches!(self, Error::TerminationError(_))
    }
}

impl Error {
    pub fn new_generic(err: &str) -> Self {
        Self::GenericError(err.into())
    }
}
