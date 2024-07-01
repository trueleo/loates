use std::borrow::Cow;

use anyhow::anyhow;

/// Error type for user task
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// If a user task returns this error variant, it means that the user has reached a critical failure and wants the test to be stopped immediately.
    #[error(transparent)]
    TerminationError(anyhow::Error),
    /// Error variant which should be shown in the UI
    #[error(transparent)]
    GenericError(#[from] anyhow::Error),
}

impl Error {
    pub fn is_termination_err(&self) -> bool {
        matches!(self, Error::TerminationError(_))
    }
}

impl Error {
    pub fn new(err: impl Into<Cow<'static, str>>) -> Self {
        Self::GenericError(anyhow!(err.into()))
    }

    pub fn termination(err: impl Into<Cow<'static, str>>) -> Self {
        Self::TerminationError(anyhow!(err.into()))
    }
}

#[cfg(feature = "reqwest")]
impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Error::GenericError(anyhow::Error::from(value))
    }
}
