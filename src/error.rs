use std::borrow::Cow;

use anyhow::anyhow;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    TerminationError(anyhow::Error),
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
