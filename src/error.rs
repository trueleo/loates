use miette::Diagnostic;

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum Error {
    #[error("Oops")]
    GenericError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Error {
    pub fn new_generic(err: &str) -> Self {
        Self::GenericError(err.into())
    }
}
