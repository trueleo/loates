/// Wrapper types for reqwest crate.
///
/// This module provides client and request builder with same api as the
/// orignal. The main reason for this wrapper client type is to override the
/// request response mechanism and emit certain traces that can be capatured
/// by rusher's tracing layer.
#[cfg(feature = "reqwest")]
pub mod reqwest;
