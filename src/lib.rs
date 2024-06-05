pub mod config;
pub mod data;
pub mod error;
pub mod executor;
pub mod logical;
pub mod runner;
pub mod tracing;
#[cfg(feature = "tui")]
pub mod tui;
pub mod user;

pub use crate::logical::Scenario;
pub use runner::Config;
pub use runner::Runner;
pub use user::User;

pub type UserResult = Result<(), crate::error::Error>;

pub use futures_channel::mpsc::unbounded as channel;
pub use futures_channel::mpsc::UnboundedReceiver as Receiver;
pub use futures_channel::mpsc::UnboundedSender as Sender;

const CRATE_NAME: &str = env!("CARGO_PKG_NAME");
const TARGET_USER_EVENT: &str = "user_event";
const SPAN_TASK: &str = "task";
const SPAN_EXEC: &str = "execution";
const SPAN_SCENARIO: &str = "scenario";
