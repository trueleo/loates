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

pub use macro_rules_attribute::apply;
pub use tokio::sync::mpsc::unbounded_channel as channel;
pub use tokio::sync::mpsc::UnboundedReceiver as Receiver;
pub use tokio::sync::mpsc::UnboundedSender as Sender;

const CRATE_NAME: &str = env!("CARGO_PKG_NAME");
pub const USER_TASK: &str = "user_event";
const SPAN_TASK: &str = "task";
const SPAN_EXEC: &str = "execution";
const SPAN_SCENARIO: &str = "scenario";
