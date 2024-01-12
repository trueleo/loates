pub mod config;
pub mod data;
pub mod error;
pub mod executor;
pub mod logical;
pub mod report;
pub mod runner;
pub mod user;

pub use crate::logical::Scenario;
pub use runner::Config;
pub use runner::Runner;
pub use user::User;

pub type UserResult = Result<report::Report, error::Error>;

pub use futures_channel::mpsc::unbounded as channel;
pub use futures_channel::mpsc::UnboundedReceiver as Receiver;
pub use futures_channel::mpsc::UnboundedSender as Sender;
