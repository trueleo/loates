// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// enum Message {}

use serde::{Deserialize, Serialize};
use ulid::Ulid;
use url::Url;

use super::Role;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub enum NodeStatus {
    RunningTest(Ulid),
    Stopping(Ulid),
    #[default]
    Idle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    pub role: Role,
    pub endpoint: Url,
    pub status: NodeStatus,
}
