// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// enum Message {}

use serde::Serialize;

use super::Role;

#[derive(Serialize)]
pub enum NodeStatus {
    RunningTest(String),
    Stopping(String),
    Idle,
}

#[derive(Serialize)]
pub struct NodeInfo {
    pub name: String,
    pub role: Role,
    pub ip: String,
    pub status: NodeStatus,
}
