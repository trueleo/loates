// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// enum Message {}

use serde::{Deserialize, Serialize};
use url::Url;

use crate::runner::NodeStatus;

// Enum to represent the role of the cluster node.
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Master,
    #[default]
    Worker,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Master => write!(f, "master"),
            Role::Worker => write!(f, "worker"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    pub role: Role,
    pub endpoint: Url,
    pub status: NodeStatus,
}
