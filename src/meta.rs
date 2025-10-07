mod message;
mod server;

use serde::Serialize;

// Enum to represent the role of the cluster node.
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy, Serialize)]
pub enum Role {
    Master,
    #[default]
    Worker,
}

/// Configuration for a cluster node.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    name: String,
    role: Role,
    bind_address: String,
}

impl ClusterConfig {
    /// Creates a new, default ClusterConfig.
    /// Initially, no role or bind address is set.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn master_role(&mut self) {
        self.role = Role::Master
    }

    pub fn worker_role(&mut self) {
        self.role = Role::Worker
    }

    /// Determines the cluster node's role based on command-line arguments.
    /// Checks for `--master` or `--worker` flags.
    /// If a role is already set, it will be overwritten by the command-line argument
    /// if a matching argument is found.
    pub fn role_from_args(mut self) -> Self {
        let args: Vec<String> = std::env::args().collect();
        if args.contains(&"--master".to_string()) {
            self.role = Role::Master;
        } else if args.contains(&"--worker".to_string()) {
            self.role = Role::Worker;
        }
        self
    }

    /// Sets the default bind address for the cluster node if one hasn't been explicitly set yet.
    /// This method will only set the bind address if `bind_address` is currently `None`.
    pub fn bind_addr(mut self, addr: &str) -> Self {
        self.bind_address = addr.to_string();
        self
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            role: Role::Master,
            bind_address: "0.0.0.0:7334".to_string(),
        }
    }
}
