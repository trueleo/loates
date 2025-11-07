pub mod discovery;
pub mod message;
pub mod server;

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use crate::meta::discovery::{
    etcd::EtcdDiscovery, mdns::MdnsDiscovery, noconf::StaticDiscovery, DiscoveryService,
};

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

/// Configuration for a cluster node.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub name: String,
    pub role: Role,
    pub bind_address: SocketAddr,
    pub url: Option<url::Url>,
    pub ip: Option<IpAddr>,
    pub port: Option<u16>,
    pub discovery_provider: String,
    #[cfg(feature = "etcd")]
    pub etcd_endpoints: Vec<String>,
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

    #[cfg(feature = "mdns")]
    pub fn enable_mdns(&mut self) {
        self.discovery_provider = "mdns".to_string();
    }

    #[cfg(feature = "etcd")]
    pub fn enable_etcd(&mut self) {
        self.discovery_provider = "etcd".to_string();
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
    pub fn bind_addr(mut self, addr: &str) -> Result<Self, std::net::AddrParseError> {
        self.bind_address = SocketAddr::from_str(addr)?;
        Ok(self)
    }
}

pub async fn build_discovery(config: &ClusterConfig) -> anyhow::Result<Arc<dyn DiscoveryService>> {
    match config.discovery_provider.as_str() {
        #[cfg(feature = "mdns")]
        "mdns" => {
            let port = config.port.unwrap_or(config.bind_address.port());
            let role = config.role;
            Ok(Arc::new(MdnsDiscovery::new(port, role, None)))
        }
        #[cfg(feature = "etcd")]
        "etcd" => {
            let endpoints = config.etcd_endpoints.clone();
            let this_node = discovery::Node {
                name: config.name.clone(),
                ip: config.ip,
                url: config.url.clone().map(|url| url.to_string()),
                port: config.port,
                role: config.role,
            };
            Ok(Arc::new(EtcdDiscovery::new(endpoints, this_node).await?))
        }
        _ => Ok(Arc::new(StaticDiscovery::default())),
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:7334".parse().unwrap(),
            name: Default::default(),
            role: Default::default(),
            discovery_provider: "static".to_string(),
            ip: None,
            url: None,
            port: None,
            #[cfg(feature = "etcd")]
            etcd_endpoints: Vec::new(),
        }
    }
}
