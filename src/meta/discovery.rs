use std::{collections::HashMap, net::IpAddr, sync::Arc};

use url::Url;

use super::message::Role;

#[derive(Debug, Default)]
pub struct NetworkSnapshot {
    pub id: ulid::Ulid,
    pub nodes: Vec<Node>,
}

impl From<Vec<Node>> for NetworkSnapshot {
    fn from(nodes: Vec<Node>) -> Self {
        Self {
            id: ulid::Ulid::new(),
            nodes,
        }
    }
}

#[derive(Debug, Default)]
pub struct ActiveSnapshotState {
    snapshot: Arc<NetworkSnapshot>,
    peers: HashMap<String, Node>,
}

impl ActiveSnapshotState {
    pub fn new(peers: Vec<Node>) -> Self {
        let map = HashMap::from_iter(peers.iter().map(|node| (node.name.clone(), node.clone())));
        Self {
            snapshot: Arc::new(NetworkSnapshot {
                id: ulid::Ulid::new(),
                nodes: peers,
            }),
            peers: map,
        }
    }

    fn insert_node(&mut self, node: Node) {
        self.peers.insert(node.name.clone(), node);
        self.snapshot = Arc::new(NetworkSnapshot {
            id: ulid::Ulid::new(),
            nodes: self.peers.values().cloned().collect(),
        });
    }

    fn remove_node(&mut self, name: &str) {
        self.peers.remove(name);
        self.snapshot = Arc::new(NetworkSnapshot {
            id: ulid::Ulid::new(),
            nodes: self.peers.values().cloned().collect(),
        });
    }

    fn update_snapshot(&self, old_snapshot: Arc<NetworkSnapshot>) -> Arc<NetworkSnapshot> {
        let current_snapshot = &self.snapshot;
        if current_snapshot.id == old_snapshot.id {
            return old_snapshot;
        }
        let nodes = old_snapshot
            .nodes
            .iter()
            .map(|node| self.peers.get(&node.name).cloned().unwrap_or(node.clone()))
            .collect();
        Arc::new(NetworkSnapshot {
            id: current_snapshot.id,
            nodes,
        })
    }

    fn latest_snapshot(&self) -> Arc<NetworkSnapshot> {
        self.snapshot.clone()
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Node {
    pub name: String,
    pub ip: Option<IpAddr>,
    pub url: Option<String>,
    pub port: Option<u16>,
    pub role: Role,
}

impl Node {
    pub fn endpoint(&self) -> Url {
        let port = self.port.unwrap_or(80);
        if let Some(ip) = self.ip {
            format!("http://{}:{}", ip, port).parse().unwrap()
        } else {
            format!("http://{}:{}", self.url.as_ref().unwrap(), port)
                .parse()
                .unwrap()
        }
    }
}

#[async_trait::async_trait]
pub trait DiscoveryService: Send + Sync {
    async fn latest_snapshot(&self) -> Arc<NetworkSnapshot>;
    async fn update_snapshot(&self, snapshot: Arc<NetworkSnapshot>) -> Arc<NetworkSnapshot>;
    async fn register(&self, node: Node) -> anyhow::Result<()>;
}

// #[cfg(feature = "kube")]
// mod kube {
//     use super::super::message::NodeInfo;

//     use super::DiscoveryBackend;

//     use k8s_openapi::api::core::v1::Pod;

//     use kube::{
//         api::{Api, ListParams},
//         Client,
//     };

//     pub struct KubeBackend {
//         pub(crate) namespace: String,
//     }

//     #[async_trait::async_trait]
//     impl DiscoveryService for KubeBackend {
//         async fn register(&self, _node: &NodeInfo) -> anyhow::Result<()> {
//             // K8s handles registration automatically — no-op
//             Ok(())
//         }

//         async fn get_nodes(&self, service_name: &str) -> anyhow::Result<Vec<NodeInfo>> {
//             let client = Client::try_default().await?;
//             let pods: Api<Pod> = Api::namespaced(client, &self.namespace);
//             let lp = ListParams::default().labels(&format!("app={}", service_name));
//             let mut nodes = vec![];

//             for p in pods.list(&lp).await? {
//                 if let Some(ip) = p.status.and_then(|s| s.pod_ip) {
//                     nodes.push(NodeInfo {
//                         service_name: service_name.to_string(),
//                         address: ip,
//                         port: 8080, // assume common port
//                         metadata: None,
//                     });
//                 }
//             }
//             Ok(nodes)
//         }
//     }
// }

// #[cfg(feature = "consul")]
// mod consul {

//     pub struct ConsulBackend {
//         pub consul_addr: String,
//     }

//     #[async_trait::async_trait]
//     impl DiscoveryService for ConsulBackend {
//         async fn register(&self, node: &NodeInfo) -> anyhow::Result<()> {
//             let url = format!("{}/v1/agent/service/register", self.consul_addr);
//             let payload = serde_json::json!({
//                 "Name": node.service_name,
//                 "Address": node.address,
//                 "Port": node.port,
//             });
//             reqwest::Client::new()
//                 .put(&url)
//                 .json(&payload)
//                 .send()
//                 .await?
//                 .error_for_status()?;
//             Ok(())
//         }

//         async fn get_nodes(&self, service_name: &str) -> anyhow::Result<Vec<NodeInfo>> {
//             let url = format!("{}/v1/catalog/service/{}", self.consul_addr, service_name);
//             let resp = reqwest::get(url)
//                 .await?
//                 .json::<Vec<serde_json::Value>>()
//                 .await?;
//             Ok(resp
//                 .into_iter()
//                 .filter_map(|v| {
//                     Some(NodeInfo {
//                         service_name: service_name.to_string(),
//                         address: v.get("Address")?.as_str()?.to_string(),
//                         port: v.get("ServicePort")?.as_u64()? as u16,
//                         metadata: None,
//                     })
//                 })
//                 .collect())
//         }
//     }
// }

#[cfg(feature = "etcd")]
pub mod etcd {
    use std::{mem::ManuallyDrop, sync::Arc, time::Duration};

    use crate::meta::discovery::{ActiveSnapshotState, DiscoveryService, NetworkSnapshot, Node};
    use etcd_client::{Client as EtcdClient, GetOptions};
    use tokio::sync::Mutex;

    const SERVICE_KEY_PREFIX: &str = "/services/loates";

    pub struct EtcdDiscovery {
        this: Node,
        locked_state: Arc<Mutex<ActiveSnapshotState>>,
        etcd_endpoints: Vec<String>,
        join_handle: ManuallyDrop<tokio::task::JoinHandle<()>>,
    }

    impl EtcdDiscovery {
        pub async fn new(etcd_endpoints: Vec<String>, this: Node) -> anyhow::Result<Self> {
            let mut client = EtcdClient::connect(etcd_endpoints.clone(), None).await?;
            let state: Arc<Mutex<ActiveSnapshotState>> = Arc::default();

            let _this = this.clone();
            let _state = state.clone();
            let task = tokio::spawn(async move {
                loop {
                    Self::update_self_nodes(_state.clone(), &mut client, &_this).await;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });

            let backend = EtcdDiscovery {
                this: this.clone(),
                locked_state: state,
                etcd_endpoints,
                join_handle: ManuallyDrop::new(task),
            };
            backend.register(this).await?;
            Ok(backend)
        }

        async fn update_self_nodes(
            state: Arc<Mutex<ActiveSnapshotState>>,
            client: &mut EtcdClient,
            this: &Node,
        ) {
            if let Ok(nodes) = Self::get_nodes(client, this).await {
                let mut state = state.lock().await;
                if !nodes.iter().all(|node| {
                    state
                        .peers
                        .get(&node.name)
                        .is_some_and(|existing_peer| existing_peer == node)
                }) {
                    state.peers = nodes
                        .iter()
                        .map(|node| (node.name.clone(), node.clone()))
                        .collect();
                    state.snapshot = Arc::new(NetworkSnapshot::from(nodes))
                }
            }
        }

        async fn get_nodes(client: &mut EtcdClient, this: &Node) -> anyhow::Result<Vec<Node>> {
            let prefix = format!("{}/", SERVICE_KEY_PREFIX);
            let resp = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await?;
            let nodes = resp
                .kvs()
                .iter()
                .filter_map(|kv| serde_json::from_slice::<Node>(kv.value()).ok())
                .filter(|node| node.name != this.name)
                .collect();
            Ok(nodes)
        }
    }

    #[async_trait::async_trait]
    impl DiscoveryService for EtcdDiscovery {
        async fn register(&self, node: Node) -> anyhow::Result<()> {
            let mut client = EtcdClient::connect(self.etcd_endpoints.clone(), None).await?;
            let key = format!("{}/{}", SERVICE_KEY_PREFIX, node.name);
            let value = serde_json::to_string(&node)?;
            client.put(key, value, None).await?;
            Ok(())
        }

        async fn latest_snapshot(&self) -> Arc<NetworkSnapshot> {
            let Ok(mut client) = EtcdClient::connect(self.etcd_endpoints.clone(), None).await
            else {
                return self.locked_state.lock().await.latest_snapshot().clone();
            };
            Self::update_self_nodes(self.locked_state.clone(), &mut client, &self.this).await;
            let current_snapshot = self.locked_state.lock().await;
            current_snapshot.latest_snapshot()
        }

        async fn update_snapshot(&self, snapshot: Arc<NetworkSnapshot>) -> Arc<NetworkSnapshot> {
            let Ok(mut client) = EtcdClient::connect(self.etcd_endpoints.clone(), None).await
            else {
                return self
                    .locked_state
                    .lock()
                    .await
                    .update_snapshot(snapshot)
                    .clone();
            };
            Self::update_self_nodes(self.locked_state.clone(), &mut client, &self.this).await;
            let current_snapshot = self.locked_state.lock().await;
            current_snapshot.update_snapshot(snapshot)
        }
    }
}

pub mod noconf {
    use std::sync::{Arc, Mutex};

    use crate::meta::discovery::{ActiveSnapshotState, DiscoveryService, NetworkSnapshot, Node};

    #[derive(Debug, Default)]
    pub struct StaticDiscovery {
        pub locked_state: Mutex<ActiveSnapshotState>,
    }

    impl StaticDiscovery {
        pub fn new(nodes: Vec<Node>) -> Self {
            Self {
                locked_state: Mutex::new(ActiveSnapshotState::new(nodes)),
            }
        }
    }

    #[async_trait::async_trait]
    impl DiscoveryService for StaticDiscovery {
        async fn latest_snapshot(&self) -> Arc<NetworkSnapshot> {
            self.locked_state.lock().unwrap().latest_snapshot()
        }

        async fn update_snapshot(&self, snapshot: Arc<NetworkSnapshot>) -> Arc<NetworkSnapshot> {
            self.locked_state.lock().unwrap().update_snapshot(snapshot)
        }

        async fn register(&self, node: Node) -> anyhow::Result<()> {
            self.locked_state.lock().unwrap().insert_node(node);
            Ok(())
        }
    }
}

#[cfg(feature = "mdns")]
pub mod mdns {
    use anyhow::Result;
    use async_trait::async_trait;
    use std::mem::ManuallyDrop;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use zeroconf::prelude::{TEventLoop as _, TMdnsBrowser as _, TTxtRecord};
    use zeroconf::service::TMdnsService as _;
    use zeroconf::{
        MdnsBrowser, MdnsService, ServiceDiscoveredCallback, ServiceDiscovery, ServiceType,
        TxtRecord,
    };

    use crate::meta::discovery::{ActiveSnapshotState, NetworkSnapshot};
    use crate::meta::Role;

    use super::DiscoveryService;
    use super::Node;

    pub struct MdnsDiscovery {
        locked_state: Arc<Mutex<ActiveSnapshotState>>,
        joinhandle: ManuallyDrop<std::thread::JoinHandle<()>>,
    }

    impl MdnsDiscovery {
        pub fn new(port: u16, role: Role, name: Option<String>) -> Self {
            let state = Arc::new(Mutex::new(ActiveSnapshotState::default()));
            let service = ServiceType::new("loates", "tcp").unwrap();
            let mut browser = MdnsBrowser::new(service.clone());
            browser.set_service_discovered_callback(on_service_discovered(state.clone()));
            let browser_event_loop = browser.browse_services().unwrap();

            let mut txt_record = TxtRecord::new();
            let _ = txt_record.insert("role", &role.to_string());
            let mut response_service = MdnsService::new(service.clone(), port);
            if let Some(name) = name.as_ref() {
                response_service.set_name(name);
            }
            response_service.set_txt_record(txt_record);
            let response_event_loop = response_service.register().unwrap();

            let discovery_task = move || loop {
                let _ = browser_event_loop.poll(Duration::from_secs(1));
                let _ = response_event_loop.poll(Duration::from_secs(1));
            };

            let handle = std::thread::spawn(discovery_task);

            Self {
                locked_state: state,
                joinhandle: ManuallyDrop::new(handle),
            }
        }
    }

    impl Drop for MdnsDiscovery {
        fn drop(&mut self) {
            let handle = unsafe { ManuallyDrop::take(&mut self.joinhandle) };
            // Ensure the thread is joined before dropping the handle
            let _ = handle.join();
        }
    }

    fn on_service_discovered(
        state: Arc<Mutex<ActiveSnapshotState>>,
    ) -> Box<ServiceDiscoveredCallback> {
        let f = move |result: zeroconf::Result<ServiceDiscovery>,
                      _: Option<Arc<dyn std::any::Any>>| {
            if let Ok(service) = result {
                if service.service_type().name() != "loates" {
                    return;
                }
                let instance_name = service.name();
                let hostname = service.host_name().clone();
                let port = *service.port();

                let role = service
                    .txt()
                    .as_ref()
                    .and_then(|txt| txt.get("role"))
                    .map(|role| {
                        if role == "master" {
                            Role::Master
                        } else {
                            Role::Worker
                        }
                    })
                    .unwrap_or(Role::Worker);

                let node = Node {
                    name: instance_name.to_string(),
                    url: Some(hostname),
                    port: Some(port),
                    role,
                    ip: None,
                };

                state.lock().unwrap().insert_node(node);
            }
        };
        Box::new(f)
    }

    #[async_trait]
    impl DiscoveryService for MdnsDiscovery {
        async fn register(&self, node: Node) -> Result<()> {
            let _ = self.locked_state.lock().unwrap().insert_node(node);
            Ok(())
        }

        async fn latest_snapshot(&self) -> Arc<NetworkSnapshot> {
            self.locked_state.lock().unwrap().latest_snapshot()
        }

        async fn update_snapshot(&self, snapshot: Arc<NetworkSnapshot>) -> Arc<NetworkSnapshot> {
            self.locked_state.lock().unwrap().update_snapshot(snapshot)
        }
    }
}
