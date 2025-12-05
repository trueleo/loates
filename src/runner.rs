use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;

use crate::data::RuntimeDataStore;
use crate::db::DatabaseConn;
use crate::logical::{self, Scenario};

#[cfg(feature = "meta")]
use crate::meta::{build_discovery, discovery::DiscoveryService, message::NodeInfo, ClusterConfig};

use crate::{CRATE_NAME, SPAN_EXEC, SPAN_SCENARIO};

use anyhow::anyhow;
use async_scoped::{self, Scope};
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use tokio::sync::Mutex;
use tracing::{event, Instrument};
use ulid::Ulid;

#[derive(Debug, Default, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum NodeStatus {
    RunningTest(Ulid),
    Stopping(Ulid),
    #[default]
    Idle,
}

pub enum RunnerCommand {
    Start,
    Stop,
}

pub struct RunnerState {
    pub logical: LogicalContext,
    pub test_id: Ulid,
    pub status: NodeStatus,
}

/// The Runner struct is the top level struct for managing and executing series of logical scenarios asynchronously.
pub struct Runner {
    #[cfg(feature = "meta")]
    distributed_config: ClusterConfig,
    db_connection: DatabaseConn,
    state: Arc<Mutex<RunnerState>>,
}

impl Runner {
    // Create new instance of Runner with a [Config](crate::config::Config) and list of [Scenario](create::logical::Scenario)
    pub fn new(scenarios: Vec<logical::Scenario>) -> Runner {
        let state = Arc::new(Mutex::new(RunnerState {
            logical: LogicalContext { scenarios },
            test_id: Ulid::new(),
            status: NodeStatus::Idle,
        }));

        let dir = dirs::data_dir()
            .unwrap_or_default()
            .join("loates/result.db");

        let db = DatabaseConn::new(duckdb::Connection::open(dir).unwrap());

        Self {
            #[cfg(feature = "meta")]
            distributed_config: ClusterConfig::default(),
            db_connection: db,
            state,
        }
    }

    pub async fn _run(
        &self,
        mut run_control: tokio::sync::mpsc::Receiver<RunnerCommand>,
    ) -> Result<(), crate::error::Error> {
        async fn run_this(
            scenarios: Vec<Scenario>,
            mut rx: tokio::sync::mpsc::Receiver<RunCommand>,
        ) -> Result<(), crate::error::Error> {
            for (scenario_index, scenario) in scenarios.iter().enumerate() {
                let mut runner = ControlledRun {
                    id: scenario_index,
                    scenario: scenario.clone(),
                    contexts: vec![],
                    is_context_init: false,
                    _receiver: rx,
                };
                runner.run_scenario().await?;
                rx = runner._receiver
            }
            if !scenarios.is_empty() {
                event!(name: "runner_exit", target: CRATE_NAME, tracing::Level::INFO, "Exit test");
            }
            Ok(())
        }

        let (mut current_task_channel, _rx) = tokio::sync::mpsc::channel(1024);
        let mut task_handle = run_this(vec![], _rx);
        let mut mut_task_handle = unsafe { Pin::new_unchecked(&mut task_handle) };

        loop {
            tokio::select! {
                task = run_control.recv() => match task {
                    Some(command) => match command {
                        RunnerCommand::Start => {
                            let (tx, rx) = tokio::sync::mpsc::channel(1024);
                            current_task_channel = tx;
                            task_handle = run_this(self.scenarios().await, rx);
                            mut_task_handle = unsafe { Pin::new_unchecked(&mut task_handle) };
                        }
                        RunnerCommand::Stop => {
                            let _ = current_task_channel.send(RunCommand::Stop).await;
                        }
                    },
                    None => {
                        break;
                    }
                },
                _ = &mut mut_task_handle => {}
            }
        }

        Ok(())
    }

    // Spawn the runner
    pub async fn run(&self) -> Result<(), crate::error::Error> {
        let (sender, run_control) = tokio::sync::mpsc::channel(1024);
        let handle = self.spawn_web(sender.clone());
        self._run(run_control).await?;
        let _ = handle.join();
        Ok(())
    }

    pub async fn scenarios(&self) -> Vec<logical::Scenario> {
        self.state.lock().await.logical.scenarios.clone()
    }

    #[cfg(feature = "meta")]
    pub fn with_distributed(mut self, config: ClusterConfig) -> Self {
        self.distributed_config = config;
        self
    }

    fn spawn_web(
        &self,
        command_sender: tokio::sync::mpsc::Sender<RunnerCommand>,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        use std::sync::Arc;

        let (tx, rx) = crate::channel();

        let tracer = crate::tracing::TracerLayer::new(tx);
        let subscriber = tracing_subscriber::layer::SubscriberExt::with(
            tracing_subscriber::Registry::default(),
            tracer,
        );
        tracing::subscriber::set_global_default(subscriber).unwrap();

        let state = self.state.clone();
        let db_conn = self.db_connection.try_clone();
        #[cfg(feature = "meta")]
        let distributed_config = self.distributed_config.clone();

        let task = async move {
            let mut rx = rx;
            let db_conn = db_conn?;

            #[cfg(feature = "meta")]
            let (discovery, node_info) = meta_server_state(distributed_config).await?;

            let app_state = Arc::new(tokio::sync::Mutex::new(crate::server::AppState {
                #[cfg(feature = "meta")]
                discovery,
                #[cfg(feature = "meta")]
                node_info,
                db: db_conn.try_clone()?,
                runner_state: state,
                runner_command: command_sender,
            }));

            let router = crate::server::router(app_state);

            let db = db_conn.try_clone()?;
            tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    let _ = db.write_message_type(&message);
                }
            });

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
            let _ = open::that("http://localhost:3000");
            axum::serve(listener, router.into_make_service()).await?;
            Ok(())
        };

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(task)
        })
    }
}

#[cfg(feature = "meta")]
async fn meta_server_state(
    distributed_config: ClusterConfig,
) -> anyhow::Result<(Arc<dyn DiscoveryService + 'static>, NodeInfo)> {
    #[cfg(feature = "meta")]
    let discovery = match build_discovery(&distributed_config).await {
        Ok(discovery) => discovery,
        Err(err) => return Err(anyhow::anyhow!("Failed to build discovery: {}", err)),
    };

    let port = distributed_config
        .port
        .unwrap_or(distributed_config.bind_address.port());
    let endpoint = if let Some(ip) = distributed_config.ip {
        format!("http://{}:{}", ip, port).parse().unwrap()
    } else {
        format!(
            "http://{}:{}",
            distributed_config.url.as_ref().unwrap(),
            port
        )
        .parse()
        .unwrap()
    };

    let node_info = NodeInfo {
        name: distributed_config.name,
        role: distributed_config.role,
        endpoint,
        status: NodeStatus::Idle,
    };

    Ok((discovery, node_info))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunCommand {
    Start,
    Stop,
}

struct ControlledRun {
    id: usize,
    scenario: Scenario,
    contexts: Vec<ExecutionRuntimeCtx>,
    is_context_init: bool,
    _receiver: tokio::sync::mpsc::Receiver<RunCommand>,
}

impl ControlledRun {
    async fn run_scenario(&mut self) -> Result<(), crate::error::Error> {
        let span = tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_SCENARIO, name = self.scenario.label.as_ref(), id = self.id as u64);
        let _entered = span.enter();

        async fn listen_for_stop<'a, T, F>(
            chan: &'a mut tokio::sync::mpsc::Receiver<RunCommand>,
            fut: F,
        ) -> ControlFlow<(), T>
        where
            T: 'a,
            F: Future<Output = T> + 'a,
        {
            tokio::pin!(fut);
            loop {
                tokio::select! {
                    command = chan.recv() => {
                        let command = command.unwrap();
                        if command == RunCommand::Stop {
                            return ControlFlow::Break(())
                        }
                    },
                    t = &mut fut => {
                        return ControlFlow::Continue(t)
                    }
                }
            }
        }

        if !self.is_context_init {
            self.contexts = self
                .scenario
                .execution_provider
                .iter()
                .map(|_| ExecutionRuntimeCtx::new())
                .collect();

            for (exec, ctx) in self
                .scenario
                .execution_provider
                .iter()
                .zip(self.contexts.iter_mut())
            {
                for m in exec.datastore_modifiers() {
                    if let ControlFlow::Break(_) =
                        listen_for_stop(&mut self._receiver, m.init_store(&mut ctx.datastore)).await
                    {
                        return Ok(());
                    }
                }
            }
        }

        self.is_context_init = true;

        let mut executors = vec![];
        let mut _executors: FuturesUnordered<_> = self
            .scenario
            .execution_provider
            .iter()
            .zip(self.contexts.iter())
            .enumerate()
            .map(|(i, (exec, ctx))| async move {
                let execution = exec.execution(ctx).await;
                (i, exec.start_after(), exec.config(), execution)
            })
            .collect();

        if let ControlFlow::Break(_) = listen_for_stop(&mut self._receiver, async {
            while let Some(e) = _executors.next().await {
                executors.push(e);
            }
        })
        .await
        {
            drop_exec(executors.into_iter().map(|(_, _, _, x)| x)).await;
            return Ok(());
        }

        let mut scope =
            unsafe { async_scoped::Scope::create(async_scoped::spawner::use_tokio::Tokio) };

        // channel to gather user_results from every executor.
        let (user_result_tx, user_result_rx) = crate::channel();

        let mut spawn_executor = FuturesUnordered::from_iter(executors.iter_mut().map(
            |(id, time, config, exec)| async move {
                tokio::time::sleep(*time).await;
                (id, config, exec)
            },
        ));

        if let ControlFlow::Break(_) = listen_for_stop(
            &mut self._receiver,
            async {
                // spawn executors according to their delay
                while let Some((idx, conf, runtime)) = spawn_executor.next().await {
                    let executor_name = conf.to_string();
                    let span = tracing::span!(target: CRATE_NAME, parent: &span, tracing::Level::INFO, SPAN_EXEC, name = %executor_name, id = *idx as u64);
                    let task = runtime.execute(user_result_tx.clone());
                    scope.spawn_cancellable(task.instrument(span.clone()), || ());
                }
            }
            ,
        )
        .await
        {
            drop(spawn_executor);
            drop(scope);
            drop_exec(executors.into_iter().map(|(_, _, _, x)| x)).await;
            return Ok(());
        }

        drop(spawn_executor);

        // drop last sender
        drop(user_result_tx);

        let has_user_terminated = listen_for_stop(
            &mut self._receiver,
            has_user_terminated(user_result_rx).boxed_local(),
        )
        .await;

        match has_user_terminated {
            ControlFlow::Continue(has_user_terminated) => {
                if has_user_terminated {
                    scope.cancel();
                } else {
                    Scope::collect(&mut scope).await;
                }
            }
            ControlFlow::Break(_) => {
                scope.cancel();
                return Ok(());
            }
        }

        drop(scope);
        drop_exec(executors.into_iter().map(|x| x.3)).await;

        if has_user_terminated == ControlFlow::Continue(true) {
            Err(crate::error::Error::TerminationError(anyhow!(
                "user terminated"
            )))
        } else {
            Ok(())
        }
    }
}

async fn drop_exec(executors: impl Iterator<Item = Box<dyn crate::executor::Executor<'_>>>) {
    futures::stream::FuturesUnordered::from_iter(executors.into_iter().map(|exec| exec.drop()))
        .collect()
        .await
}

#[derive(Debug, Clone)]
pub struct LogicalContext {
    scenarios: Vec<logical::Scenario>,
}

#[derive(Debug, Default)]
pub(crate) struct ExecutionRuntimeCtx {
    datastore: RuntimeDataStore,
}

impl ExecutionRuntimeCtx {
    pub fn new() -> Self {
        Self {
            datastore: RuntimeDataStore::default(),
        }
    }

    pub fn datastore(&self) -> &RuntimeDataStore {
        &self.datastore
    }
}

async fn has_user_terminated(
    mut user_result_rx: tokio::sync::mpsc::UnboundedReceiver<Result<(), crate::error::Error>>,
) -> bool {
    let mut results = Vec::with_capacity(128);
    while user_result_rx.recv_many(&mut results, 128).await > 0 {
        if let Some(err) = results.iter().filter_map(|x| x.as_ref().err()).next() {
            event!(name: "termination_error", target: CRATE_NAME, tracing::Level::INFO, err = %err);
            return true;
        }
    }
    false
}
