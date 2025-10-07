use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;

use crate::data::DatastoreModifier;
use crate::data::RuntimeDataStore;
use crate::logical::Scenario;
use crate::meta::ClusterConfig;
use crate::{CRATE_NAME, SPAN_EXEC, SPAN_SCENARIO};

use crate::logical;

use anyhow::anyhow;
use async_scoped::{self, Scope};
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use tracing::{event, Instrument};

/// The Runner struct is the top level struct for managing and executing series of logical scenarios asynchronously.
pub struct Runner {
    logical: LogicalContext,
    #[cfg(feature = "tui")]
    enable_tui: bool,
    #[cfg(feature = "web")]
    enable_web: bool,
    #[cfg(feature = "meta")]
    distributed_config: Option<ClusterConfig>,
}

impl Runner {
    // Create new instance of Runner with a [Config](crate::config::Config) and list of [Scenario](create::logical::Scenario)
    pub fn new(scenarios: Vec<logical::Scenario>) -> Runner {
        Self {
            logical: LogicalContext { scenarios },
            #[cfg(feature = "tui")]
            enable_tui: false,
            #[cfg(feature = "web")]
            enable_web: false,
            #[cfg(feature = "meta")]
            distributed_config: None,
        }
    }

    pub async fn _run(&self) -> Result<(), crate::error::Error> {
        for (scenario_index, scenario) in self.scenarios().iter().enumerate() {
            let (tx, rx) = tokio::sync::mpsc::channel(1024);
            let mut runner = ControlledRun {
                id: scenario_index,
                scenario: scenario.clone(),
                contexts: vec![],
                is_context_init: false,
                _receiver: rx,
            };
            runner.run_scenario().await?;
        }

        event!(name: "runner_exit", target: CRATE_NAME, tracing::Level::INFO, "Exit test");
        Ok(())
    }

    // Spawn the runner
    pub async fn run(&self) -> Result<(), crate::error::Error> {
        #[cfg(feature = "tui")]
        let tui_handle = self.spawn_tui();

        #[cfg(feature = "web")]
        let web_handle = self.spawn_web();

        #[cfg(feature = "meta")]
        let meta_handle = self.spawn_meta();

        #[cfg(any(feature = "web", feature = "tui"))]
        self._run().await?;

        #[cfg(feature = "tui")]
        if let Some(handle) = tui_handle {
            let _ = handle.join();
        }

        #[cfg(feature = "web")]
        if let Some(handle) = web_handle {
            let _ = handle.await;
        }

        #[cfg(feature = "meta")]
        if let Some(handle) = meta_handle {
            let _ = handle.await;
        }

        Ok(())
    }

    pub fn scenarios(&self) -> &[logical::Scenario] {
        &self.logical.scenarios
    }

    #[cfg(feature = "tui")]
    pub fn enable_tui(mut self, enable: bool) -> Self {
        self.enable_tui = enable;
        self
    }

    #[cfg(feature = "web")]
    pub fn enable_web(mut self, enable: bool) -> Self {
        self.enable_web = enable;
        self
    }

    #[cfg(feature = "meta")]
    pub fn with_distributed(mut self, config: ClusterConfig) -> Self {
        self.distributed_config = Some(config);
        self
    }

    #[cfg(feature = "tui")]
    fn spawn_tui(
        &self,
    ) -> Option<std::thread::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> {
        use std::sync::{Arc, Mutex};

        if !self.enable_tui {
            return None;
        }

        let (tx, rx) = crate::channel();

        let tracer = crate::tracing::TracerLayer::new(tx);
        let subscriber = tracing_subscriber::layer::SubscriberExt::with(
            tracing_subscriber::Registry::default(),
            tracer,
        );

        tracing::subscriber::set_global_default(subscriber).unwrap();

        let app = Arc::new(Mutex::new(crate::app::App::new(&self.logical.scenarios)));
        Some(std::thread::spawn(|| crate::app::tui::run(app, rx)))
    }

    #[cfg(feature = "web")]
    fn spawn_web(
        &self,
    ) -> Option<tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> {
        use std::sync::{Arc, Mutex};

        if !self.enable_web {
            return None;
        }

        let (tx, rx) = crate::channel();

        let tracer = crate::tracing::TracerLayer::new(tx);
        let subscriber = tracing_subscriber::layer::SubscriberExt::with(
            tracing_subscriber::Registry::default(),
            tracer,
        );

        tracing::subscriber::set_global_default(subscriber).unwrap();

        let app = Arc::new(Mutex::new(crate::app::App::new(&self.logical.scenarios)));
        Some(tokio::spawn(crate::app::web::run(app, rx)))
    }

    #[cfg(feature = "meta")]
    fn spawn_meta(
        &self,
    ) -> Option<tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> {
        use std::sync::{Arc, Mutex};

        let cluster_config = self.distributed_config.clone().unwrap_or_default();

        let (tx, rx) = crate::channel();

        let tracer = crate::tracing::TracerLayer::new(tx);
        let subscriber = tracing_subscriber::layer::SubscriberExt::with(
            tracing_subscriber::Registry::default(),
            tracer,
        );

        tracing::subscriber::set_global_default(subscriber).unwrap();

        let app = Arc::new(Mutex::new(crate::app::App::new(&self.logical.scenarios)));
        // Some(tokio::spawn(todo!()))
        todo!()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunCommand {
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
struct LogicalContext {
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

    pub async fn modify(&mut self, f: &dyn DatastoreModifier) {
        f.init_store(&mut self.datastore).await;
    }

    pub fn datastore_mut(&mut self) -> &mut RuntimeDataStore {
        &mut self.datastore
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
