use std::pin::Pin;
use std::time::{Duration, Instant};

use crate::data::DatastoreModifier;
use crate::data::RuntimeDataStore;
use crate::{CRATE_NAME, SPAN_EXEC, SPAN_SCENARIO};

use crate::logical;

use async_scoped::{self, Scope};
use tokio::sync::oneshot;
use tracing::{event, Instrument, Level};

/// The Runner struct is the top level struct for managing and executing series of logical scenarios asynchronously.
pub struct Runner<'env> {
    logical: LogicalContext<'env>,
    #[cfg(feature = "tui")]
    enable_tui: bool,
}

impl<'env> Runner<'env> {
    // Create new instance of Runner with a [Config](crate::config::Config) and list of [Scenario](create::logical::Scenario)
    pub fn new(scenarios: Vec<logical::Scenario<'env>>) -> Runner<'env> {
        Self {
            logical: LogicalContext { scenarios },
            #[cfg(feature = "tui")]
            enable_tui: true,
        }
    }

    // Spawn the runner
    pub async fn run(&self) -> Result<(), crate::error::Error> {
        #[cfg(feature = "tui")]
        let tui_handle = self.spawn_tui();

        let mut runtime_ctx = self.create_contexts();
        let runtime_ctx_mut = runtime_ctx.iter_mut().map(|x| x.iter_mut());

        let mut runtime_scenarios = Vec::new();
        for (scenario, context) in self.logical.scenarios.iter().zip(runtime_ctx_mut) {
            let mut runtime_scenario = Vec::new();
            for (exec, context) in scenario.execution_provider.iter().zip(context) {
                runtime_scenario.push((exec.label(), exec.execution(context).await))
            }
            runtime_scenarios.push((scenario.label.clone(), runtime_scenario))
        }

        for (scenario_index, (scenario_name, scenario)) in runtime_scenarios.iter_mut().enumerate()
        {
            let span = tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_SCENARIO, name = scenario_name.as_ref(), id = scenario_index as u64);
            let _entered = span.enter();
            let mut scope =
                unsafe { async_scoped::Scope::create(async_scoped::spawner::use_tokio::Tokio) };

            let mut results = vec![];
            for (executor_index, (executor_name, executor)) in scenario.iter_mut().enumerate() {
                let (sync_tx, sync_rx) = tokio::sync::oneshot::channel::<()>();
                let (task, res) = executor.execute();
                results.push(res);
                let span = tracing::span!(target: CRATE_NAME, parent: &span, tracing::Level::INFO, SPAN_EXEC, name = executor_name.as_ref(), id = executor_index as u64);
                let span_ = span.clone();
                scope.spawn_cancellable(
                    async move {
                        task.await;
                        let _ = sync_tx.send(());
                    }
                    .instrument(span_),
                    || (),
                );
                scope.spawn(unbounded_timer(sync_rx).instrument(span));
            }

            let mut res =
                tokio_stream::StreamMap::from_iter(results.into_iter().map(|mut results| {
                    (
                        "",
                        Box::pin(async_stream::stream! {
                              while let Some(item) = results.recv().await {
                                  yield item;
                              }
                        })
                            as Pin<Box<dyn futures::Stream<Item = _> + Send>>,
                    )
                }));

            let mut terminated_by_user = false;
            use tokio_stream::StreamExt;
            while let Some((_, value)) = res.next().await {
                if let Err(crate::error::Error::TerminationError(err)) = value {
                    event!(name: "termination_error", target: CRATE_NAME, tracing::Level::INFO, err = %err);
                    scope.cancel();
                    terminated_by_user = true;
                    break;
                }
            }

            if !terminated_by_user {
                Scope::collect(&mut scope).await;
            } else {
                break;
            }
        }

        event!(name: "runner_exit", target: CRATE_NAME, tracing::Level::INFO, "Exit test");

        #[cfg(feature = "tui")]
        if let Some(handle) = tui_handle {
            let _ = handle.join();
        }

        Ok(())
    }

    fn create_contexts(&self) -> Vec<Vec<ExecutionRuntimeCtx>> {
        self.logical
            .scenarios
            .iter()
            .map(|scenario| {
                scenario
                    .execution_provider
                    .iter()
                    .map(|_| ExecutionRuntimeCtx::new())
                    .collect()
            })
            .collect()
    }

    pub fn scenario(&self) -> &[logical::Scenario<'env>] {
        &self.logical.scenarios
    }

    #[cfg(feature = "tui")]
    pub fn enable_tui(mut self, enable: bool) -> Self {
        self.enable_tui = enable;
        self
    }

    #[cfg(feature = "tui")]
    fn spawn_tui(
        &self,
    ) -> Option<std::thread::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> {
        if !self.enable_tui {
            return None;
        }

        let (tracer, rx_tracer) = crate::tracing::TraceHttp::new();
        let subscriber = tracing_subscriber::layer::SubscriberExt::with(
            tracing_subscriber::Registry::default(),
            tracer,
        );

        tracing::subscriber::set_global_default(subscriber).unwrap();

        let app = crate::tui::App::new(&self.logical.scenarios);
        Some(std::thread::spawn(|| app.run(rx_tracer)))
    }
}

pub struct LogicalContext<'env> {
    scenarios: Vec<logical::Scenario<'env>>,
}

#[derive(Debug, Default)]
pub struct ExecutionRuntimeCtx {
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
}

pub struct Config {}

// async timer that ticks and sends a duration event.
// This timer can be stopped using a oneshot channel.
async fn unbounded_timer(mut stop: oneshot::Receiver<()>) {
    let start_time = Instant::now();
    let timer = || async {
        let duration_since = Instant::now().duration_since(start_time).as_secs();
        event!(target: CRATE_NAME, Level::INFO, duration = duration_since);
        tokio::time::sleep(Duration::from_secs(1)).await
    };
    loop {
        let timer = timer();
        tokio::select! {
            _ = (&mut stop) => {
                break;
            }
            _ = (timer) => {}
        }
    }
}
