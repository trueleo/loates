use std::borrow::Cow;

use crate::data::DatastoreModifier;
use crate::data::RuntimeDataStore;
use crate::executor::Executor;
use crate::{UserResult, CRATE_NAME, SPAN_EXEC, SPAN_SCENARIO};

use crate::logical;

use async_scoped::{self, Scope};
use tracing::{event, Instrument, Span};

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
        let mut scenarios = self.runtime_scenario(&mut runtime_ctx).await;

        for (scenario_index, (scenario_name, scenario)) in scenarios.iter_mut().enumerate() {
            let span = tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_SCENARIO, name = scenario_name.as_ref(), id = scenario_index as u64);
            let _entered = span.enter();

            let mut scope =
                unsafe { async_scoped::Scope::create(async_scoped::spawner::use_tokio::Tokio) };

            // gather user_results from every executor.
            let (user_result_tx, user_result_rx) = crate::channel();

            for (executor_index, (executor_name, executor)) in scenario.iter_mut().enumerate() {
                let span = tracing::span!(target: CRATE_NAME, parent: &span, tracing::Level::INFO, SPAN_EXEC, name = executor_name.as_ref(), id = executor_index as u64);
                scoped_executor_spawn(span, executor, &mut scope, user_result_tx.clone());
            }

            drop(user_result_tx);
            if has_user_terminated(user_result_rx).await {
                scope.cancel();
                break;
            } else {
                Scope::collect(&mut scope).await;
            }
        }

        event!(name: "runner_exit", target: CRATE_NAME, tracing::Level::INFO, "Exit test");

        #[cfg(feature = "tui")]
        if let Some(handle) = tui_handle {
            let _ = handle.join();
        }

        Ok(())
    }

    async fn runtime_scenario<'a>(
        &'a self,
        runtime_ctx: &'a mut [Vec<ExecutionRuntimeCtx>],
    ) -> Vec<(Cow<str>, Vec<(Cow<str>, Box<dyn Executor + '_>)>)> {
        let mut scenarios = Vec::new();
        let runtime_ctx_mut = runtime_ctx.iter_mut().map(|x| x.iter_mut());
        for (logical_scenario, context) in self.logical.scenarios.iter().zip(runtime_ctx_mut) {
            let mut scenario = Vec::new();
            for (exec, context) in logical_scenario.execution_provider.iter().zip(context) {
                scenario.push((exec.label(), exec.execution(context).await))
            }
            scenarios.push((logical_scenario.label.clone(), scenario))
        }
        scenarios
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

        let (tracer, rx_tracer) = crate::tracing::TracerLayer::new();
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

async fn has_user_terminated<'s>(
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

fn scoped_executor_spawn<'s, 'a: 's>(
    span: Span,
    exec: &'s mut Box<dyn Executor + 'a>,
    scope: &mut Scope<'s, (), async_scoped::spawner::use_tokio::Tokio>,
    tx: crate::Sender<UserResult>,
) {
    // close the ubounded_timer
    let task = exec.execute(tx);
    scope.spawn_cancellable(task.instrument(span.clone()), || ());
}
