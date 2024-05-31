use crate::data::DatastoreModifier;
use crate::{data::RuntimeDataStore, UserResult};

use crate::{error::Error, logical};

use async_scoped::{self, Scope};
use futures::StreamExt as _;
use tracing::Instrument;

/// The Runner struct is the top level struct for managing and executing series of logical scenarios asynchronously.
pub struct Runner<'a> {
    logical: LogicalContext<'a>,
    tx: crate::Sender<UserResult>,
}

impl<'a> Runner<'a> {
    // Create new instance of Runner with a [Config](crate::config::Config) and list of [Scenario](create::logical::Scenario)
    pub fn new(
        config: Config,
        scenarios: Vec<logical::Scenario<'a>>,
    ) -> (Runner, crate::Receiver<UserResult>) {
        let (tx, rx) = crate::channel();
        (
            Self {
                logical: LogicalContext { config, scenarios },
                tx,
            },
            rx,
        )
    }

    // Spawn the runner
    pub async fn run(&self) -> Result<(), Error> {
        let mut runtime_ctx: Vec<Vec<ExecutionRuntimeCtx>> = self
            .logical
            .scenarios
            .iter()
            .map(|scenario| {
                scenario
                    .execution_provider
                    .iter()
                    .map(|_| ExecutionRuntimeCtx::new())
                    .collect()
            })
            .collect();

        let runtime_ctx_mut = runtime_ctx.iter_mut().map(|x| x.iter_mut());

        let mut runtime_scenarios = Vec::new();
        for (scenario, context) in self.logical.scenarios.iter().zip(runtime_ctx_mut) {
            let mut runtime_scenario = Vec::new();
            for (exec, context) in scenario.execution_provider.iter().zip(context) {
                runtime_scenario.push((exec.name(), exec.execution(context).await))
            }
            runtime_scenarios.push((scenario.name.clone(), runtime_scenario))
        }

        for (scenario_name, scenario) in runtime_scenarios.iter_mut() {
            let span = tracing::span!(target: "rusher", tracing::Level::INFO, "scenario", name = scenario_name);
            let _entered = span.enter();
            let mut scope =
                unsafe { async_scoped::Scope::create(async_scoped::spawner::use_tokio::Tokio) };
            for (exec_name, exec) in scenario.iter_mut() {
                let span = tracing::span!(target: "rusher", parent: &span, tracing::Level::INFO, "exec", name = exec_name);
                let (task, mut res) = exec.execute();
                let tx = self.tx.clone();
                scope.spawn_cancellable(task.instrument(span), || ());
                scope.spawn(async move {
                    while let Some(value) = res.next().await {
                        let _ = tx.unbounded_send(value);
                    }
                })
            }
            Scope::collect(&mut scope).await;
        }

        Ok(())
    }
}

pub struct LogicalContext<'a> {
    config: Config,
    scenarios: Vec<logical::Scenario<'a>>,
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

    pub fn modify(&mut self, f: Box<dyn DatastoreModifier>) {
        f.init_store(&mut self.datastore);
    }

    pub fn datastore_mut(&mut self) -> &mut RuntimeDataStore {
        &mut self.datastore
    }
}

pub struct Config {}
