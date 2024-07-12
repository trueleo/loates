use std::{borrow::Cow, fmt::Write, time::Duration};

use crate::{
    data::DatastoreModifier, executor::DataExecutor, runner::ExecutionRuntimeCtx,
    user::AsyncUserBuilder,
};

/// Rate of iteration.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Rate(
    /// Number of iterations
    pub usize,
    /// Time interval in which to perform those iterations
    pub Duration,
);

impl From<Rate> for (usize, Duration) {
    fn from(value: Rate) -> Self {
        (value.0, value.1)
    }
}

impl std::fmt::Display for Rate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0))?;
        f.write_char('/')?;
        f.write_fmt(format_args!("{:?}", self.1))?;
        Ok(())
    }
}

/// Executor type that is to be used within an execution.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all_fields = "camelCase"))]
#[cfg_attr(feature = "serde", serde(tag = "type"))]
pub enum Executor {
    /// Excecute the user call only once then exit.
    Once,
    /// Contantly drive all the the users for a certain duration doing as many iterations as possible.
    Constant {
        /// Number of users
        users: usize,
        /// Duration of execution
        duration: Duration,
    },
    /// Share N iterations among K users.
    /// This executor does not divide and allocate iterations to users beforehand,
    /// so if a user is able to go through iterations faster during runtime, it will end up doing more iterations that others.
    Shared {
        /// Number of users
        users: usize,
        /// Number of iterations
        iterations: usize,
        /// Duration of execution
        duration: Duration,
    },
    /// Have each user run certain number of iterations.
    /// Test finishes when all users have finished their execution.
    PerUser {
        /// Number of users
        users: usize,
        /// Number of iterations each user will perform.
        iterations: usize,
    },
    /// Executor for performing iterations at a given rate.
    /// Time taken for completion of an iteration is variable, thus this
    /// executor cannot guarantee perfect throughput. If executor does not
    /// meet the rate deifined in its config then it will try to compensate
    /// and match the given rate by allocating more users duing runtime.
    ConstantArrivalRate {
        /// Number of users to pre-allocate
        pre_allocate_users: usize,
        /// Rate of iteration
        rate: Rate,
        /// Maximum number of users that could be spawned by this executor
        max_users: usize,
        /// Duration of execution
        duration: Duration,
    },
    /// Executor with stages, where in each stage executor allocates certain number of users for a specific duration and have them run as many iterations as possible.
    /// Use this executor when you want to ramp the number of users up or down during specific periods of time.
    RampingUser {
        /// Number of users to pre-allocate
        pre_allocate_users: usize,
        /// stages of this execution. Sequence of number of user and duration
        stages: Vec<(usize, Duration)>,
    },
    /// Executor with stages, where in each stage executor is given an arrival rate for a certain duration.
    /// Similar to ConstantArrivalRate executor, it tries to match the given rate of iteration.
    /// If iteration rate falls short, it compenstates for lack of iterations by spawning more users during runtime.
    /// Use this executor when you want to change the iteration rates during specific periods of time.
    RampingArrivalRate {
        /// Number of users to pre-allocate
        pre_allocate_users: usize,
        /// Maximum number of users that could be spawned by this executor
        max_users: usize,
        /// stages of this execution. Sequence of Rate and duration
        stages: Vec<(Rate, Duration)>,
    },
}

impl std::fmt::Display for Executor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Executor::Once => f.write_str("Once"),
            Executor::Constant { users, duration } => {
                write!(f, "Constant ({} users) {:?}", users, duration)
            }
            Executor::Shared {
                users, iterations, ..
            } => write!(f, "Shared ({} users) {}", users, iterations),
            Executor::PerUser { users, iterations } => {
                write!(f, "PerUser ({} users) {}", users, iterations)
            }
            Executor::ConstantArrivalRate { rate, duration, .. } => {
                write!(f, "ConstantArrivalRate {} for {:?}", rate, duration)
            }
            Executor::RampingUser { stages, .. } => {
                write!(f, "RampingUser ({} stages)", stages.len())
            }
            Executor::RampingArrivalRate { stages, .. } => {
                write!(f, "RampingArrivalRate ({}, stages)", stages.len())
            }
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait ExecutionProvider {
    fn start_after(&self) -> Duration;
    fn config(&self) -> &Executor;
    async fn execution<'a>(
        &'a self,
        ctx: &'a mut ExecutionRuntimeCtx,
    ) -> Box<dyn crate::executor::Executor + 'a>;
}

/// Named collection of executions which should run in parallel to each other.    
///
/// A scenario is conceptually a test model which simulates a traffic pattern / load.
/// For more detailed guide on how to organize a scenario and use multiple Execution in a test. Look at [examples](https://github.com/trueleo/loates/examples).  
pub struct Scenario<'env> {
    pub(crate) label: Cow<'static, str>,
    pub(crate) execution_provider: Vec<Box<dyn ExecutionProvider + 'env>>,
}

impl<'env> Scenario<'env> {
    /// Create a new scenario with a label and a single execution. More execution can be added using [with_executor](Self::with_executor) method
    pub fn new<Ub>(label: impl Into<Cow<'static, str>>, execution: Execution<'env, Ub>) -> Self
    where
        Ub: for<'a> AsyncUserBuilder<'a> + 'env,
    {
        Self {
            label: label.into(),
            execution_provider: vec![Box::new(execution)],
        }
    }

    /// Append a new executor to this scenario.
    pub fn with_executor<Ub>(mut self, execution: Execution<'env, Ub>) -> Self
    where
        Ub: for<'a> AsyncUserBuilder<'a> + 'env,
    {
        self.execution_provider.push(Box::new(execution));
        self
    }
}

/// Logical execution plan that outlines which user type to spawn during runtime and under which [`Executor`].
///
/// A [`Scenario`] can contain one or more of these *execution plans*.
pub struct Execution<'env, Ub> {
    start_after: Duration,
    user_builder: Ub,
    datastore_modifiers: Vec<Box<dyn DatastoreModifier + 'env>>,
    executor: Executor,
}

impl<'env, Ub> Execution<'env, Ub> {
    /// Create a new Execution with a [`user builder`](AsyncUserBuilder) and an [`Executor`]
    pub fn new(user_builder: Ub, executor: Executor) -> Self {
        Self {
            start_after: Duration::ZERO,
            user_builder,
            datastore_modifiers: vec![],
            executor,
        }
    }
}

impl Execution<'static, ()> {
    /// Create a new Execution plan using builder pattern
    pub fn builder() -> Execution<'static, ()> {
        Self {
            start_after: Duration::ZERO,
            user_builder: (),
            datastore_modifiers: Vec::new(),
            executor: Executor::Once,
        }
    }

    /// Register user builder that will be used in this execution.
    pub fn with_user_builder<'env, F>(self, user_builder: F) -> Execution<'env, F>
    where
        F: for<'a> AsyncUserBuilder<'a> + 'env,
    {
        Execution::<'env, _> {
            user_builder,
            executor: self.executor,
            datastore_modifiers: self.datastore_modifiers,
            start_after: self.start_after,
        }
    }
}

impl<'env, Ub> Execution<'env, Ub>
where
    Ub: for<'a> AsyncUserBuilder<'a> + 'env,
{
    /// Append a new datastore initializer to this execution. When perparing to run a scenario, this will be used to initialize [`RuntimeDataStore`](crate::data::RuntimeDataStore) created for this execution.
    pub fn with_data<T: DatastoreModifier + 'env>(mut self, f: T) -> Self {
        self.datastore_modifiers
            .push(Box::new(f) as Box<dyn DatastoreModifier + 'env>);
        self
    }

    /// [`Executor`] type which should be used for this execution.
    pub fn with_executor(mut self, executor: Executor) -> Self {
        self.executor = executor;
        self
    }

    /// Start this execution some duration after start of the scenario
    pub fn start_after(mut self, duration: Duration) -> Self {
        self.start_after = duration;
        self
    }

    /// Convert this Execution to a Scenario with provided label.
    pub fn to_scenario(self, label: impl Into<Cow<'static, str>>) -> Scenario<'env> {
        Scenario::new(label, self)
    }
}

#[async_trait::async_trait]
impl<'env, Ub> ExecutionProvider for Execution<'env, Ub>
where
    Ub: for<'a> AsyncUserBuilder<'a>,
{
    fn start_after(&self) -> Duration {
        self.start_after
    }

    fn config(&self) -> &Executor {
        &self.executor
    }

    async fn execution<'a>(
        &'a self,
        ctx: &'a mut ExecutionRuntimeCtx,
    ) -> Box<dyn crate::executor::Executor + 'a> {
        for modifiers in self.datastore_modifiers.iter() {
            ctx.modify(&**modifiers).await;
        }
        let user_builder = &self.user_builder;
        let executor = self.executor.clone();
        Box::new(
            DataExecutor::<Ub>::new(ctx.datastore_mut(), user_builder, executor)
                .await
                .unwrap(),
        ) as Box<dyn crate::executor::Executor + '_>
    }
}
