use std::{borrow::Cow, fmt::Write, time::Duration};

use crate::{
    data::DatastoreModifier,
    executor::DataExecutor,
    runner::ExecutionRuntimeCtx,
    user::{AsyncFnBuilder, AsyncUserBuilder},
};

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Rate(pub usize, pub Duration);

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

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all_fields = "camelCase"))]
#[cfg_attr(feature = "serde", serde(tag = "type"))]
pub enum Executor {
    Once,
    Constant {
        users: usize,
        duration: Duration,
    },
    Shared {
        users: usize,
        iterations: usize,
        duration: Duration,
    },
    PerUser {
        users: usize,
        iterations: usize,
    },
    ConstantArrivalRate {
        pre_allocate_users: usize,
        rate: Rate,
        max_users: usize,
        duration: Duration,
    },
    RampingUser {
        pre_allocate_users: usize,
        stages: Vec<(usize, Duration)>,
    },
    RampingArrivalRate {
        pre_allocate_users: usize,
        max_users: usize,
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
pub trait ExecutionProvider {
    fn config(&self) -> &Executor;
    async fn execution<'a>(
        &'a self,
        ctx: &'a mut ExecutionRuntimeCtx,
    ) -> Box<dyn crate::executor::Executor + 'a>;
}

pub struct Scenario<'env> {
    pub(crate) label: Cow<'static, str>,
    pub(crate) execution_provider: Vec<Box<dyn ExecutionProvider + 'env>>,
}

impl<'env> Scenario<'env> {
    pub fn new<T: ExecutionProvider + 'env>(
        label: impl Into<Cow<'static, str>>,
        execution: T,
    ) -> Self {
        Self {
            label: label.into(),
            execution_provider: vec![Box::new(execution)],
        }
    }

    pub fn with_executor<T: ExecutionProvider + 'env>(mut self, execution: T) -> Self {
        self.execution_provider.push(Box::new(execution));
        self
    }
}

pub struct ExecutionPlan<'env, Ub> {
    label: Option<Cow<'static, str>>,
    user_builder: Ub,
    datastore_modifiers: Vec<Box<dyn DatastoreModifier + 'env>>,
    executor: Executor,
}

impl<'env, Ub> ExecutionPlan<'env, Ub> {
    fn new(
        label: Option<Cow<'static, str>>,
        user_builder: Ub,
        datastore_modifiers: Vec<Box<dyn DatastoreModifier>>,
        executor: Executor,
    ) -> Self {
        Self {
            label,
            user_builder,
            datastore_modifiers,
            executor,
        }
    }
}

impl ExecutionPlan<'static, ()> {
    pub fn builder() -> ExecutionPlan<'static, ()> {
        Self {
            label: None,
            user_builder: (),
            datastore_modifiers: Vec::new(),
            executor: Executor::Once,
        }
    }

    pub fn with_user_builder<'env, F>(
        self,
        user_builder: F,
    ) -> ExecutionPlan<'env, AsyncFnBuilder<F>>
    where
        F: Into<AsyncFnBuilder<F>> + Sync + 'env,
        AsyncFnBuilder<F>: for<'a> AsyncUserBuilder<'a>,
    {
        let user_builder: AsyncFnBuilder<F> = user_builder.into();
        ExecutionPlan::<'env, _>::new(
            self.label,
            user_builder,
            self.datastore_modifiers,
            self.executor,
        )
    }
}

impl<'env, Ub> ExecutionPlan<'env, Ub> {
    pub fn with_data<T: DatastoreModifier + 'env>(mut self, f: T) -> Self {
        self.datastore_modifiers
            .push(Box::new(f) as Box<dyn DatastoreModifier + 'env>);
        self
    }

    pub fn with_executor(mut self, executor: Executor) -> Self {
        self.executor = executor;
        self
    }
}

#[async_trait::async_trait]
impl<'env, Ub> ExecutionProvider for ExecutionPlan<'env, Ub>
where
    Ub: for<'a> AsyncUserBuilder<'a>,
{
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
