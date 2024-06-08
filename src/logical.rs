use std::{borrow::Cow, fmt::Write, time::Duration};

use crate::{
    data::DatastoreModifier,
    executor::DataExecutor,
    runner::ExecutionRuntimeCtx,
    user::{AsyncFnBuilder, AsyncUserBuilder},
};

#[derive(Debug, Clone, Copy)]
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
        stages: Vec<(Duration, usize)>,
    },
    RampingArrivalRate {
        pre_allocate_users: usize,
        max_users: usize,
        stages: Vec<(Rate, Duration)>,
    },
}

#[async_trait::async_trait]
pub trait ExecutionProvider {
    fn label(&self) -> Cow<'_, str>;
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

    pub fn with_label(&mut self, label: impl Into<Cow<'static, str>>) {
        self.label = Some(label.into());
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
    fn label(&self) -> Cow<'_, str> {
        if let Some(ref label) = self.label {
            return Cow::Borrowed(label.as_ref());
        }

        match &self.executor {
            Executor::Once => Cow::Borrowed("Once"),
            Executor::Constant { users, duration } => {
                format!("Constant ({} users) {:?}", users, duration).into()
            }
            Executor::Shared {
                users, iterations, ..
            } => format!("Shared ({} users) {}", users, iterations).into(),
            Executor::PerUser { users, iterations } => {
                format!("PerUser ({} users) {}", users, iterations).into()
            }
            Executor::ConstantArrivalRate { rate, duration, .. } => {
                format!("ConstantArrivalRate {} for {:?}", rate, duration).into()
            }
            Executor::RampingUser { stages, .. } => {
                format!("RampingUser ({} stages)", stages.len()).into()
            }
            Executor::RampingArrivalRate { stages, .. } => {
                format!("RampingArrivalRate ({}, stages)", stages.len()).into()
            }
        }
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
