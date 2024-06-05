use std::{borrow::Cow, marker::PhantomData, time::Duration};

use crate::{
    data::{DatastoreModifier, Extractor},
    executor::DataExecutor,
    runner::ExecutionRuntimeCtx,
    user::AsyncUserBuilder,
    User,
};

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
        rate: usize,
        time_unit: Duration,
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
        stages: Vec<((usize, Duration), Duration)>,
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
}

pub struct ExecutionPlan<'env, U, Ub, Args> {
    label: Option<Cow<'static, str>>,
    user_builder: &'env Ub,
    datastore_modifiers: Vec<Box<dyn DatastoreModifier>>,
    executor: Executor,
    _t: PhantomData<fn(Args)>,
    _u: PhantomData<fn(U)>,
}

impl<'env, U, Ub, Args> ExecutionPlan<'env, U, Ub, Args> {
    pub fn new(
        label: Option<Cow<'static, str>>,
        user_builder: &'env Ub,
        datastore_modifiers: Vec<Box<dyn DatastoreModifier>>,
        executor: Executor,
    ) -> Self {
        Self {
            label,
            user_builder,
            datastore_modifiers,
            executor,
            _t: PhantomData,
            _u: PhantomData,
        }
    }
}

impl ExecutionPlan<'static, (), (), ()> {
    pub fn builder() -> ExecutionPlan<'static, (), (), ()> {
        Self {
            label: None,
            user_builder: &(),
            datastore_modifiers: Vec::new(),
            executor: Executor::Once,
            _t: PhantomData,
            _u: PhantomData,
        }
    }

    pub fn with_label(&mut self, label: impl Into<Cow<'static, str>>) {
        self.label = Some(label.into());
    }

    pub fn with_user_builder<'env, U, Args, Ub: AsyncUserBuilder<Args, U> + 'env>(
        self,
        user_builder: &'env Ub,
    ) -> ExecutionPlan<'env, U, Ub, Args> {
        ExecutionPlan::<'env, U, Ub, Args>::new(
            self.label,
            user_builder,
            self.datastore_modifiers,
            self.executor,
        )
    }
}

impl<U, Ub, Args> ExecutionPlan<'_, U, Ub, Args> {
    pub fn with_data<T: DatastoreModifier + 'static>(mut self, f: T) -> Self {
        self.datastore_modifiers
            .push(Box::new(f) as Box<dyn DatastoreModifier>);
        self
    }

    pub fn with_executor(mut self, executor: Executor) -> Self {
        self.executor = executor;
        self
    }
}

#[async_trait::async_trait]
impl<'env, Args, Ub, U> ExecutionProvider for ExecutionPlan<'env, U, Ub, Args>
where
    Ub: AsyncUserBuilder<Args, U> + Sync,
    U: User,
    Args: for<'a> Extractor<'a> + Send + Sync,
{
    fn label(&self) -> Cow<'_, str> {
        if let Some(ref label) = self.label {
            return Cow::Borrowed(label.as_ref());
        }

        match &self.executor {
            Executor::Once => Cow::Borrowed("Once"),
            Executor::Constant { users, duration } => {
                format!("Constant {} users {:?}", users, duration).into()
            }
            Executor::Shared {
                users, iterations, ..
            } => format!("Shared {} users {}", users, iterations).into(),
            Executor::PerUser { users, iterations } => {
                format!("PerUser {} users {}", users, iterations).into()
            }
            Executor::ConstantArrivalRate {
                rate,
                time_unit,
                duration,
                ..
            } => format!(
                "ConstantArrivalRate {}/{:?} for {:?}",
                rate, time_unit, duration
            )
            .into(),
            Executor::RampingUser { stages, .. } => {
                format!("RampingUser stages {}", stages.len()).into()
            }
            Executor::RampingArrivalRate { stages, .. } => {
                format!("RampingArrivalRate stages {}", stages.len()).into()
            }
        }
    }

    async fn execution<'a>(
        &'a self,
        ctx: &'a mut ExecutionRuntimeCtx,
    ) -> Box<dyn crate::executor::Executor + 'a> {
        let data = ctx.datastore_mut();
        self.datastore_modifiers
            .iter()
            .for_each(|modifier| modifier.init_store(data));
        let user_builder = self.user_builder;
        let executor = self.executor.clone();
        Box::new(DataExecutor::<'a, U, Ub, Args>::new(data, user_builder, executor).await)
            as Box<dyn crate::executor::Executor + '_>
    }
}
