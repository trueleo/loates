use std::{
    marker::PhantomData,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use futures::Future;
use tracing::{event, Instrument, Level};

use crate::{
    data::{Extractor, RuntimeDataStore},
    logical,
    user::AsyncUserBuilder,
    User, UserResult, CRATE_NAME, SPAN_TASK, TARGET_USER_EVENT,
};

type ExecutorTask<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait Executor: Send {
    fn execute(&mut self) -> (ExecutorTask<'_>, crate::Receiver<UserResult>);
}

pub(crate) enum DataExecutor<'a, U, Ub, Args> {
    Once(Once<U>),
    Constant(Constant<U>),
    Shared(SharedIterations<U>),
    PerUser(PerUserIteration<U>),
    RampingUser(RampingUser<'a, U, Ub, Args>),
    // ConstantArrivalRate is RampingArrivalRate with 1 stage
    ConstantArrivalRate(RampingArrivalRate<'a, U, Ub, Args>),
    RampingArrivalRate(RampingArrivalRate<'a, U, Ub, Args>),
}

impl<'a, U, Ub, Args> DataExecutor<'a, U, Ub, Args>
where
    U: User + 'a,
    Ub: AsyncUserBuilder<Args, U> + Sync,
    Args: Extractor<'a> + 'a + Send,
{
    pub async fn new(
        datastore: &'a RuntimeDataStore,
        user_builder: &'a Ub,
        executor: logical::Executor,
    ) -> Self {
        match executor {
            logical::Executor::Once => {
                let mut users = build_inital_users(datastore, user_builder, 1).await;
                Self::Once(Once::new(users.pop().unwrap()))
            }
            logical::Executor::Constant { users, duration } => {
                let users = build_inital_users(datastore, user_builder, users).await;
                Self::Constant(Constant::new(users, duration))
            }
            logical::Executor::Shared {
                users,
                iterations,
                duration,
            } => {
                let users = build_inital_users(datastore, user_builder, users).await;
                Self::Shared(SharedIterations::new(users, iterations, duration))
            }
            logical::Executor::PerUser { users, iterations } => {
                let users = build_inital_users(datastore, user_builder, users).await;
                Self::PerUser(PerUserIteration::new(users, iterations))
            }
            logical::Executor::ConstantArrivalRate {
                pre_allocate_users,
                rate,
                time_unit,
                max_users,
                duration,
            } => Self::ConstantArrivalRate(RampingArrivalRate::new(
                datastore,
                user_builder,
                pre_allocate_users,
                vec![((rate, time_unit), duration)],
                max_users,
            )),
            logical::Executor::RampingUser {
                pre_allocate_users,
                stages,
            } => Self::RampingUser(RampingUser::new(
                datastore,
                user_builder,
                stages,
                pre_allocate_users,
            )),
            logical::Executor::RampingArrivalRate {
                pre_allocate_users,
                max_users,
                stages,
            } => Self::RampingArrivalRate(RampingArrivalRate::new(
                datastore,
                user_builder,
                pre_allocate_users,
                stages,
                max_users,
            )),
        }
    }
}

#[async_trait::async_trait]
impl<'a, Args, U, Ub> Executor for DataExecutor<'a, U, Ub, Args>
where
    U: User + 'a,
    Ub: AsyncUserBuilder<Args, U> + Sync,
    Args: Extractor<'a> + 'a + Send,
{
    fn execute(&mut self) -> (ExecutorTask<'_>, crate::Receiver<UserResult>) {
        match self {
            DataExecutor::Once(exec) => exec.execute(),
            DataExecutor::Constant(exec) => exec.execute(),
            DataExecutor::Shared(exec) => exec.execute(),
            DataExecutor::PerUser(exec) => exec.execute(),
            DataExecutor::RampingUser(exec) => exec.execute(),
            DataExecutor::ConstantArrivalRate(exec) => exec.execute(),
            DataExecutor::RampingArrivalRate(exec) => exec.execute(),
        }
    }
}

async fn user_call<'a>(
    task: Pin<Box<dyn Future<Output = Result<(), crate::error::Error>> + Send + 'a>>,
) -> Result<(), crate::error::Error> {
    let res = task.await;
    if let Err(ref err) = res {
        event!(name: "error", target: TARGET_USER_EVENT, Level::INFO, err = %err)
    }
    res
}

pub(crate) struct Once<U> {
    user: U,
}

impl<U> Once<U> {
    fn new(user: U) -> Self {
        Once { user }
    }
}

impl<U> Executor for Once<U>
where
    U: User,
{
    fn execute(&mut self) -> (ExecutorTask<'_>, crate::Receiver<UserResult>) {
        let (tx, rx) = crate::channel();
        let task = self.user.call();
        let exec = async move {
            let spawner = async_scoped::spawner::use_tokio::Tokio;
            let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
            event!(target: CRATE_NAME, Level::INFO, users = 1u64, users_max = 1u64);
            scope.spawn_cancellable(
                async move {
                    let _ = tx.send(user_call(task).await);
                }
                .instrument(tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK)),
                || (),
            );
            let _ = scope.collect().await;
        };
        (Box::pin(exec), rx)
    }
}

pub(crate) struct Constant<U> {
    users: Vec<U>,
    duration: Duration,
}

impl<U> Constant<U> {
    fn new(users: Vec<U>, duration: Duration) -> Self {
        Self { users, duration }
    }
}

impl<U> Executor for Constant<U>
where
    U: User,
{
    fn execute(&mut self) -> (ExecutorTask<'_>, crate::Receiver<UserResult>) {
        let (tx, rx) = crate::channel();

        let users_len = self.users.len();
        let total_duration_as_secs = self.duration.as_secs();
        let total_duration = self.duration;

        let end_time = Instant::now() + total_duration;
        let tasks = self.users.iter_mut().map(move |user| {
            let tx = tx.clone();
            async move {
                while std::time::Instant::now() < end_time {
                    let res = user_call(user.call())
                        .instrument(
                            tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK),
                        )
                        .await;
                    let _ = tx.send(res);
                }
            }
        });

        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, users = users_len, users_max = users_len);
            event!(target: CRATE_NAME, Level::INFO, total_duration = total_duration_as_secs);
            let spawner = async_scoped::spawner::use_tokio::Tokio;
            let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
            for task in tasks {
                scope.spawn_cancellable(task.in_current_span(), || ());
            }
            let _ = scope.collect().await;
        };

        (Box::pin(task), rx)
    }
}

pub(crate) struct SharedIterations<U> {
    users: Vec<U>,
    iterations: usize,
    duration: Duration,
}

impl<U> SharedIterations<U> {
    fn new(users: Vec<U>, iterations: usize, duration: Duration) -> Self {
        Self {
            users,
            iterations,
            duration,
        }
    }
}

impl<U> SharedIterations<U>
where
    U: User,
{
    fn execute(&mut self) -> (ExecutorTask<'_>, crate::Receiver<UserResult>) {
        let (tx, rx) = crate::channel();
        let users_len = self.users.len();
        let iterations = self.iterations;
        let total_duration_as_secs = self.duration.as_secs();

        let end_time = Instant::now() + self.duration;

        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, users = users_len, users_max = users_len);
            event!(target: CRATE_NAME, Level::INFO, total_duration = total_duration_as_secs);
            let iterations_completed = AtomicUsize::new(iterations);
            let tasks = self.users.iter_mut().map(|user| {
                let tx = tx.clone();
                let iterations_completed = &iterations_completed;
                async move {
                    while std::time::Instant::now() < end_time {
                        let current_iteration =
                            iterations_completed.fetch_add(1, Ordering::Relaxed);
                        if current_iteration >= iterations {
                            break;
                        }
                        let _ = tx.send(user.call().instrument(
                            tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK),
                        ).await);
                    }
                }
            });

            let spawner = async_scoped::spawner::use_tokio::Tokio;
            let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
            for task in tasks {
                scope.spawn_cancellable(task.in_current_span(), || ());
            }
            let _ = scope.collect().await;
        };

        (Box::pin(task), rx)
    }
}

pub(crate) struct PerUserIteration<U> {
    users: Vec<U>,
    iterations: usize,
}

impl<U> PerUserIteration<U> {
    fn new(users: Vec<U>, iterations: usize) -> Self {
        Self { users, iterations }
    }
}

impl<U> Executor for PerUserIteration<U>
where
    U: User,
{
    fn execute(&mut self) -> (ExecutorTask<'_>, crate::Receiver<UserResult>) {
        let (tx, rx) = crate::channel();
        let Self { users, iterations } = self;
        let users_len = users.len();
        let iterations = *iterations;
        let tasks = users.iter_mut().map(move |user| {
            let tx = tx.clone();
            async move {
                for _ in 0..iterations {
                    let _ = tx.send(user.call().await);
                }
            }
        });

        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, users = users_len, users_max = users_len);
            let spawner = async_scoped::spawner::use_tokio::Tokio;
            let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
            for task in tasks {
                scope.spawn_cancellable(
                    task.instrument(
                        tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK),
                    ),
                    || (),
                );
            }
            let _ = scope.collect().await;
        };

        (Box::pin(task), rx)
    }
}

pub(crate) struct RampingUser<'a, U, Ub, Args> {
    datastore: &'a RuntimeDataStore,
    user_builder: &'a Ub,
    pre_allocate_users: usize,
    stages: Vec<(Duration, usize)>,
    _u: PhantomData<U>,
    _a: PhantomData<Args>,
}

impl<'a, U, Ub, Args> RampingUser<'a, U, Ub, Args> {
    fn new(
        datastore: &'a RuntimeDataStore,
        user_builder: &'a Ub,
        stages: Vec<(Duration, usize)>,
        initial_users: usize,
    ) -> Self {
        Self {
            datastore,
            user_builder,
            pre_allocate_users: initial_users,
            stages,
            _u: PhantomData,
            _a: PhantomData,
        }
    }
}

impl<'a, U, Ub, Args> Executor for RampingUser<'a, U, Ub, Args>
where
    U: User,
    Ub: AsyncUserBuilder<Args, U>,
    Args: Extractor<'a> + Send,
{
    fn execute(&mut self) -> (ExecutorTask<'_>, crate::Receiver<UserResult>) {
        let (tx, rx) = crate::channel();
        let datastore = self.datastore;
        let user_builder = self.user_builder;
        let pre_allocated_users = self.pre_allocate_users;
        let stages = &*self.stages;

        let task = async move {
            let mut users = Vec::new();

            for _ in 0..pre_allocated_users {
                let user = user_builder
                    .build(Args::from_runtime(datastore).unwrap())
                    .await
                    .unwrap();
                users.push(user);
            }

            for (duration, target_users) in stages {
                let len = users.len();
                if len < *target_users {
                    for _ in 0..(target_users - len) {
                        let user = user_builder
                            .build(Args::from_runtime(datastore).unwrap())
                            .await
                            .unwrap();
                        users.push(user);
                    }
                }
                let end_time = Instant::now() + *duration;
                let tasks = users.iter_mut().map(|user| {
                    let tx = tx.clone();
                    async move {
                        while Instant::now() < end_time {
                            let _ = tx.send(user.call().await);
                        }
                    }
                });
                let spawner = async_scoped::spawner::use_tokio::Tokio;
                let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
                tasks.into_iter().for_each(|task| {
                    let span = tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK);
                    scope.spawn_cancellable(task.instrument(span), || ());
                });
                let _ = scope.collect().await;
            }
        };

        (Box::pin(task), rx)
    }
}

pub(crate) struct RampingArrivalRate<'a, U, Ub, Args> {
    datastore: &'a RuntimeDataStore,
    user_builder: &'a Ub,
    pre_allocate_users: usize,
    stages: Vec<((usize, Duration), Duration)>,
    max_users: usize,
    _u: PhantomData<U>,
    _a: PhantomData<Args>,
}

impl<'a, U, Ub, Args> RampingArrivalRate<'a, U, Ub, Args> {
    fn new(
        datastore: &'a RuntimeDataStore,
        user_builder: &'a Ub,
        pre_allocate_users: usize,
        stages: Vec<((usize, Duration), Duration)>,
        max_users: usize,
    ) -> Self {
        Self {
            datastore,
            user_builder,
            pre_allocate_users,
            stages,
            max_users,
            _u: PhantomData,
            _a: PhantomData,
        }
    }
}

impl<'a, U, Ub, Args> Executor for RampingArrivalRate<'a, U, Ub, Args>
where
    U: User,
    Ub: AsyncUserBuilder<Args, U>,
    Args: Extractor<'a> + Send,
{
    fn execute(&mut self) -> (ExecutorTask<'_>, crate::Receiver<UserResult>) {
        let (tx, rx) = crate::channel();

        let datastore = self.datastore;
        let user_builder = self.user_builder;
        let pre_allocated_users = self.pre_allocate_users;
        let max_users = self.max_users;
        let stages = &*self.stages;

        let task = async move {
            let mut users = Vec::new();

            for _ in 0..pre_allocated_users {
                let user = user_builder
                    .build(Args::from_runtime(datastore).unwrap())
                    .await
                    .unwrap();
                users.push(tokio::sync::Mutex::new(user));
            }

            for ((rate, time_unit), duration) in stages {
                let end_time = Instant::now() + *duration;

                while Instant::now() < end_time {
                    let next_rate_check_time = Instant::now() + *time_unit;
                    let mut current_rate = 0;

                    let spawner = async_scoped::spawner::use_tokio::Tokio;
                    let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };

                    let mut user_iter = users.iter().cycle().filter_map(|x| x.try_lock().ok());

                    let now = Instant::now();
                    while now < next_rate_check_time && now < end_time && current_rate < *rate {
                        let mut user = user_iter.next().unwrap();
                        let tx = tx.clone();
                        let task = async move {
                            let _ = tx.send(user.call().await);
                        };
                        let span =
                            tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK);
                        scope.spawn_cancellable(task.instrument(span), || ());
                        current_rate += 1;
                    }

                    scope.collect().await;
                    drop(scope);

                    if current_rate < *rate && users.len() < max_users {
                        for _ in 0..(rate - current_rate) {
                            let user = user_builder
                                .build(Args::from_runtime(datastore).unwrap())
                                .await
                                .unwrap();
                            users.push(tokio::sync::Mutex::new(user))
                        }
                    }

                    if Instant::now() <= end_time || current_rate < *rate {
                        // Sleep until to make sure we wait before next set of task;
                        tokio::time::sleep_until(next_rate_check_time.into()).await;
                    }
                }
            }
        };

        (Box::pin(task), rx)
    }
}

async fn build_inital_users<'a, U, Ub, Args>(
    runtime: &'a RuntimeDataStore,
    user_builder: &'a Ub,
    count: usize,
) -> Vec<U>
where
    Ub: AsyncUserBuilder<Args, U>,
    Args: Extractor<'a> + Send,
{
    let mut res = vec![];
    for _ in 0..count {
        let user = user_builder
            .build(Args::from_runtime(runtime).unwrap())
            .await
            .unwrap();
        res.push(user)
    }
    res
}
