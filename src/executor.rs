use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use futures::Future;
use tokio::sync::Mutex;
use tracing::{event, Instrument, Level};

use crate::{
    data::RuntimeDataStore,
    error::Error,
    logical::{self, Rate},
    user::AsyncUserBuilder,
    User, UserResult, CRATE_NAME, SPAN_TASK,
};

type ExecutorTask<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait Executor: Send {
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_>;
}

pub(crate) enum DataExecutor<'ctx, Ub: for<'a> AsyncUserBuilder<'a>> {
    Once(Once<<Ub as AsyncUserBuilder<'ctx>>::Output>),
    Constant(Constant<<Ub as AsyncUserBuilder<'ctx>>::Output>),
    Shared(SharedIterations<<Ub as AsyncUserBuilder<'ctx>>::Output>),
    PerUser(PerUserIteration<<Ub as AsyncUserBuilder<'ctx>>::Output>),
    RampingUser(RampingUser<'ctx, Ub>),
    // ConstantArrivalRate is RampingArrivalRate with 1 stage
    ConstantArrivalRate(RampingArrivalRate<'ctx, Ub>),
    RampingArrivalRate(RampingArrivalRate<'ctx, Ub>),
}

impl<'ctx, Ub: for<'a> AsyncUserBuilder<'a>> DataExecutor<'ctx, Ub> {
    pub async fn new(
        datastore: &'ctx RuntimeDataStore,
        user_builder: &'ctx Ub,
        executor: logical::Executor,
    ) -> Result<Self, Error> {
        let s = match executor {
            logical::Executor::Once => {
                let mut users = build_users(datastore, user_builder, 1).await?;
                Self::Once(Once::new(users.pop().unwrap()))
            }
            logical::Executor::Constant { users, duration } => {
                let users = build_users(datastore, user_builder, users).await?;
                Self::Constant(Constant::new(users, duration))
            }
            logical::Executor::Shared {
                users,
                iterations,
                duration,
            } => {
                let users = build_users(datastore, user_builder, users).await?;
                Self::Shared(SharedIterations::new(users, iterations, duration))
            }
            logical::Executor::PerUser { users, iterations } => {
                let users = build_users(datastore, user_builder, users).await?;
                Self::PerUser(PerUserIteration::new(users, iterations))
            }
            logical::Executor::ConstantArrivalRate {
                pre_allocate_users,
                rate,
                max_users,
                duration,
            } => Self::ConstantArrivalRate(RampingArrivalRate::new(
                datastore,
                user_builder,
                pre_allocate_users,
                vec![(rate, duration)],
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
        };

        Ok(s)
    }
}

#[async_trait::async_trait]
impl<'ctx, Ub> Executor for DataExecutor<'ctx, Ub>
where
    Ub: for<'a> AsyncUserBuilder<'a>,
{
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        match self {
            DataExecutor::Once(exec) => exec.execute(tx),
            DataExecutor::Constant(exec) => exec.execute(tx),
            DataExecutor::Shared(exec) => exec.execute(tx),
            DataExecutor::PerUser(exec) => exec.execute(tx),
            DataExecutor::RampingUser(exec) => exec.execute(tx),
            DataExecutor::ConstantArrivalRate(exec) => exec.execute(tx),
            DataExecutor::RampingArrivalRate(exec) => exec.execute(tx),
        }
    }
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
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
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
        Box::pin(exec)
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

impl<U: User> Executor for Constant<U> {
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
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

        Box::pin(task)
    }
}

pub(crate) struct SharedIterations<U> {
    users: Vec<U>,
    iterations: usize,
    duration: Duration,
}

impl<U: User> SharedIterations<U> {
    fn new(users: Vec<U>, iterations: usize, duration: Duration) -> Self {
        Self {
            users,
            iterations,
            duration,
        }
    }
}

impl<U: User> SharedIterations<U> {
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        let users_len = self.users.len();
        let iterations = self.iterations;
        let total_duration_as_secs = self.duration.as_secs();

        let end_time = Instant::now() + self.duration;
        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, users = users_len, users_max = users_len);
            event!(target: CRATE_NAME, Level::INFO, total_duration = total_duration_as_secs);
            let iterations_completed = AtomicUsize::new(0);
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
                        let _ = tx.send(user_call(user.call()).instrument(
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

        Box::pin(task)
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

impl<U: User> Executor for PerUserIteration<U> {
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        let Self { users, iterations } = self;
        let users_len = users.len();
        let iterations = *iterations;
        let tasks = users.iter_mut().map(move |user| {
            let tx = tx.clone();
            async move {
                for _ in 0..iterations {
                    let _ = tx.send(
                        user_call(user.call())
                            .instrument(
                                tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK),
                            )
                            .await,
                    );
                }
            }
        });

        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, users = users_len, users_max = users_len);
            event!(target: CRATE_NAME, Level::INFO, total_iteration = iterations);
            let spawner = async_scoped::spawner::use_tokio::Tokio;
            let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
            for task in tasks {
                scope.spawn_cancellable(task.in_current_span(), || ());
            }
            let _ = scope.collect().await;
        };

        Box::pin(task)
    }
}

pub(crate) struct RampingUser<'ctx, Ub> {
    datastore: &'ctx RuntimeDataStore,
    user_builder: &'ctx Ub,
    pre_allocate_users: usize,
    stages: Vec<(Duration, usize)>,
}

impl<'ctx, Ub> RampingUser<'ctx, Ub> {
    fn new(
        datastore: &'ctx RuntimeDataStore,
        user_builder: &'ctx Ub,
        stages: Vec<(Duration, usize)>,
        initial_users: usize,
    ) -> Self {
        Self {
            datastore,
            user_builder,
            pre_allocate_users: initial_users,
            stages,
        }
    }
}

impl<'ctx, Ub> Executor for RampingUser<'ctx, Ub>
where
    Ub: for<'a> AsyncUserBuilder<'a>,
{
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        let datastore = self.datastore;
        let user_builder = self.user_builder;
        let pre_allocated_users = self.pre_allocate_users;
        let stages = &*self.stages;
        let total_duration: u64 = stages.iter().map(|(duration, _)| duration.as_secs()).sum();

        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, total_duration = total_duration);
            let mut users = build_users(datastore, user_builder, pre_allocated_users)
                .await
                .unwrap();
            event!(target: CRATE_NAME, Level::INFO, users = users.len(), users_max = pre_allocated_users);

            for (index, (duration, target_users)) in stages.iter().enumerate() {
                event!(target: CRATE_NAME, Level::INFO, stage = index + 1, stages = stages.len(), stage_duration = duration.as_secs());
                event!(target: CRATE_NAME, Level::INFO, users = users.len(), users_max = target_users.max(&pre_allocated_users));

                let len = users.len();
                if len < *target_users {
                    users.extend(
                        build_users(datastore, user_builder, target_users - len)
                            .await
                            .unwrap(),
                    );
                }
                event!(target: CRATE_NAME, Level::INFO, users = users.len(), users_max = target_users.max(&pre_allocated_users));

                let end_time = Instant::now() + *duration;
                let tasks = users.iter_mut().map(|user| {
                    let tx = tx.clone();
                    async move {
                        while Instant::now() < end_time {
                            let _ = tx.send(user_call(user.call()).instrument(tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK)).await);
                        }
                    }
                });
                let spawner = async_scoped::spawner::use_tokio::Tokio;
                let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
                tasks.into_iter().for_each(|task| {
                    scope.spawn_cancellable(task.in_current_span(), || ());
                });
                let _ = scope.collect().await;
            }
        };

        Box::pin(task)
    }
}

pub(crate) struct RampingArrivalRate<'ctx, Ub> {
    datastore: &'ctx RuntimeDataStore,
    user_builder: &'ctx Ub,
    pre_allocate_users: usize,
    stages: Vec<(Rate, Duration)>,
    max_users: usize,
}

impl<'ctx, Ub> RampingArrivalRate<'ctx, Ub> {
    fn new(
        datastore: &'ctx RuntimeDataStore,
        user_builder: &'ctx Ub,
        pre_allocate_users: usize,
        stages: Vec<(Rate, Duration)>,
        max_users: usize,
    ) -> Self {
        Self {
            datastore,
            user_builder,
            pre_allocate_users,
            stages,
            max_users,
        }
    }
}

impl<'ctx, Ub> Executor for RampingArrivalRate<'ctx, Ub>
where
    Ub: for<'a> AsyncUserBuilder<'a>,
{
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        let datastore = self.datastore;
        let user_builder = self.user_builder;
        let pre_allocated_users = self.pre_allocate_users;
        let max_users = self.max_users;
        let stages = &*self.stages;
        let total_duration: u64 = stages.iter().map(|(_, duration)| duration.as_secs()).sum();

        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, total_duration = total_duration);
            let mut users: Vec<_> = build_users(datastore, user_builder, pre_allocated_users)
                .await
                .unwrap()
                .into_iter()
                .map(Mutex::new)
                .collect();
            event!(target: CRATE_NAME, Level::INFO, users = users.len(), users_max = pre_allocated_users);

            for (index, (Rate(rate, time_unit), duration)) in stages.iter().enumerate() {
                let end_time = Instant::now() + *duration;
                event!(target: CRATE_NAME, Level::INFO, stage = index + 1, stages = stages.len(), stage_duration = duration.as_secs());

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
                            let _ = tx.send(user_call(user.call()).await);
                        };
                        let span =
                            tracing::span!(target: CRATE_NAME, tracing::Level::INFO, SPAN_TASK);
                        scope.spawn_cancellable(task.instrument(span), || ());
                        current_rate += 1;
                    }

                    scope.collect().await;
                    drop(scope);

                    if current_rate < *rate && users.len() < max_users {
                        users.extend(
                            build_users(datastore, user_builder, rate - current_rate)
                                .await
                                .unwrap()
                                .into_iter()
                                .map(Mutex::new),
                        );
                    }
                    event!(target: CRATE_NAME, Level::INFO, users = users.len(), users_max = pre_allocated_users);

                    if Instant::now() <= end_time || current_rate < *rate {
                        // Sleep until to make sure we wait before next set of task;
                        tokio::time::sleep_until(next_rate_check_time.into()).await;
                    }
                }
            }
        };

        Box::pin(task)
    }
}

async fn user_call<'a>(
    task: impl Future<Output = Result<(), crate::error::Error>> + Send + 'a,
) -> Result<(), crate::error::Error> {
    let res = task.await;
    if let Err(ref err) = res {
        event!(name: "error", target: CRATE_NAME, Level::INFO, err = %err)
    }
    res
}

async fn build_users<'a, Ub: AsyncUserBuilder<'a>>(
    store: &'a RuntimeDataStore,
    user_builder: &'a Ub,
    count: usize,
) -> Result<Vec<<Ub as AsyncUserBuilder<'a>>::Output>, Error> {
    let mut res = vec![];
    for _ in 0..count {
        let user = user_builder.build(store).await?;
        res.push(user)
    }
    Ok(res)
}
