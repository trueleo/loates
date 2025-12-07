use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use futures::{stream::FuturesUnordered, Future};
use tokio::sync::Mutex;
use tracing::{event, Instrument, Level};

use crate::{
    data::RuntimeDataStore,
    error::Error,
    logical::{self, Rate},
    user::{AsyncUserBuilder, User},
    UserResult, CRATE_NAME, SPAN_TASK,
};

type ExecutorTask<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait Executor<'ctx>: Send
where
    Self: 'ctx,
{
    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_>;
    fn drop(self: Box<Self>) -> ExecutorTask<'ctx> {
        Box::pin(std::future::ready(()))
    }
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

impl<'ctx, Ub> Executor<'ctx> for DataExecutor<'ctx, Ub>
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

    fn drop(self: Box<Self>) -> ExecutorTask<'ctx> {
        use futures::StreamExt;

        match *self {
            DataExecutor::Once(Once { user }) => Box::pin(user.drop()),
            DataExecutor::Constant(Constant { users, .. })
            | DataExecutor::Shared(SharedIterations { users, .. })
            | DataExecutor::PerUser(PerUserIteration { users, .. })
            | DataExecutor::RampingUser(RampingUser { users, .. }) => {
                Box::pin(FuturesUnordered::from_iter(users.into_iter().map(|x| x.drop())).collect())
            }
            DataExecutor::ConstantArrivalRate(RampingArrivalRate { users, .. })
            | DataExecutor::RampingArrivalRate(RampingArrivalRate { users, .. }) => Box::pin(
                FuturesUnordered::from_iter(users.into_iter().map(|x| x.into_inner().drop()))
                    .collect(),
            ),
        }
    }
}

pub(crate) struct Once<U> {
    user: U,
}

impl<U: User> Once<U> {
    fn new(user: U) -> Self {
        Once { user }
    }

    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        let task = self.user.call();
        let exec = async move {
            let spawner = async_scoped::spawner::use_tokio::Tokio;
            let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
            event!(target: CRATE_NAME, Level::INFO, users = 1u64);
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

impl<U: User> Constant<U> {
    fn new(users: Vec<U>, duration: Duration) -> Self {
        Self { users, duration }
    }

    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        let users_len = self.users.len();
        let end_time = Instant::now() + self.duration;
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
            let spawner = async_scoped::spawner::use_tokio::Tokio;
            let mut scope = unsafe { async_scoped::TokioScope::create(spawner) };
            for task in tasks {
                scope.spawn_cancellable(task.in_current_span(), || ());
            }
            event!(target: CRATE_NAME, Level::INFO, users = users_len);
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
        let end_time = Instant::now() + self.duration;
        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, users = users_len);
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

impl<U: User> PerUserIteration<U> {
    fn new(users: Vec<U>, iterations: usize) -> Self {
        Self { users, iterations }
    }

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
            event!(target: CRATE_NAME, Level::INFO, users = users_len);
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

pub(crate) struct RampingUser<'ctx, Ub: AsyncUserBuilder<'ctx> + Sync> {
    datastore: &'ctx RuntimeDataStore,
    user_builder: &'ctx Ub,
    pre_allocate_users: usize,
    stages: Vec<(usize, Duration)>,
    users: Vec<Ub::Output>,
}

impl<'ctx, Ub> RampingUser<'ctx, Ub>
where
    Ub: AsyncUserBuilder<'ctx> + Sync,
{
    fn new(
        datastore: &'ctx RuntimeDataStore,
        user_builder: &'ctx Ub,
        stages: Vec<(usize, Duration)>,
        initial_users: usize,
    ) -> Self {
        Self {
            datastore,
            user_builder,
            pre_allocate_users: initial_users,
            stages,
            users: Vec::default(),
        }
    }

    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        let datastore = self.datastore;
        let user_builder = self.user_builder;
        let pre_allocated_users = self.pre_allocate_users;
        let stages = &*self.stages;
        let users = &mut self.users;

        let task = async move {
            *users = build_users(datastore, user_builder, pre_allocated_users)
                .await
                .unwrap();
            event!(target: CRATE_NAME, Level::INFO, users = users.len());

            for (index, (target_users, duration)) in stages.iter().enumerate() {
                let start_time = chrono::Utc::now().timestamp_millis();
                event!(target: CRATE_NAME, Level::INFO, stage = index + 1, stage_start_time = start_time);

                let len = users.len();
                if len < *target_users {
                    users.extend(
                        build_users(datastore, user_builder, target_users - len)
                            .await
                            .unwrap(),
                    );
                }
                event!(target: CRATE_NAME, Level::INFO, users = users.len());

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

pub(crate) struct RampingArrivalRate<'ctx, Ub: AsyncUserBuilder<'ctx> + Sync> {
    datastore: &'ctx RuntimeDataStore,
    user_builder: &'ctx Ub,
    pre_allocate_users: usize,
    stages: Vec<(Rate, Duration)>,
    max_users: usize,
    users: Vec<Mutex<Ub::Output>>,
}

impl<'ctx, Ub> RampingArrivalRate<'ctx, Ub>
where
    Ub: AsyncUserBuilder<'ctx> + Sync,
{
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
            users: Vec::default(),
        }
    }

    fn execute(&mut self, tx: crate::Sender<UserResult>) -> ExecutorTask<'_> {
        let datastore = self.datastore;
        let user_builder = self.user_builder;
        let pre_allocated_users = self.pre_allocate_users;
        let max_users = self.max_users;
        let stages = &*self.stages;
        let total_duration: u64 = stages.iter().map(|(_, duration)| duration.as_secs()).sum();
        let users = &mut self.users;

        let task = async move {
            event!(target: CRATE_NAME, Level::INFO, total_duration = total_duration);
            *users = build_users(datastore, user_builder, pre_allocated_users)
                .await
                .unwrap()
                .into_iter()
                .map(Mutex::new)
                .collect();
            event!(target: CRATE_NAME, Level::INFO, users = users.len());

            for (index, (Rate(rate, time_unit), duration)) in stages.iter().enumerate() {
                let end_time = Instant::now() + *duration;
                let start_time = chrono::Utc::now().timestamp_millis();
                event!(target: CRATE_NAME, Level::INFO, stage = index + 1, stage_start_time = start_time);

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
                    event!(target: CRATE_NAME, Level::INFO, users = users.len());

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
    use futures::stream::StreamExt;

    let user_futures = FuturesUnordered::new();
    for _ in 0..count {
        user_futures.push(user_builder.build(store));
    }
    let users: Vec<_> = user_futures.collect().await;

    users.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };
    use tokio::sync::mpsc;
    use tracing::{field::Visit, Subscriber};
    use tracing_subscriber::{layer::Context, prelude::*, registry::LookupSpan, Layer};

    #[derive(Debug)]
    struct MockUser {
        id: usize,
        should_fail: bool,
        call_count: Arc<AtomicUsize>,
    }

    impl User for MockUser {
        async fn call(&mut self) -> Result<(), Error> {
            let call_count = self.call_count.clone();
            let should_fail = self.should_fail;
            let id = self.id;
            call_count.fetch_add(1, Ordering::SeqCst);
            if should_fail {
                Err(Error::GenericError(anyhow::anyhow!(
                    "Mock user {} failed",
                    id
                )))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Debug, Default)]
    struct CapturedEvents {
        pub events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    #[derive(Debug, Clone)]
    struct CapturedEvent {
        pub level: Level,
        pub target: String,
        pub fields: HashMap<String, String>,
        pub name: String,
        pub span_name: Option<String>,
    }

    struct MyVisitor<'a>(pub &'a mut HashMap<String, String>);

    impl<'a> Visit for MyVisitor<'a> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{:?}", value));
        }
    }

    impl<S> Layer<S> for CapturedEvents
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
            let mut fields = HashMap::new();
            event.record(&mut MyVisitor(&mut fields));

            let parent_span_name = ctx
                .current_span()
                .metadata()
                .map(|metadata| metadata.name().to_string());

            self.events.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                target: event.metadata().target().to_string(),
                fields,
                name: event.metadata().name().to_string(),
                span_name: parent_span_name,
            });
        }
    }

    #[tokio::test]
    async fn test_once_executor_success() {
        let captured_events = Arc::new(Mutex::new(Vec::new()));
        let collector = CapturedEvents {
            events: captured_events.clone(),
        };
        let _guard = tracing_subscriber::registry().with(collector).set_default();

        // 2. Setup mock user
        let call_count = Arc::new(AtomicUsize::new(0));
        let mock_user = MockUser {
            id: 1,
            should_fail: false,
            call_count: call_count.clone(),
        };

        let mut once_executor = Once::new(mock_user);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let executor_task = once_executor.execute(tx);
        executor_task.await;

        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "User should be called exactly once"
        );

        let result = rx.recv().await.expect("Should receive a result");
        assert!(result.is_ok(), "User call should succeed");

        let events = captured_events.lock().unwrap();
        assert_eq!(
            events.len(),
            1,
            "Expected 1 tracing event for success scenario"
        );

        let user_count_event = events.first().expect("Expected user count event");
        assert_eq!(user_count_event.level, Level::INFO);
        assert_eq!(user_count_event.target, CRATE_NAME);
        assert_eq!(user_count_event.fields.get("users"), Some(&"1".to_string()));
        assert!(
            user_count_event.span_name.is_none(),
            "User count event should not be in SPAN_TASK"
        );
    }

    #[tokio::test]
    async fn test_once_executor_failure() {
        let captured_events = Arc::new(Mutex::new(Vec::new()));
        let collector = CapturedEvents {
            events: captured_events.clone(),
        };
        let _guard = tracing_subscriber::registry().with(collector).set_default();

        let call_count = Arc::new(AtomicUsize::new(0));
        let mock_user = MockUser {
            id: 1,
            should_fail: true,
            call_count: call_count.clone(),
        };

        let mut once_executor = Once::new(mock_user);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let executor_task = once_executor.execute(tx);
        executor_task.await;

        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "User should be called exactly once"
        );

        let result = rx.recv().await.expect("Should receive a result");
        assert!(result.is_err(), "User call should fail");
        assert_eq!(
            result.unwrap_err().to_string(),
            "Mock user 1 failed".to_string()
        );

        let events = captured_events.lock().unwrap();
        assert_eq!(
            events.len(),
            2,
            "Expected 2 tracing events for failure scenario"
        );

        let user_count_event = events
            .iter()
            .find(|e| e.fields.get("users") == Some(&"1".to_string()));
        assert!(user_count_event.is_some(), "Expected user count event");
        assert!(
            user_count_event.unwrap().span_name.is_none(),
            "User count event should not be in SPAN_TASK"
        );

        let error_event = events.iter().find(|e| e.name == "error");
        assert!(error_event.is_some(), "Expected an error event");
        let error_event = error_event.unwrap();
        assert_eq!(error_event.level, Level::INFO);
        assert_eq!(error_event.target, CRATE_NAME);
        assert!(error_event
            .fields
            .get("err")
            .unwrap()
            .contains("Mock user 1 failed"));
        assert_eq!(
            error_event.span_name.as_deref(),
            Some(SPAN_TASK),
            "Error event should be in SPAN_TASK"
        );
    }
}
