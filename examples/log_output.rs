use std::time::Duration;

use rusher::data::RuntimeDataStore;
use rusher::error::Error;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::runner::Runner;
use rusher::tracing::message::Message;
use rusher::tracing::TraceHttp;
use rusher::{User, USER_TASK};
use tracing::{event, Level};
// use tracing_subscriber::fmt::format::{format, FmtSpan};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

struct MyUser {}

#[async_trait::async_trait]
impl User for MyUser {
    async fn call(&mut self) -> Result<(), Error> {
        event!(name: "event.counter", target: USER_TASK, Level::INFO, value = 1u64);
        // event!(name: "event.counter", target: USER_TASK, Level::INFO, value = 2u64);
        // tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(())
    }
}

async fn user_builder(_: &RuntimeDataStore) -> impl User {
    MyUser {}
}

#[tokio::main]
async fn main() {
    // tracing_subscriber::fmt::SubscriberBuilder::default()
    //     .event_format(format().compact())
    //     .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
    //     .init();

    let (tracer, mut rx) = TraceHttp::new();
    tracing::subscriber::set_global_default(Registry::default().with(tracer)).unwrap();

    let execution = ExecutionPlan::builder()
        .with_user_builder(user_builder)
        .with_executor(Executor::Constant {
            users: 4,
            duration: Duration::from_secs(1),
        });
    let scenario = Scenario::new("scene1".to_string(), execution);
    let scenarios = vec![scenario];

    let handler = tokio::spawn(async move {
        let mut counter = 0usize;
        let mut vec = Vec::with_capacity(6000);
        loop {
            counter += rx.recv_many(&mut vec, 6000).await;
            println!("{:?}", counter);
            if vec.iter().any(|val| matches!(val, Message::End)) {
                break;
            }
            vec.clear()
        }
    });

    Runner::new(scenarios).run().await.unwrap();
    handler.await.unwrap();
}
