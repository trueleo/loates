use rusher::data::RuntimeDataStore;
use rusher::error::Error;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::runner::Runner;
use rusher::User;
use tracing_subscriber::fmt::format::{format, FmtSpan};

struct MyUser {}

#[async_trait::async_trait]
impl User for MyUser {
    async fn call(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

async fn user_builder(_: &RuntimeDataStore) -> impl User {
    MyUser {}
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .event_format(format().compact())
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();

    let execution = ExecutionPlan::builder()
        .with_user_builder(user_builder)
        .with_executor(Executor::Once);

    let execution_once = ExecutionPlan::builder()
        .with_user_builder(user_builder)
        .with_executor(Executor::Once);

    let scenario = Scenario::new("scene1".to_string(), execution).with_executor(execution_once);
    let scenarios = vec![scenario];

    // tokio::spawn(async move {
    //     while let Some(val) = rx.recv().await {
    //         println!("{:?}", val);
    //     }
    // });

    Runner::new(scenarios).run().await.unwrap();
}
