use std::any::TypeId;

use futures::StreamExt;
use http::StatusCode;
use rusher::data::RuntimeDataStore;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::report::Report;
use rusher::runner::{Config, Runner};
use rusher::{User, UserResult};
use tracing::level_filters::LevelFilter;
use tracing::{event, Level};
use tracing_subscriber::layer::SubscriberExt;

#[derive(Debug, Default)]
struct MyUser;

#[async_trait::async_trait]
impl User for MyUser {
    async fn call(&mut self) -> UserResult {
        event!(name: "plank", target: "load_test", Level::INFO, attar = 1);
        tokio::task::yield_now().await;
        event!(name: "plank", target: "load_test", Level::INFO, attar = 2);
        tokio::task::yield_now().await;
        event!(name: "plank", target: "load_test", Level::INFO, attar = 3);
        Ok(Report::new(StatusCode::OK))
    }
}

fn datastore(store: &mut RuntimeDataStore) {
    store.insert(TypeId::of::<String>(), Box::new("hello".to_string()));
}

#[tokio::main]
async fn main() {
    let (tracer, mut rx_tracer) = rusher::tracing::TraceHttp::new();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .finish()
        .with(tracer);

    let _ = tracing::subscriber::set_global_default(subscriber);

    let user_builder = || MyUser;
    let execution = ExecutionPlan::builder()
        .with_user_builder(&user_builder)
        .with_data(datastore)
        .with_executor(Executor::Once);
    let scenario = Scenario::new("scene1".to_string(), execution);
    let (runtime, mut rx_report) = Runner::new(Config {}, vec![scenario]);
    tokio::spawn(async move {
        while let Some(item) = rx_report.next().await {
            println!("{:?}", item)
        }
    });

    tokio::spawn(async move {
        while let Some(item) = rx_tracer.next().await {
            println!("{:?}", item)
        }
    });

    runtime.run().await.unwrap();
}
