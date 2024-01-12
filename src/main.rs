use std::any::TypeId;
use std::time::Duration;

use futures::StreamExt;
use http::StatusCode;
use rusher::data::RuntimeDataStore;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::report::Report;
use rusher::runner::{Config, Runner};
use rusher::{User, UserResult};

#[derive(Default)]
struct MyUser;

#[async_trait::async_trait]
impl User for MyUser {
    async fn call(&mut self) -> UserResult {
        Ok(Report::new(StatusCode::OK))
    }
}

fn datastore(store: &mut RuntimeDataStore) {
    store.insert(TypeId::of::<String>(), Box::new("hello".to_string()));
}

#[tokio::main]
async fn main() {
    let user_builder = || MyUser;
    let execution = ExecutionPlan::builder()
        .with_user_builder(&user_builder)
        .with_data(datastore)
        .with_executor(Executor::ConstantArrivalRate {
            pre_allocate_users: 1,
            rate: 1,
            time_unit: Duration::from_secs(1),
            max_users: 1,
            duration: Duration::from_secs(20),
        });
    let scenario = Scenario::new(execution);
    let (runtime, mut rx) = Runner::new(Config {}, vec![scenario]);
    tokio::spawn(async move {
        while let Some(item) = rx.next().await {
            println!("{:?}", item)
        }
    });
    runtime.run().await.unwrap();
}
