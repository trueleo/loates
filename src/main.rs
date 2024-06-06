use std::time::Duration;

use rusher::data::RuntimeDataStore;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::runner::Runner;
use rusher::{apply, User, UserResult};

#[derive(Debug, Default)]
struct MyUser {
    num: usize,
}

#[async_trait::async_trait]
impl User for MyUser {
    async fn call(&mut self) -> UserResult {
        self.num += 1;
        tokio::time::sleep(Duration::from_millis(1000)).await;
        Err(rusher::error::Error::GenericError("bruh".into()))
        // Ok(())
    }
}

#[apply(rusher::boxed_future)]
async fn datastore(store: &mut RuntimeDataStore) {
    store.insert("hello");
}

#[tokio::main]
async fn main() {
    let user_builder = || MyUser { num: 0 };

    let execution = ExecutionPlan::builder()
        .with_user_builder(&user_builder)
        .with_data(datastore)
        .with_executor(Executor::Constant {
            users: 2,
            duration: Duration::from_secs(4),
        });

    let execution_once = ExecutionPlan::builder()
        .with_user_builder(&user_builder)
        .with_executor(Executor::Constant {
            users: 4,
            duration: Duration::from_secs(4),
        });

    let scenario = Scenario::new("scene1".to_string(), execution).with_executor(execution_once);
    let scenarios = vec![scenario];

    Runner::new(scenarios).enable_tui(true).run().await.unwrap();
}
