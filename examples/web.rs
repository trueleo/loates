use std::time::Duration;

use rusher::client::reqwest::Client;
use rusher::data::RuntimeDataStore;
use rusher::error::Error;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::runner::Runner;
use rusher::{apply, User};

struct MyUser<Iter> {
    client: Client,
    post_content: Iter,
}

#[async_trait::async_trait]
impl<'a, Iter> User for MyUser<Iter>
where
    Iter: Iterator<Item = &'a String> + Send,
{
    async fn call(&mut self) -> Result<(), Error> {
        // In each iteration get the next string
        let body = self.post_content.next().unwrap().to_string();
        let res = self
            .client
            .post("https://httpbin.org/anything")
            .body(body)
            .send()
            .await
            .map_err(|err| Error::GenericError(err.into()))?;

        if !res.status().is_success() {
            let body = res
                .bytes()
                .await
                .map_err(|err| Error::TerminationError(err.into()))?;

            let err = String::from_utf8_lossy(&body).to_string();
            return Err(Error::termination(err));
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(())
    }
}

#[apply(rusher::boxed_future)]
async fn datastore(store: &mut RuntimeDataStore) {
    let data = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    store.insert(data);
    store.insert(Client::new());
}

async fn user_builder(runtime: &RuntimeDataStore) -> impl User + '_ {
    let client: &Client = runtime.get().unwrap();
    let content: &Vec<String> = runtime.get().unwrap();

    MyUser {
        client: client.clone(),
        post_content: content.iter().cycle(),
    }
}

#[tokio::main]
async fn main() {
    let execution_ramping_user = ExecutionPlan::builder()
        .with_user_builder(user_builder)
        .with_data(datastore)
        .with_executor(Executor::RampingUser {
            pre_allocate_users: 10,
            stages: vec![(1, Duration::from_secs(10)), (1, Duration::from_secs(3))],
        });

    let scenario1 = Scenario::new("scene1".to_string(), execution_ramping_user);

    let execution_once = ExecutionPlan::builder()
        .with_user_builder(user_builder)
        .with_data(datastore)
        .with_executor(Executor::Once);

    let scenario2 = Scenario::new("scene2".to_string(), execution_once);

    let scenarios = vec![scenario1, scenario2];

    Runner::new(scenarios).enable_web(true).run().await.unwrap();
}
