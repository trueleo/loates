use std::time::Duration;

use rusher::client::reqwest::Client;
use rusher::error::Error;
use rusher::prelude::*;

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
    let execution_once = ExecutionPlan::builder()
        .with_user_builder(user_builder)
        .with_data(datastore)
        .with_executor(Executor::Once);

    let execution_shared = ExecutionPlan::builder()
        .with_user_builder(user_builder)
        .with_data(datastore)
        .with_executor(Executor::Shared {
            users: 2,
            iterations: 1000,
            duration: Duration::from_secs(100),
        });

    let scenario =
        Scenario::new("scene1".to_string(), execution_shared).with_executor(execution_once);
    let scenarios = vec![scenario];

    Runner::new(scenarios).enable_tui(true).run().await.unwrap();
}
