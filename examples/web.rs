use std::time::Duration;

use loates::client::reqwest::Client;
use loates::error::Error;
use loates::prelude::*;

struct MyUser<Iter> {
    client: Client,
    post_content: Iter,
}

impl<'a, Iter> User for MyUser<Iter>
where
    Iter: Iterator<Item = &'a String> + Send,
{
    async fn call(&mut self) -> UserResult {
        // In each iteration get the next string
        let body = self.post_content.next().unwrap().to_string();
        let res = self
            .client
            .post("https://httpbin.org/anything")
            .body(body)
            .send()
            .await?;

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
    let execution_ramping_user = Execution::builder()
        .with_user_builder(user_builder)
        .with_data(datastore)
        .with_executor(Executor::RampingUser {
            pre_allocate_users: 10,
            stages: vec![(1, Duration::from_secs(10)), (1, Duration::from_secs(3))],
        });

    let scenario1 = Scenario::new("scene1", execution_ramping_user);

    let scenario2 = Scenario::new(
        "scene2",
        Execution::new(user_builder, Executor::Once).with_data(datastore),
    );

    let scenarios = vec![scenario1, scenario2];

    Runner::new(scenarios).enable_web(true).run().await.unwrap();
}
