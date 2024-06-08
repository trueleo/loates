use std::pin::Pin;
use std::time::Duration;

use futures::Future;
use reqwest::Client;
use rusher::data::RuntimeDataStore;
use rusher::error::Error;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::runner::Runner;
use rusher::user::BoxedUser;
use rusher::{apply, User};

struct MyUser {
    // client: Client,
    // post_content: Iter,
}

#[async_trait::async_trait]
impl User for MyUser {
    async fn call(&mut self) -> Result<(), Error> {
        // In each iteration get the next string
        // let body = self.post_content.next().unwrap().to_string();
        let res = reqwest::Client::new()
            .post("https://httpbin.org/anything")
            .body("a")
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

        Ok(())
    }
}

#[apply(rusher::boxed_future)]
async fn datastore(store: &mut RuntimeDataStore) {
    // let data = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    // store.insert(data);
    // store.insert(Client::new());
}

fn user_builder<'a>(
    store: &'a RuntimeDataStore,
) -> Pin<Box<dyn Future<Output = Result<BoxedUser<'a>, Error>> + Send + 'a>> {
    let user: Result<Box<dyn User + 'a>, Error> = Ok(Box::new(MyUser {}));
    Box::pin(async move { user })
}

#[tokio::main]
async fn main() {
    let execution = ExecutionPlan::builder()
        .with_user_builder(&user_builder)
        .with_data(datastore)
        .with_executor(Executor::RampingUser {
            pre_allocate_users: 2,
            stages: vec![(Duration::from_secs(10), 2), (Duration::from_secs(20), 3)],
        });

    // let execution_once = ExecutionPlan::builder()
    //     .with_user_builder(&user_builder)
    //     // .with_data(datastore)
    //     .with_executor(Executor::Constant {
    //         users: 4,
    //         duration: Duration::from_secs(4),
    //     });

    let scenario = Scenario::new("scene1".to_string(), execution);
    let scenarios = vec![scenario];

    Runner::new(scenarios).enable_tui(true).run().await.unwrap();
}
