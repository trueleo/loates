/*! Rusher is an simple and easy load testing framework for Rust,
inspired by [grafana k6](https://k6.io) and powered by
[`tracing`](https://docs.rs/tracing/latest/tracing/) crate.

Although this framework covers common usecases like HTTP load testing,
it is usecase agnostic. Metrics for a test is generated via use of
tracing span and events inside of a user call, more about this is documented
in section [generating custom metrics](#custom-metrics). These spans and
events are collected via a tracing subscriber.

# Concepts

Load testing is a process where an application is subjected to a specific
amount of simulated load to evaluate its performance and reliability.
By simulating multiple users accessing the application simultaneously, developers can identify potential bottlenecks, optimize performance, and
ensure the system can handle expected traffic.

* *Virtual Users* - Implementaion of User trait defines how to simulate this interaction with your application in ways similar to a real users. For more detail explaination look at [`User`](user::User).
Users of this framework are suppose to implement [`User`](user::User) trait for each variant of user along with their user builder function.

* *User Builder* - A user builder function defines how to create a User instance. A User can borrow shared data from a [RuntimeDataStore](data::RuntimeDataStore).

* *Datastores* - Each execution creates a [RuntimeDataStore](data::RuntimeDataStore) before it begins execution, which is used to hold shared data that will be used by User.

* *Executors* - Executors control how rusher will allocate Users and perform iterations. Look at Executor docs for more details.

* *Scenario* - Scenario is grouping of one or more executors. Scenarios are ran sequentially in order that they are defined.


# Example
```no_run
use std::time::Duration;

use rusher::client::reqwest::Client;
use rusher::error::Error;
use rusher::prelude::*;

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
```

### Features
- `tui` Enables tui mode, allowing for user to look at live feed of execution in terminal.
- `web` Enables web mode which contains a simple axum server along with a inbuilt UI for looking at updates.
- `serde` - Enable serialization with serde.
- `reqwest` - Wrapper client type for reqwest.
*/

#[cfg(any(feature = "tui", feature = "web"))]
pub mod app;

#[cfg(feature = "reqwest")]
pub mod client;

pub mod data;
pub mod error;
pub mod executor;
pub mod logical;
pub mod runner;
pub mod tracing;
pub mod user;

pub type UserResult = Result<(), crate::error::Error>;

pub mod prelude {
    pub use crate::data::RuntimeDataStore;
    pub use crate::logical::Execution;
    pub use crate::logical::Executor;
    pub use crate::logical::Scenario;
    pub use crate::runner::Config;
    pub use crate::runner::Runner;
    pub use crate::user::User;
    pub use crate::UserResult;
    pub use macro_rules_attribute::apply;
}

#[allow(unused)]
use tokio::sync::mpsc::unbounded_channel as channel;
#[allow(unused)]
use tokio::sync::mpsc::UnboundedReceiver as Receiver;
#[allow(unused)]
use tokio::sync::mpsc::UnboundedSender as Sender;

const CRATE_NAME: &str = env!("CARGO_PKG_NAME");
pub const USER_TASK: &str = "user_event";
const SPAN_TASK: &str = "task";
const SPAN_EXEC: &str = "execution";
const SPAN_SCENARIO: &str = "scenario";
