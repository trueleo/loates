# Rusher

Rusher is a blazingly fast 🚀 load testing library for Rust, with focus on ease of use and flexiblity.
Rusher is inspired by tools like grafana k6, you can easily simulate a high number of users to test the performance and reliability of your web applications and APIs.

Instead of being a tool like k6 which runs javascript for you, rusher is a simple rust library that any user can use to define their testcases. Rusher is not desgined with any paticular usecase in mind. It's upto the user to decide what they wanna loadtest and how.

## Features

- **High Performance**: Leveraging Rust's performance and safety, Rusher can handle a high number of virtual users and concurrent requests with minimal overhead.
- **Extensible**: Customizable test scenarios with a simple to use api.
- **Real-time Metrics**: Get real-time feedback and detailed metrics on your application's performance.

## Roadmap

- CLI/TUI components
- Inbuilt webserver to show results on a browser realtime
- Making it easier to run in a distributed setup
- covering more usecases.
- Simulating more complex patterns

## Getting Started

### Prerequisites

- Rust (latest stable version)
- Cargo (Rust package manager)

### Example

```rust
use std::any::TypeId;
use std::time::Duration;

use futures::StreamExt;
use http::StatusCode;

use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::report::Report;
use rusher::runner::{Config, Runner};
use rusher::{User, UserResult};

#[derive(Default)]
struct MyUser;

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
```

Just build your test and deploy anywhere.

```shell
cargo run --release
```

## Contributing

We welcome contributions from the community! If you'd like to contribute, please fork the repository and submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

1.  Fork the repository
2.  Create a new branch (git checkout -b feature-branch)
3.  Make your changes
4.  Commit your changes (git commit -m 'Add some feature')
5.  Push to the branch (git push origin feature-branch)
6.  Open a pull request

### Happy Load Testing! 🚀
