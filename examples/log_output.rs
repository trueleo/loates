use std::time::Duration;

use rusher::error::Error;
use rusher::prelude::*;

use rusher::tracing::message::Message;
use rusher::tracing::TracerLayer;
// use tracing_subscriber::fmt::format::{format, FmtSpan};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

struct MyUser {}

impl User for MyUser {
    async fn call(&mut self) -> UserResult {
        // In each iteration get the next string
        let res = rusher::client::reqwest::Client::new()
            .post("https://httpbin.org/anything")
            .body("abc")
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

async fn user_builder(_: &RuntimeDataStore) -> impl User {
    MyUser {}
}

#[tokio::main]
async fn main() {
    // tracing_subscriber::fmt::SubscriberBuilder::default()
    //     .event_format(format().compact())
    //     .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
    //     .init();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let tracer = TracerLayer::new(tx);
    tracing::subscriber::set_global_default(Registry::default().with(tracer)).unwrap();

    let execution = ExecutionPlan::builder()
        .with_user_builder(user_builder)
        .with_executor(Executor::PerUser {
            users: 1,
            iterations: 8,
        });

    let scenario = Scenario::new("scene1".to_string(), execution);
    let scenarios = vec![scenario];

    // let handler = tokio::spawn(async move {
    //     let mut counter = 0usize;
    //     let mut vec = Vec::with_capacity(6000);
    //     loop {
    //         counter += rx.recv_many(&mut vec, 6000).await;
    //         println!("{:?}", counter);
    //         if vec.iter().any(|val| matches!(val, Message::End)) {
    //             break;
    //         }
    //         vec.clear()
    //     }
    // });

    let handler = tokio::spawn(async move {
        while let Some(val) = rx.recv().await {
            println!("{:?}", val);
            if matches!(val, Message::End) {
                break;
            }
        }
    });

    Runner::new(scenarios).run().await.unwrap();
    handler.await.unwrap();
}
