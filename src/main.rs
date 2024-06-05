use std::any::TypeId;
use std::thread;
use std::time::Duration;

use rusher::data::RuntimeDataStore;
use rusher::error::Error;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::runner::Runner;
use rusher::{User, UserResult};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

#[derive(Debug, Default)]
struct MyUser;

#[async_trait::async_trait]
impl User for MyUser {
    async fn call(&mut self) -> UserResult {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        // Err(Error::TerminationError("bruh".into()))
        Ok(())
    }
}

fn datastore(store: &mut RuntimeDataStore) {
    store.insert(TypeId::of::<String>(), Box::new("hello".to_string()));
}

#[tokio::main]
async fn main() {
    let (tracer, rx_tracer) = rusher::tracing::TraceHttp::new();
    let subscriber = Registry::default().with(tracer);
    let _ = tracing::subscriber::set_global_default(subscriber);

    // use tracing_subscriber::fmt::format::FmtSpan;
    // tracing_subscriber::fmt()
    //     .pretty()
    //     .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
    //     .init();

    let user_builder = || MyUser;

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

    let app = rusher::tui::App::new(&scenarios);

    let runtime = Runner::new(scenarios);
    let tui = thread::spawn(|| app.run(rx_tracer));

    // tokio::spawn(async move {
    //     while let Some(event) = rx_tracer.next().await {
    //         println!("{:?}", event);
    //     }
    // });

    runtime.run().await.unwrap();
    tui.join().unwrap().unwrap();
}
