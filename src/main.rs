use std::thread;
use std::time::Duration;

use rusher::data::RuntimeDataStore;
use rusher::logical::{ExecutionPlan, Executor, Scenario};
use rusher::runner::Runner;
use rusher::{apply, User, UserResult};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

#[derive(Debug, Default)]
struct MyUser {
    data: usize,
}

#[async_trait::async_trait]
impl User for MyUser {
    async fn call(&mut self) -> UserResult {
        self.data += 1;
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
    let (tracer, rx_tracer) = rusher::tracing::TraceHttp::new();
    let subscriber = Registry::default().with(tracer);
    let _ = tracing::subscriber::set_global_default(subscriber);

    // use tracing_subscriber::fmt::format::FmtSpan;
    // tracing_subscriber::fmt()
    //     .pretty()
    //     .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
    //     .init();

    let user_builder = || MyUser { data: 0 };

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
