use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::StatusCode,
    response::{sse::Event, Html, IntoResponse, Response},
    routing::{get, Router},
    Json,
};
use futures::Future;
use serde::Deserialize;
use static_files::Resource;
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::tracing::message::Message;

use super::App;

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

pub fn run(
    app: Arc<Mutex<App>>,
    mut rx: crate::Receiver<Message>,
) -> impl Future<Output = Result<(), Box<dyn Error + Send + Sync + 'static>>> + Send + 'static {
    let static_files = Arc::new(generate());

    let router = Router::new()
        .route("/updates", get(stream_messages))
        .with_state(app.clone())
        .route("/commands", axum::routing::post(commands))
        .route("/*path", get(serve_static))
        .fallback(index)
        .with_state(static_files)
        .layer(tower_http::cors::CorsLayer::very_permissive());

    async move {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let end = matches!(message, Message::End);
                app.lock().unwrap().handle_message(message.clone());
                if end {
                    break;
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
        let _ = open::that("http://localhost:3000");
        axum::serve(listener, router.into_make_service()).await?;
        Ok(())
    }
}

async fn index(static_files: State<Arc<HashMap<&'static str, Resource>>>) -> Html<&'static [u8]> {
    static_files.get("index.html").unwrap().data.into()
}

async fn serve_static(
    Path(path): Path<String>,
    static_files: State<Arc<HashMap<&'static str, Resource>>>,
) -> Result<impl IntoResponse, StatusCode> {
    if let Some(resource) = static_files.get(&*path) {
        let resp = Response::builder()
            .status(200)
            .header(axum::http::header::CONTENT_TYPE, resource.mime_type)
            .body(Body::from(Bytes::from_static(resource.data)))
            .unwrap();
        Ok(resp)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn stream_messages(State(app): State<Arc<Mutex<App>>>) -> impl IntoResponse {
    let app = app.clone();

    let messages = async_stream::stream! {
        loop {
            let (event, ended) = {
                 let app = app.lock().unwrap();
                 let ended = app
                     .scenarios
                     .iter()
                     .all(|s| s.execs.iter().all(|exec| exec.ended));
                 let event =  Event::default().json_data(&*app);
                (event, ended)
            };
            yield event;
            if ended {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };

    axum::response::Sse::new(messages)
}

#[derive(Debug, Deserialize)]
struct Command {
    action: String,
    value: String,
}

async fn commands(Json(command): Json<Command>) -> impl IntoResponse {
    println!("Received command: {} {}", command.action, command.value);
    StatusCode::OK
}
