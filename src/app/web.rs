use axum::{
    extract::State,
    http::StatusCode,
    response::{sse::Event, Html, IntoResponse},
    routing::{get, Router},
    Json,
};
use futures::Future;
use serde::Deserialize;
use std::{
    error::Error,
    future::IntoFuture,
    sync::{Arc, Mutex},
};

use crate::tracing::message::Message;

use super::App;

pub fn run(
    app: Arc<Mutex<App>>,
    mut rx: crate::Receiver<Message>,
) -> impl Future<Output = Result<(), Box<dyn Error + Send + Sync + 'static>>> + Send + 'static {
    let (broadcast_tx, _) = tokio::sync::broadcast::channel(100);

    let router = Router::new()
        .route("/", get(index))
        .route("/updates", get(stream_messages))
        .with_state(broadcast_tx.clone())
        .route("/init", get(get_schema))
        .with_state(app.clone())
        .route("/commands", axum::routing::post(commands))
        .fallback(get(index));

    async move {
        let handle = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let end = matches!(message, Message::End);
                app.lock().unwrap().handle_message(message.clone());
                let _ = broadcast_tx.send(message);
                if end {
                    break;
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
        let server = axum::serve(listener, router.into_make_service()).into_future();

        tokio::select! {
            res = handle => {
                res?;
            }
            res = server => {
                res?;
            }
        }
        Ok(())
    }
}

async fn index() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SSE Updates</title>
    <style>
        body {
            font-family: Arial, sans-serif;
        }
        #updates {
            margin-top: 20px;
        }
        .update {
            padding: 10px;
            border-bottom: 1px solid #ccc;
        }
    </style>
</head>
<body>
    <h1>Real-Time Updates</h1>
    <div id="updates">
        <!-- Updates will be appended here -->
    </div>

    <script>
        // Function to create a new update element
        function createUpdateElement(data) {
            const updateElement = document.createElement('div');
            updateElement.className = 'update';
            updateElement.textContent = data;
            return updateElement;
        }

        // Initialize the EventSource
        const eventSource = new EventSource('/updates');

        // Event listener for incoming messages
        eventSource.onmessage = function(event) {
            const updatesContainer = document.getElementById('updates');
            const updateElement = createUpdateElement(event.data);
            updatesContainer.appendChild(updateElement);
        };

        // Event listener for errors
        eventSource.onerror = function(event) {
            console.error('EventSource failed:', event);
        };
    </script>
</body>
</html>"#,
    )
}

async fn stream_messages(
    State(messages): State<tokio::sync::broadcast::Sender<Message>>,
) -> impl IntoResponse {
    let mut messages = messages.subscribe();

    let messages = async_stream::stream! {
        loop {
            let message = messages.recv().await.map_err(axum::Error::new)?;
            yield Event::default().json_data(&message);
            if matches!(message, Message::End) {
                break;
            }
        }
    };

    axum::response::Sse::new(messages)
}

async fn get_schema(State(app): State<Arc<Mutex<App>>>) -> impl IntoResponse {
    let t = app.lock().unwrap().clone();
    axum::Json::from(t)
}

#[derive(Debug, Deserialize)]
struct Command {
    action: String,
    value: String,
}

async fn commands(Json(payload): Json<Command>) -> impl IntoResponse {
    println!("Received command: {:?}", payload);
    StatusCode::OK
}
