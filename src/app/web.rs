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
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::tracing::message::Message;

use super::App;

pub fn run(
    app: Arc<Mutex<App>>,
    mut rx: crate::Receiver<Message>,
) -> impl Future<Output = Result<(), Box<dyn Error + Send + Sync + 'static>>> + Send + 'static {
    let router = Router::new()
        .route("/updates", get(stream_messages))
        .with_state(app.clone())
        .route("/commands", axum::routing::post(commands))
        .fallback(get(index))
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
        axum::serve(listener, router.into_make_service()).await?;
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
