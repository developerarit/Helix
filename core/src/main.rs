use axum::{
    Json, Router,
    extract::State,
    response::sse::{Event, Sse},
};
use futures::stream::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

const OLLAMA_HOST: &str = "http://localhost:11434";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatRequest {
    prompt: String,
}

#[derive(Serialize)]
struct StreamPayload {
    content: String,
    done: bool,
}

#[derive(Serialize)]
struct ErrorPayload {
    error: String,
}

struct AppState {
    client: Client,
    history: Mutex<Vec<Message>>,
}

type BoxStream = Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send + 'static>>;

#[tokio::main]
async fn main() {
    let client = Client::new();
    let state = Arc::new(AppState {
        client,
        history: Mutex::new(Vec::new()),
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/chat", axum::routing::post(chat))
        .layer(cors)
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "4000".into());
    let addr = format!("0.0.0.0:{port}");
    println!("helix-core listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn chat(State(state): State<Arc<AppState>>, Json(req): Json<ChatRequest>) -> Sse<BoxStream> {
    let user_msg = Message {
        role: "user".into(),
        content: req.prompt,
    };

    state.history.lock().await.push(user_msg);

    let history = state.history.lock().await.clone();

    let ollama_body = serde_json::json!({
        "model": "llama3",
        "messages": history,
        "stream": true,
    });

    let response = state
        .client
        .post(format!("{OLLAMA_HOST}/api/chat"))
        .json(&ollama_body)
        .send()
        .await;

    let ollama_res = match response {
        Ok(r) => r,
        Err(e) => {
            let err_msg = format!("Failed to connect to Ollama: {e}");
            let stream = futures::stream::once(async move {
                Ok(Event::default()
                    .json_data(ErrorPayload { error: err_msg })
                    .unwrap())
            });
            return Sse::new(Box::pin(stream) as BoxStream);
        }
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(64);
    let history_ref = Arc::clone(&state);

    tokio::spawn(async move {
        use futures::StreamExt;

        let mut stream = ollama_res.bytes_stream();
        let mut full_response = String::new();
        let mut buffer = String::new();

        while let Some(chunk_result) = stream.next().await {
            let chunk = match chunk_result {
                Ok(c) => c,
                Err(_) => break,
            };

            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(newline_pos) = buffer.find('\n') {
                let line = buffer[..newline_pos].trim().to_string();
                buffer = buffer[newline_pos + 1..].to_string();

                if line.is_empty() {
                    continue;
                }

                let parsed: Result<serde_json::Value, _> = serde_json::from_str(&line);
                let json = match parsed {
                    Ok(j) => j,
                    Err(_) => continue,
                };

                let done = json["done"].as_bool().unwrap_or(false);
                let content = json["message"]["content"].as_str().unwrap_or("");

                full_response.push_str(content);

                let payload = StreamPayload {
                    content: content.to_string(),
                    done,
                };

                let event = Event::default().json_data(payload).unwrap();
                if tx.send(Ok(event)).await.is_err() {
                    return;
                }

                if done {
                    history_ref.history.lock().await.push(Message {
                        role: "assistant".into(),
                        content: full_response.clone(),
                    });
                }
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Sse::new(Box::pin(stream) as BoxStream)
}
