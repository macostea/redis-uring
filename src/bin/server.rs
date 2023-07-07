use redis_uring::server;
use tokio_uring::net::TcpListener;
use tokio::signal;
use tracing_subscriber::{util::SubscriberInitExt, fmt, layer::SubscriberExt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::registry().with(fmt::Layer::default()).try_init()?;
  let port = 8080;
  
  tokio_uring::start(async {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port).parse().unwrap()).unwrap();
    server::run(listener, signal::ctrl_c()).await;
    Ok(())
  })
}

