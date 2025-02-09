use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::handler::Handler;

pub async fn run_server(addr: &str, handler: Handler) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Listening for Kafka connections on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("New connection from: {}", addr);
                let handler = handler.clone();
                tokio::spawn(async move {
                    if let Err(e) = handler.handle_connection(socket).await {
                        warn!("Connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                warn!("Failed to accept connection: {}", e);
            }
        }
    }
} 