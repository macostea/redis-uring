use tokio::time::{self, Duration};
use std::{future::Future, sync::Arc};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio_uring::net::{TcpListener, TcpStream};
use tracing::{info, error};

use crate::{Connection, Shutdown};

struct Listener {
  listener: TcpListener,
  limit_connections: Arc<Semaphore>,
  notify_shutdown: broadcast::Sender<()>,
  shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
  async fn run(&mut self) -> crate::Result<()> {
    info!("accepting inbound connections");

    loop {
      let permit = self.limit_connections.clone().acquire_owned().await.unwrap();

      let socket = self.accept().await?;

      let mut handler = Handler {
        connection: Connection::new(socket),
        shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
        _shutdown_complete: self.shutdown_complete_tx.clone()
      };

      tokio_uring::spawn(async move {
        if let Err(err) = handler.run().await {
          error!(cause = ?err, "connection error");
        }
        // Move the permit into the task and drop it after completion.
        // This returns the permit back to the semaphore.
        drop(permit);
      });

      // tokio_uring::spawn(async move {
      //   use tokio_uring::buf::IoBuf;
  
      //   info!("connected");
      //   let mut n = 0;
  
      //   let mut buf = vec![0u8; 4096];
  
      //   loop {
      //     let (result, nbuf) = socket.read(buf).await;
      //     buf = nbuf;
      //     let read = result.unwrap();
      //     if read == 0 {
      //       break;
      //     }
  
      //     let (res, slice) = socket.write_all(buf.slice(..read)).await;
      //     let _ = res.unwrap();
      //     buf = slice.into_inner();
      //     println!("all {} bytes ping-ponged", read);
      //     n += read;
      //   }
      // });
    }
  }

  async fn accept(&mut self) -> crate::Result<TcpStream> {
    let mut backoff = 1;

    loop {
      match self.listener.accept().await {
        Ok((socket, _)) => return Ok(socket),
        Err(err) => {
          if backoff > 64 {
            return Err(err.into());
          }
        }
      }

      time::sleep(Duration::from_secs(backoff)).await;

      backoff *= 2;
    }
  }
}

struct Handler {
  connection: Connection,
  shutdown: Shutdown,
  _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
      while !self.shutdown.is_shutdown() {
        let maybe_frame = tokio::select! {
          res = self.connection.read_frame() => res?,
          _ = self.shutdown.recv() => {
            return Ok(());
          }
        };

        let frame = match maybe_frame {
          Some(frame) => frame,
          None => return Ok(()),
        };
      }
      Ok(())
    }
}

const MAX_CONNECTIONS: usize = 250;

pub async fn run(listener: TcpListener, shutdown: impl Future) {
  let (notify_shutdown, _) = broadcast::channel(1);
  let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

  let mut server = Listener {
    listener,
    limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
    notify_shutdown,
    shutdown_complete_tx,
  };

  tokio::select! {
    res = server.run() => {
      if let Err(err) = res {
        error!(cause = %err, "failed to accept");
      }
    }

    _ = shutdown => {
      info!("shutting down");
    }
  }

}