
use std::io::Cursor;

use bytes::{BytesMut, Buf};
use tokio_uring::net::TcpStream;
use tracing::info;

use crate::{Frame, frame};
pub struct Connection {
  stream: TcpStream,
  buffer: BytesMut
}

impl Connection {
  pub fn new(socket: TcpStream) -> Connection {
    Connection {
      stream: socket,
      buffer: BytesMut::with_capacity(4 * 1024),
    }
  }

  pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
    loop {
      if let Some(frame) = self.parse_frame()? {
        info!("Parsed frame {}", frame);
        return Ok(Some(frame));
      }

      let read_buffer = BytesMut::with_capacity(4 * 1024);

      let (result, nbuf) = self.stream.read(read_buffer).await;

      if 0 == result? {
        if nbuf.is_empty() {
          return Ok(None);
        } else {
          return Err("Connection reset by peer".into());
        }
      }

      self.buffer.extend(nbuf);
    }
  }

      /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
      use frame::Error::Incomplete;

      // Cursor is used to track the "current" location in the
      // buffer. Cursor also implements `Buf` from the `bytes` crate
      // which provides a number of helpful utilities for working
      // with bytes.
      let mut buf = Cursor::new(&self.buffer[..]);

      // The first step is to check if enough data has been buffered to parse
      // a single frame. This step is usually much faster than doing a full
      // parse of the frame, and allows us to skip allocating data structures
      // to hold the frame data unless we know the full frame has been
      // received.
      match Frame::check(&mut buf) {
          Ok(_) => {
              // The `check` function will have advanced the cursor until the
              // end of the frame. Since the cursor had position set to zero
              // before `Frame::check` was called, we obtain the length of the
              // frame by checking the cursor position.
              let len = buf.position() as usize;

              // Reset the position to zero before passing the cursor to
              // `Frame::parse`.
              buf.set_position(0);

              // Parse the frame from the buffer. This allocates the necessary
              // structures to represent the frame and returns the frame
              // value.
              //
              // If the encoded frame representation is invalid, an error is
              // returned. This should terminate the **current** connection
              // but should not impact any other connected client.
              let frame = Frame::parse(&mut buf)?;

              // Discard the parsed data from the read buffer.
              //
              // When `advance` is called on the read buffer, all of the data
              // up to `len` is discarded. The details of how this works is
              // left to `BytesMut`. This is often done by moving an internal
              // cursor, but it may be done by reallocating and copying data.
              self.buffer.advance(len);

              // Return the parsed frame to the caller.
              Ok(Some(frame))
          }
          // There is not enough data present in the read buffer to parse a
          // single frame. We must wait for more data to be received from the
          // socket. Reading from the socket will be done in the statement
          // after this `match`.
          //
          // We do not want to return `Err` from here as this "error" is an
          // expected runtime condition.
          Err(Incomplete) => Ok(None),
          // An error was encountered while parsing the frame. The connection
          // is now in an invalid state. Returning `Err` from here will result
          // in the connection being closed.
          Err(e) => Err(e.into()),
      }
  }
}