//! NDJSON framing (spec §2.2): one JSON value per `\n`-terminated line, at most 16 MiB.

use ch_protocol::MAX_FRAME_BYTES;
use serde::Serialize;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
};
use tokio::sync::{mpsc, oneshot};

use crate::error::EmitError;

/// One line read from the peer.
pub(crate) enum Incoming {
    /// A non-empty frame, without its line terminator.
    Frame(Vec<u8>),
    /// End of stream.
    Eof,
    /// The line exceeded [`MAX_FRAME_BYTES`]: a fatal protocol error.
    TooLong,
}

/// Reads frames, tolerating `\r\n` and skipping empty lines.
pub(crate) struct FrameReader<R> {
    inner: R,
    buf: Vec<u8>,
}

impl<R: AsyncBufRead + Unpin> FrameReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::new(),
        }
    }

    pub(crate) async fn next(&mut self) -> std::io::Result<Incoming> {
        loop {
            self.buf.clear();
            let n = (&mut self.inner)
                .take(MAX_FRAME_BYTES as u64)
                .read_until(b'\n', &mut self.buf)
                .await?;
            if n == 0 {
                return Ok(Incoming::Eof);
            }
            if self.buf.last() == Some(&b'\n') {
                self.buf.pop();
            } else if n == MAX_FRAME_BYTES {
                return Ok(Incoming::TooLong);
            }
            // else: a final line without a newline before EOF; accept it.
            if self.buf.last() == Some(&b'\r') {
                self.buf.pop();
            }
            if self.buf.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            return Ok(Incoming::Frame(std::mem::take(&mut self.buf)));
        }
    }
}

/// What the writer task receives.
pub(crate) enum Out {
    /// A complete frame including its trailing newline.
    Frame(Vec<u8>),
    /// Flush everything queued so far, then stop and acknowledge.
    Close(oneshot::Sender<()>),
}

/// Serialise one message into a frame. Fails when it would exceed the frame limit.
pub(crate) fn encode<T: Serialize>(msg: &T) -> Result<Vec<u8>, EmitError> {
    let mut v = serde_json::to_vec(msg).expect("protocol messages serialise");
    v.push(b'\n');
    if v.len() > MAX_FRAME_BYTES {
        return Err(EmitError::FrameTooLarge(v.len()));
    }
    Ok(v)
}

/// The single writer: frames go out in queue order; flushes whenever the queue drains.
pub(crate) async fn writer_task<W: AsyncWrite + Unpin>(
    mut w: W,
    mut rx: mpsc::UnboundedReceiver<Out>,
) {
    while let Some(out) = rx.recv().await {
        match out {
            Out::Frame(f) => {
                if let Err(e) = w.write_all(&f).await {
                    tracing::debug!("stdout write failed: {e}");
                    break;
                }
                if rx.is_empty() && w.flush().await.is_err() {
                    break;
                }
            }
            Out::Close(ack) => {
                let _ = w.flush().await;
                let _ = w.shutdown().await;
                let _ = ack.send(());
                return;
            }
        }
    }
    let _ = w.flush().await;
    // Drain so `Close` acks are still answered after a write error.
    rx.close();
    while let Some(out) = rx.recv().await {
        if let Out::Close(ack) = out {
            let _ = ack.send(());
        }
    }
}

/// Bridge the process's stdin into an async reader through a dedicated OS thread.
///
/// A thread rather than `tokio::io::stdin()`: tokio's stdin uses the blocking pool, and a read
/// stuck there would keep the runtime from shutting down after `shutdown`. The thread is
/// detached; process exit ends it.
pub(crate) fn stdin_reader() -> impl AsyncRead + Unpin + Send + 'static {
    let (mut tx, rx) = tokio::io::duplex(256 * 1024);
    let handle = tokio::runtime::Handle::current();
    std::thread::Builder::new()
        .name("ch-sdk-stdin".into())
        .spawn(move || {
            use std::io::Read;
            let mut stdin = std::io::stdin().lock();
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match stdin.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if handle.block_on(tx.write_all(&buf[..n])).is_err() {
                            break;
                        }
                    }
                }
            }
            // Dropping `tx` signals EOF to the reader.
        })
        .expect("spawn stdin thread");
    rx
}
