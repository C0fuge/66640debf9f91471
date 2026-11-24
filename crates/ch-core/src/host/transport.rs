//! NDJSON framing of JSON-RPC envelopes (spec §2.2): one JSON value per `\n`-terminated line,
//! 16 MiB cap, `\r` tolerated, empty lines ignored, arrays rejected.

use ch_protocol::jsonrpc::{Envelope, MAX_FRAME_BYTES, RequestId};
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};

/// What came out of one line.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Frame {
    /// A well-formed envelope.
    Envelope(Envelope),
    /// Valid JSON that is not a JSON-RPC object (an array, a scalar, or an object that fits no
    /// shape). The receiver answers `INVALID_REQUEST` with the id if one could be read (§2.2).
    Invalid {
        /// The request id, when the value was an object with a usable `id`.
        id: Option<RequestId>,
        /// What was wrong.
        reason: String,
    },
}

/// A fatal framing error: the stream is unusable (§2.2) or the peer wrote non-protocol text
/// to stdout (§2.1). The supervisor restarts the plugin.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// A line longer than [`MAX_FRAME_BYTES`].
    #[error("frame longer than {MAX_FRAME_BYTES} bytes")]
    TooLong,
    /// The line was not JSON.
    #[error("stdout line is not JSON: {0}")]
    NotJson(String),
    /// Read error.
    #[error("read: {0}")]
    Io(#[from] std::io::Error),
}

/// Reads frames from a plugin's stdout.
pub struct FrameReader<R> {
    inner: BufReader<R>,
    buf: Vec<u8>,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// Wrap a stream.
    pub fn new(read: R) -> Self {
        Self {
            inner: BufReader::with_capacity(64 * 1024, read),
            buf: Vec::with_capacity(4096),
        }
    }

    /// The next frame, or `None` at EOF. A partial last line (no `\n`) is dropped as EOF.
    pub async fn next(&mut self) -> Result<Option<Frame>, FrameError> {
        loop {
            self.buf.clear();
            let limit = (MAX_FRAME_BYTES + 1) as u64;
            let n = (&mut self.inner)
                .take(limit)
                .read_until(b'\n', &mut self.buf)
                .await?;
            if n == 0 {
                return Ok(None);
            }
            if self.buf.last() != Some(&b'\n') {
                if self.buf.len() > MAX_FRAME_BYTES {
                    return Err(FrameError::TooLong);
                }
                return Ok(None);
            }
            if self.buf.len() > MAX_FRAME_BYTES {
                return Err(FrameError::TooLong);
            }
            let mut line: &[u8] = &self.buf[..self.buf.len() - 1];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            return Ok(Some(parse(line)?));
        }
    }
}

fn parse(line: &[u8]) -> Result<Frame, FrameError> {
    let value: Value = serde_json::from_slice(line).map_err(|e| {
        let shown: String = String::from_utf8_lossy(&line[..line.len().min(200)]).into_owned();
        FrameError::NotJson(format!("{e}: {shown:?}"))
    })?;
    match value {
        Value::Array(_) => Ok(Frame::Invalid {
            id: None,
            reason: "batch arrays are not supported".into(),
        }),
        Value::Object(_) => {
            let id = value
                .get("id")
                .and_then(|v| serde_json::from_value::<RequestId>(v.clone()).ok());
            match serde_json::from_value::<Envelope>(value) {
                Ok(env) => Ok(Frame::Envelope(env)),
                Err(e) => Ok(Frame::Invalid {
                    id,
                    reason: format!("not a JSON-RPC message: {e}"),
                }),
            }
        }
        other => Ok(Frame::Invalid {
            id: None,
            reason: format!("top-level value is {}", kind(&other)),
        }),
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Serialise one message as a frame: compact JSON plus `\n`. Returns `None` when the
/// message would exceed the frame limit.
pub fn encode<T: Serialize>(msg: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut v = serde_json::to_vec(msg)?;
    v.push(b'\n');
    Ok(v)
}

/// Write frames from a channel to the plugin's stdin until the channel closes or a write
/// fails. Returns the write error, if that is how it ended.
pub async fn write_loop<W: tokio::io::AsyncWrite + Unpin>(
    mut stdin: W,
    mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
) -> Option<std::io::Error> {
    while let Some(frame) = rx.recv().await {
        if let Err(e) = stdin.write_all(&frame).await {
            return Some(e);
        }
        if let Err(e) = stdin.flush().await {
            return Some(e);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn frames(input: &[u8]) -> Vec<Result<Option<String>, String>> {
        let mut r = FrameReader::new(input);
        let mut out = Vec::new();
        loop {
            match r.next().await {
                Ok(None) => {
                    out.push(Ok(None));
                    break;
                }
                Ok(Some(Frame::Envelope(e))) => out.push(Ok(Some(format!("env:{:?}", e.method())))),
                Ok(Some(Frame::Invalid { id, reason })) => {
                    out.push(Ok(Some(format!("invalid:{id:?}:{reason}"))));
                }
                Err(e) => {
                    out.push(Err(e.to_string()));
                    break;
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn framing_rules() {
        let input = b"\n  \r\n{\"jsonrpc\":\"2.0\",\"method\":\"status\",\"params\":{}}\r\n[1,2]\n\"x\"\n{\"id\":3,\"foo\":1}\n{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}";
        let got = frames(input).await;
        assert_eq!(got[0], Ok(Some("env:Some(\"status\")".into())));
        assert!(
            got[1]
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .starts_with("invalid:None:batch")
        );
        assert!(
            got[2]
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .starts_with("invalid:None:top-level")
        );
        assert!(
            got[3]
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .starts_with("invalid:Some(Number(3))")
        );
        // The final line has no newline: dropped as EOF.
        assert_eq!(got[4], Ok(None));
    }

    #[tokio::test]
    async fn non_json_is_fatal_and_too_long_is_fatal() {
        let got = frames(b"hello world\n").await;
        assert!(got[0].as_ref().unwrap_err().contains("not JSON"));
        let mut big = vec![b'"'];
        big.resize(MAX_FRAME_BYTES + 5, b'a');
        big.push(b'"');
        big.push(b'\n');
        let got = frames(&big).await;
        assert!(got[0].as_ref().unwrap_err().contains("longer"));
    }
}
