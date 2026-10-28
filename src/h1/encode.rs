use crate::header::HeaderMap;
use crate::{Error, Method, StatusCode, Version};

/// How an outgoing request body is delimited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestBodyEncoding {
    /// No body and no framing field.
    Empty,
    /// A known-length body.
    ContentLength(u64),
    /// A streaming chunked body.
    Chunked,
}

/// A response body framing decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResponseBodyEncoding {
    /// No body framing (HEAD, 1xx, 204, 304).
    None,
    /// A known-length body.
    ContentLength(u64),
    /// Chunked framing.
    Chunked,
    /// Body ends when the connection closes.
    UntilClose,
}

const REQUEST_SKIP: &[&str] = &["host", "content-length", "transfer-encoding", "connection"];
const RESPONSE_SKIP: &[&str] = &["content-length", "transfer-encoding", "connection"];

/// Serializes a request head. The library owns Host, framing, and Connection.
pub(crate) fn encode_request_head(
    out: &mut Vec<u8>,
    method: &Method,
    target: &str,
    version: Version,
    headers: &HeaderMap,
    body: RequestBodyEncoding,
    host: &str,
    close: bool,
) -> Result<(), Error> {
    out.extend_from_slice(method.as_str().as_bytes());
    out.push(b' ');
    out.extend_from_slice(target.as_bytes());
    out.push(b' ');
    out.extend_from_slice(version.as_str().as_bytes());
    out.extend_from_slice(b"\r\n");
    headers.encode_into_without(out, REQUEST_SKIP)?;

    if !headers.contains("host") {
        out.extend_from_slice(b"Host: ");
        out.extend_from_slice(host.as_bytes());
        out.extend_from_slice(b"\r\n");
    } else if let Some(host_value) = headers.get_raw("host") {
        out.extend_from_slice(b"Host: ");
        out.extend_from_slice(host_value.as_bytes());
        out.extend_from_slice(b"\r\n");
    } else if let Some(host_value) = headers.get::<crate::header::Host>() {
        out.extend_from_slice(b"Host: ");
        out.extend_from_slice(host_value.as_str().as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        out.extend_from_slice(b"Host: ");
        out.extend_from_slice(host.as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    match body {
        RequestBodyEncoding::Empty => {}
        RequestBodyEncoding::ContentLength(length) => {
            out.extend_from_slice(b"Content-Length: ");
            out.extend_from_slice(length.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RequestBodyEncoding::Chunked => {
            out.extend_from_slice(b"Transfer-Encoding: chunked\r\n");
        }
    }

    if close {
        out.extend_from_slice(b"Connection: close\r\n");
    } else if version == Version::Http10 {
        out.extend_from_slice(b"Connection: keep-alive\r\n");
    }

    out.extend_from_slice(b"\r\n");
    Ok(())
}

/// Serializes a response head. The library owns framing and Connection.
pub(crate) fn encode_response_head(
    out: &mut Vec<u8>,
    status: StatusCode,
    version: Version,
    headers: &HeaderMap,
    body: ResponseBodyEncoding,
    close: bool,
) -> Result<(), Error> {
    out.extend_from_slice(version.as_str().as_bytes());
    out.push(b' ');
    out.extend_from_slice(status.as_u16().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(status.canonical_reason().as_bytes());
    out.extend_from_slice(b"\r\n");
    headers.encode_into_without(out, RESPONSE_SKIP)?;

    match body {
        ResponseBodyEncoding::None => {}
        ResponseBodyEncoding::ContentLength(length) => {
            out.extend_from_slice(b"Content-Length: ");
            out.extend_from_slice(length.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        ResponseBodyEncoding::Chunked => {
            out.extend_from_slice(b"Transfer-Encoding: chunked\r\n");
        }
        ResponseBodyEncoding::UntilClose => {
            out.extend_from_slice(b"Connection: close\r\n");
        }
    }

    if close && body != ResponseBodyEncoding::UntilClose {
        out.extend_from_slice(b"Connection: close\r\n");
    } else if !close && version == Version::Http10 && body != ResponseBodyEncoding::UntilClose {
        out.extend_from_slice(b"Connection: keep-alive\r\n");
    }

    out.extend_from_slice(b"\r\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{ContentType, HeaderMap};

    #[test]
    fn request_head_has_library_owned_framing() {
        let mut headers = HeaderMap::new();
        headers.set(ContentType::json());
        let mut out = Vec::new();
        encode_request_head(
            &mut out,
            &Method::Post,
            "/x",
            Version::Http11,
            &headers,
            RequestBodyEncoding::ContentLength(2),
            "example.com",
            false,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("POST /x HTTP/1.1\r\n"));
        assert!(text.contains("Host: example.com\r\n"));
        assert!(text.contains("Content-Length: 2\r\n"));
        assert!(text.ends_with("\r\n\r\n"));
    }

    #[test]
    fn response_head_frames_chunked() {
        let mut out = Vec::new();
        encode_response_head(
            &mut out,
            StatusCode::OK,
            Version::Http11,
            &HeaderMap::new(),
            ResponseBodyEncoding::Chunked,
            false,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("Transfer-Encoding: chunked\r\n"));
        assert!(!text.contains("Content-Length"));
    }
}
