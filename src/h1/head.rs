use crate::header::{Header, HeaderMap, HeaderName, HeaderRef, HeaderValue, Host};
use crate::{Error, Limits, Method, ProtocolErrorKind, StatusCode, Target, Version};

/// How the body of an HTTP/1.1 message is delimited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BodyFraming {
    None,
    ContentLength(u64),
    Chunked,
    UntilClose,
}

/// A parsed HTTP/1.1 request head.
#[derive(Debug)]
pub(crate) struct RequestHead {
    pub method: Method,
    pub target: Vec<u8>,
    pub version: Version,
    pub headers: HeaderMap,
    pub framing: BodyFraming,
    pub keep_alive: bool,
    pub expect_continue: bool,
    pub expect_unsupported: bool,
}

impl RequestHead {
    pub(crate) fn target(&self) -> Result<Target<'_>, Error> {
        Target::parse_for_request(&self.target, &self.method, self.version).map_err(Error::Protocol)
    }

    pub(crate) fn decode_headers(
        &mut self,
        registry: &crate::header::HeaderRegistry,
    ) -> Result<(), Error> {
        self.headers.decode_all(registry)?;
        Ok(())
    }
}

/// A parsed HTTP/1.1 response head.
#[derive(Debug)]
pub(crate) struct ResponseHead {
    pub status: StatusCode,
    pub version: Version,
    pub headers: HeaderMap,
    pub framing: BodyFraming,
    pub keep_alive: bool,
}

impl ResponseHead {
    pub(crate) fn decode_headers(
        &mut self,
        registry: &crate::header::HeaderRegistry,
    ) -> Result<(), Error> {
        self.headers.decode_all(registry)?;
        Ok(())
    }
}

/// Parses a request head from the front of `buf`.
///
/// Returns `Ok(None)` when more bytes are needed. `Ok(Some((head, consumed)))` means the head
/// is complete and `consumed` bytes belong to it.
pub(crate) fn parse_request_head(
    buf: &[u8],
    limits: &Limits,
) -> Result<Option<(RequestHead, usize)>, Error> {
    let max = limits.max_head_bytes as usize;
    let mut headers = vec![httparse::EMPTY_HEADER; limits.max_header_count as usize];
    let mut parsed = httparse::Request::new(&mut headers);
    let status = match parsed.parse(buf) {
        Ok(status) => status,
        Err(httparse::Error::TooManyHeaders) => {
            return Err(Error::HeadTooLarge {
                limit: limits.max_head_bytes,
            });
        }
        Err(httparse::Error::Version) => {
            return Err(Error::protocol(
                Version::Http11,
                ProtocolErrorKind::Unsupported,
                "unsupported HTTP version",
            ));
        }
        Err(error) => {
            return Err(Error::protocol(
                Version::Http11,
                ProtocolErrorKind::MalformedMessage,
                httparse_detail(error),
            ));
        }
    };
    let consumed = match status {
        httparse::Status::Complete(consumed) => consumed,
        httparse::Status::Partial => {
            if buf.len() > max {
                return Err(Error::HeadTooLarge {
                    limit: limits.max_head_bytes,
                });
            }
            return Ok(None);
        }
    };
    if consumed > max {
        return Err(Error::HeadTooLarge {
            limit: limits.max_head_bytes,
        });
    }
    validate_head_bytes(&buf[..consumed])?;

    let version = match parsed.version {
        Some(0) => Version::Http10,
        Some(1) => Version::Http11,
        _ => {
            return Err(Error::protocol(
                Version::Http11,
                ProtocolErrorKind::Unsupported,
                "unsupported HTTP version",
            ));
        }
    };
    let method = parsed.method.ok_or_else(|| {
        Error::protocol(
            version,
            ProtocolErrorKind::MalformedMessage,
            "missing request method",
        )
    })?;
    let method = Method::from_bytes(method.as_bytes()).map_err(|_| {
        Error::protocol(
            version,
            ProtocolErrorKind::MalformedMessage,
            "invalid request method",
        )
    })?;
    let target = parsed.path.ok_or_else(|| {
        Error::protocol(
            version,
            ProtocolErrorKind::MalformedMessage,
            "missing request target",
        )
    })?;
    let target = target.as_bytes().to_vec();

    // Validate the target early so method restrictions apply before any handler sees it.
    let parsed_target =
        Target::parse_for_request(&target, &method, version).map_err(Error::Protocol)?;

    let mut headers_map = HeaderMap::with_capacity(parsed.headers.len());
    let mut content_lengths: Vec<u64> = Vec::new();
    let mut transfer_encodings: Vec<String> = Vec::new();
    let mut hosts: Vec<String> = Vec::new();
    let mut connection_tokens: Vec<String> = Vec::new();
    let mut expect_continue = false;
    let mut expect_unsupported = false;

    for header in parsed.headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| {
            Error::protocol(
                version,
                ProtocolErrorKind::MalformedMessage,
                "invalid header name",
            )
        })?;
        let value = HeaderValue::from_bytes(header.value).map_err(|_| {
            Error::protocol(
                version,
                ProtocolErrorKind::MalformedMessage,
                "invalid header value",
            )
        })?;
        let text = value.to_str().map_err(|_| {
            Error::protocol(
                version,
                ProtocolErrorKind::MalformedMessage,
                "header value is not valid UTF-8",
            )
        })?;
        let text = text.trim_matches(' ');
        match name.as_str() {
            "content-length" => {
                content_lengths.push(parse_content_length(text, version)?);
            }
            "transfer-encoding" => {
                transfer_encodings.push(text.to_owned());
            }
            "host" => hosts.push(text.to_owned()),
            "connection" => {
                for token in text.split(',') {
                    connection_tokens.push(token.trim().to_ascii_lowercase());
                }
            }
            "expect" => {
                if text.eq_ignore_ascii_case("100-continue") {
                    expect_continue = true;
                } else {
                    expect_unsupported = true;
                }
            }
            _ => {}
        }
        headers_map.append_raw_owned(name, value);
    }

    if content_lengths.len() > 1 {
        return Err(Error::protocol(
            version,
            ProtocolErrorKind::Framing,
            "duplicate content-length",
        ));
    }
    if !transfer_encodings.is_empty() && !content_lengths.is_empty() {
        return Err(Error::protocol(
            version,
            ProtocolErrorKind::Framing,
            "content-length and transfer-encoding both present",
        ));
    }
    let framing = if let Some(length) = content_lengths.first() {
        BodyFraming::ContentLength(*length)
    } else if !transfer_encodings.is_empty() {
        if transfer_encodings.len() != 1 || !transfer_encodings[0].eq_ignore_ascii_case("chunked") {
            return Err(Error::protocol(
                version,
                ProtocolErrorKind::Unsupported,
                "unsupported transfer-encoding",
            ));
        }
        BodyFraming::Chunked
    } else {
        BodyFraming::None
    };

    if hosts.len() > 1 {
        return Err(Error::protocol(
            version,
            ProtocolErrorKind::Framing,
            "duplicate host",
        ));
    }
    let connection_close = connection_tokens.iter().any(|token| token == "close");
    let connection_keep_alive = connection_tokens.iter().any(|token| token == "keep-alive");
    let keep_alive = match version {
        Version::Http11 => !connection_close,
        Version::Http10 => connection_keep_alive && !connection_close,
        _ => false,
    };

    match &parsed_target {
        Target::Origin { .. } | Target::Asterisk => {
            if version == Version::Http11 && hosts.is_empty() {
                return Err(Error::protocol(
                    version,
                    ProtocolErrorKind::MalformedMessage,
                    "HTTP/1.1 request has no Host field",
                ));
            }
            if let Some(host) = hosts.first() {
                validate_host_text(host, version)?;
            }
        }
        Target::Absolute { authority, .. } => {
            let authority = std::str::from_utf8(authority).map_err(|_| {
                Error::protocol(
                    version,
                    ProtocolErrorKind::MalformedMessage,
                    "absolute-form authority is not UTF-8",
                )
            })?;
            if let Some(host) = hosts.first() {
                if !host.eq_ignore_ascii_case(authority) {
                    return Err(Error::protocol(
                        version,
                        ProtocolErrorKind::MalformedMessage,
                        "Host conflicts with absolute-form authority",
                    ));
                }
            } else {
                let host =
                    Host::decode(HeaderRef::from_bytes(authority.as_bytes()).map_err(|_| {
                        Error::protocol(
                            version,
                            ProtocolErrorKind::MalformedMessage,
                            "invalid absolute-form authority",
                        )
                    })?)
                    .map_err(|_| {
                        Error::protocol(
                            version,
                            ProtocolErrorKind::MalformedMessage,
                            "invalid absolute-form authority",
                        )
                    })?;
                headers_map.set(host);
            }
        }
        Target::Authority(_) => {
            // CONNECT is parsed and then rejected by the server because v1.0 has no tunnels.
        }
    }

    Ok(Some((
        RequestHead {
            method,
            target,
            version,
            headers: headers_map,
            framing,
            keep_alive,
            expect_continue,
            expect_unsupported,
        },
        consumed,
    )))
}

/// Parses a response head from the front of `buf`.
pub(crate) fn parse_response_head(
    buf: &[u8],
    limits: &Limits,
    request_method: &Method,
) -> Result<Option<(ResponseHead, usize)>, Error> {
    let max = limits.max_head_bytes as usize;
    let mut headers = vec![httparse::EMPTY_HEADER; limits.max_header_count as usize];
    let mut parsed = httparse::Response::new(&mut headers);
    let status = match parsed.parse(buf) {
        Ok(status) => status,
        Err(httparse::Error::TooManyHeaders) => {
            return Err(Error::HeadTooLarge {
                limit: limits.max_head_bytes,
            });
        }
        Err(httparse::Error::Version) => {
            return Err(Error::protocol(
                Version::Http11,
                ProtocolErrorKind::Unsupported,
                "unsupported HTTP version",
            ));
        }
        Err(error) => {
            return Err(Error::protocol(
                Version::Http11,
                ProtocolErrorKind::MalformedMessage,
                httparse_detail(error),
            ));
        }
    };
    let consumed = match status {
        httparse::Status::Complete(consumed) => consumed,
        httparse::Status::Partial => {
            if buf.len() > max {
                return Err(Error::HeadTooLarge {
                    limit: limits.max_head_bytes,
                });
            }
            return Ok(None);
        }
    };
    if consumed > max {
        return Err(Error::HeadTooLarge {
            limit: limits.max_head_bytes,
        });
    }
    validate_head_bytes(&buf[..consumed])?;

    let version = match parsed.version {
        Some(0) => Version::Http10,
        Some(1) => Version::Http11,
        _ => {
            return Err(Error::protocol(
                Version::Http11,
                ProtocolErrorKind::Unsupported,
                "unsupported HTTP version",
            ));
        }
    };
    let code = parsed.code.ok_or_else(|| {
        Error::protocol(
            version,
            ProtocolErrorKind::MalformedMessage,
            "missing response status",
        )
    })?;
    let status = StatusCode::from_u16(code).map_err(|_| {
        Error::protocol(
            version,
            ProtocolErrorKind::MalformedMessage,
            "invalid response status",
        )
    })?;
    if status.as_u16() == 101 {
        return Err(Error::protocol(
            version,
            ProtocolErrorKind::Unsupported,
            "protocol upgrades are not supported",
        ));
    }

    let mut headers_map = HeaderMap::with_capacity(parsed.headers.len());
    let mut content_lengths: Vec<u64> = Vec::new();
    let mut transfer_encodings: Vec<String> = Vec::new();
    let mut connection_tokens: Vec<String> = Vec::new();

    for header in parsed.headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| {
            Error::protocol(
                version,
                ProtocolErrorKind::MalformedMessage,
                "invalid header name",
            )
        })?;
        let value = HeaderValue::from_bytes(header.value).map_err(|_| {
            Error::protocol(
                version,
                ProtocolErrorKind::MalformedMessage,
                "invalid header value",
            )
        })?;
        let text = value.to_str().map_err(|_| {
            Error::protocol(
                version,
                ProtocolErrorKind::MalformedMessage,
                "header value is not valid UTF-8",
            )
        })?;
        let text = text.trim_matches(' ');
        match name.as_str() {
            "content-length" => content_lengths.push(parse_content_length(text, version)?),
            "transfer-encoding" => transfer_encodings.push(text.to_owned()),
            "connection" => {
                for token in text.split(',') {
                    connection_tokens.push(token.trim().to_ascii_lowercase());
                }
            }
            _ => {}
        }
        headers_map.append_raw_owned(name, value);
    }

    if content_lengths.len() > 1 {
        return Err(Error::protocol(
            version,
            ProtocolErrorKind::Framing,
            "duplicate content-length",
        ));
    }
    if !transfer_encodings.is_empty() && !content_lengths.is_empty() {
        return Err(Error::protocol(
            version,
            ProtocolErrorKind::Framing,
            "content-length and transfer-encoding both present",
        ));
    }
    let no_body = status.is_empty_body() || *request_method == Method::Head;
    let framing = if no_body {
        BodyFraming::None
    } else if let Some(length) = content_lengths.first() {
        BodyFraming::ContentLength(*length)
    } else if !transfer_encodings.is_empty() {
        if transfer_encodings.len() != 1 || !transfer_encodings[0].eq_ignore_ascii_case("chunked") {
            return Err(Error::protocol(
                version,
                ProtocolErrorKind::Unsupported,
                "unsupported transfer-encoding",
            ));
        }
        BodyFraming::Chunked
    } else {
        BodyFraming::UntilClose
    };

    let connection_close = connection_tokens.iter().any(|token| token == "close");
    let connection_keep_alive = connection_tokens.iter().any(|token| token == "keep-alive");
    let keep_alive = match version {
        Version::Http11 => !connection_close,
        Version::Http10 => connection_keep_alive && !connection_close,
        _ => false,
    };

    Ok(Some((
        ResponseHead {
            status,
            version,
            headers: headers_map,
            framing,
            keep_alive,
        },
        consumed,
    )))
}

fn parse_content_length(text: &str, version: Version) -> Result<u64, Error> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::protocol(
            version,
            ProtocolErrorKind::Framing,
            "invalid content-length",
        ));
    }
    text.parse().map_err(|_| {
        Error::protocol(
            version,
            ProtocolErrorKind::Framing,
            "content-length out of range",
        )
    })
}

fn validate_host_text(host: &str, version: Version) -> Result<(), Error> {
    Host::decode(HeaderRef::from_bytes(host.as_bytes()).map_err(|_| {
        Error::protocol(
            version,
            ProtocolErrorKind::MalformedMessage,
            "invalid Host value",
        )
    })?)
    .map(|_| ())
    .map_err(|_| {
        Error::protocol(
            version,
            ProtocolErrorKind::MalformedMessage,
            "invalid Host value",
        )
    })
}

fn validate_head_bytes(bytes: &[u8]) -> Result<(), Error> {
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            0 => {
                return Err(Error::protocol(
                    Version::Http11,
                    ProtocolErrorKind::MalformedMessage,
                    "NUL in message head",
                ));
            }
            b'\r' => {
                if bytes.get(index + 1) != Some(&b'\n') {
                    return Err(Error::protocol(
                        Version::Http11,
                        ProtocolErrorKind::MalformedMessage,
                        "bare CR in message head",
                    ));
                }
                if matches!(bytes.get(index + 2), Some(b' ' | b'\t')) {
                    return Err(Error::protocol(
                        Version::Http11,
                        ProtocolErrorKind::MalformedMessage,
                        "obs-fold is not allowed",
                    ));
                }
                index += 2;
            }
            b'\n' => {
                return Err(Error::protocol(
                    Version::Http11,
                    ProtocolErrorKind::MalformedMessage,
                    "bare LF in message head",
                ));
            }
            _ => index += 1,
        }
    }
    // Reject whitespace between a field name and its colon.
    for line in bytes.split(|byte| *byte == b'\n').skip(1) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            break;
        }
        let colon = match line.iter().position(|byte| *byte == b':') {
            Some(colon) => colon,
            None => continue,
        };
        if line[..colon]
            .iter()
            .any(|byte| matches!(byte, b' ' | b'\t'))
        {
            return Err(Error::protocol(
                Version::Http11,
                ProtocolErrorKind::MalformedMessage,
                "whitespace before header colon",
            ));
        }
    }
    Ok(())
}

fn httparse_detail(error: httparse::Error) -> &'static str {
    match error {
        httparse::Error::HeaderName => "invalid header name",
        httparse::Error::HeaderValue => "invalid header value",
        httparse::Error::NewLine => "invalid line ending",
        httparse::Error::Status => "invalid status line",
        httparse::Error::Token => "invalid token",
        httparse::Error::TooManyHeaders => "too many headers",
        httparse::Error::Version => "unsupported version",
    }
}

/// A chunked-body decoder that consumes bytes from the front of a `BytesMut`.
pub(crate) struct ChunkedDecoder {
    remaining: u64,
    state: ChunkState,
    trailer_bytes: usize,
    done: bool,
    trailers: HeaderMap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChunkState {
    Size,
    Data,
    DataCrlf,
    Trailers,
    Done,
}

impl ChunkedDecoder {
    pub(crate) fn new() -> Self {
        Self {
            remaining: 0,
            state: ChunkState::Size,
            trailer_bytes: 0,
            done: false,
            trailers: HeaderMap::new(),
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    pub(crate) fn trailers(&self) -> &HeaderMap {
        &self.trailers
    }

    /// Consumes as much as possible from `input`, pushing decoded data into `out`.
    pub(crate) fn feed(
        &mut self,
        input: &mut bytes::BytesMut,
        out: &crate::body::BodyChannel,
        max_trailer_bytes: u32,
    ) -> Result<ChunkedStatus, Error> {
        loop {
            match self.state {
                ChunkState::Size => {
                    let Some(line_end) = find_crlf(input) else {
                        if input.len() > 1024 {
                            return Err(Error::protocol(
                                Version::Http11,
                                ProtocolErrorKind::Framing,
                                "chunk size line too long",
                            ));
                        }
                        return Ok(ChunkedStatus::NeedMore);
                    };
                    let line = &input[..line_end];
                    let size_text = match line.iter().position(|byte| *byte == b';') {
                        Some(extension) => &line[..extension],
                        None => line,
                    };
                    if size_text.is_empty()
                        || !size_text.iter().all(|byte| byte.is_ascii_hexdigit())
                    {
                        return Err(Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::Framing,
                            "invalid chunk size",
                        ));
                    }
                    let size = u64::from_str_radix(
                        std::str::from_utf8(size_text).map_err(|_| {
                            Error::protocol(
                                Version::Http11,
                                ProtocolErrorKind::Framing,
                                "invalid chunk size",
                            )
                        })?,
                        16,
                    )
                    .map_err(|_| {
                        Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::Framing,
                            "chunk size out of range",
                        )
                    })?;
                    let _ = input.split_to(line_end + 2);
                    if size == 0 {
                        self.state = ChunkState::Trailers;
                    } else {
                        self.remaining = size;
                        self.state = ChunkState::Data;
                    }
                }
                ChunkState::Data => {
                    if input.is_empty() {
                        return Ok(ChunkedStatus::NeedMore);
                    }
                    let capacity = out.limit().saturating_sub(out.available());
                    if capacity == 0 {
                        return Ok(ChunkedStatus::NeedMore);
                    }
                    let take = self.remaining.min(input.len() as u64).min(capacity as u64) as usize;
                    if take == 0 {
                        return Ok(ChunkedStatus::NeedMore);
                    }
                    let data = input.split_to(take).freeze();
                    if !out.push(data) {
                        return Err(Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::FlowControl,
                            "body buffer exhausted",
                        ));
                    }
                    self.remaining -= take as u64;
                    if self.remaining == 0 {
                        self.state = ChunkState::DataCrlf;
                    } else {
                        return Ok(ChunkedStatus::NeedMore);
                    }
                }
                ChunkState::DataCrlf => {
                    if input.len() < 2 {
                        return Ok(ChunkedStatus::NeedMore);
                    }
                    if &input[..2] != b"\r\n" {
                        return Err(Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::Framing,
                            "missing CRLF after chunk data",
                        ));
                    }
                    let _ = input.split_to(2);
                    self.state = ChunkState::Size;
                }
                ChunkState::Trailers => {
                    let Some(line_end) = find_crlf(input) else {
                        if self.trailer_bytes + input.len() > max_trailer_bytes as usize {
                            return Err(Error::protocol(
                                Version::Http11,
                                ProtocolErrorKind::Trailer,
                                "trailers too large",
                            ));
                        }
                        return Ok(ChunkedStatus::NeedMore);
                    };
                    let line = &input[..line_end];
                    self.trailer_bytes += line_end + 2;
                    if self.trailer_bytes > max_trailer_bytes as usize {
                        return Err(Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::Trailer,
                            "trailers too large",
                        ));
                    }
                    if line.is_empty() {
                        let _ = input.split_to(line_end + 2);
                        self.state = ChunkState::Done;
                        self.done = true;
                        return Ok(ChunkedStatus::Done);
                    }
                    let colon = line.iter().position(|byte| *byte == b':').ok_or_else(|| {
                        Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::Trailer,
                            "invalid trailer field",
                        )
                    })?;
                    let name = HeaderName::from_bytes(&line[..colon]).map_err(|_| {
                        Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::Trailer,
                            "invalid trailer name",
                        )
                    })?;
                    if matches!(
                        name.as_str(),
                        "content-length" | "transfer-encoding" | "connection" | "host"
                    ) {
                        return Err(Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::Trailer,
                            "forbidden trailer field",
                        ));
                    }
                    let value = &line[colon + 1..];
                    let value = value.strip_prefix(b" ").unwrap_or(value);
                    let value = HeaderValue::from_bytes(value).map_err(|_| {
                        Error::protocol(
                            Version::Http11,
                            ProtocolErrorKind::Trailer,
                            "invalid trailer value",
                        )
                    })?;
                    self.trailers.append_raw_owned(name, value);
                    let _ = input.split_to(line_end + 2);
                }
                ChunkState::Done => return Ok(ChunkedStatus::Done),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChunkedStatus {
    NeedMore,
    Done,
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|window| window == b"\r\n")
}

/// Appends one chunked-transfer body chunk.
pub(crate) fn encode_chunk(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

/// Appends the terminating zero chunk, optional trailers and the final CRLF.
pub(crate) fn encode_last_chunk(
    out: &mut Vec<u8>,
    trailers: Option<&HeaderMap>,
) -> Result<(), Error> {
    out.extend_from_slice(b"0\r\n");
    if let Some(trailers) = trailers {
        trailers.encode_into(out)?;
    }
    out.extend_from_slice(b"\r\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;

    fn limits() -> Limits {
        Limits::default()
    }

    #[test]
    fn parses_a_simple_request() {
        let raw = b"GET /hello?x=1 HTTP/1.1\r\nHost: example.com\r\nX-A: b\r\n\r\n";
        let (head, consumed) = parse_request_head(raw, &limits()).unwrap().unwrap();
        assert_eq!(consumed, raw.len());
        assert_eq!(head.method, Method::Get);
        assert_eq!(head.version, Version::Http11);
        assert!(head.keep_alive);
        assert_eq!(head.target().unwrap().path(), b"/hello");
        assert_eq!(head.headers.get_raw("x-a").unwrap().as_bytes(), b"b");
    }

    #[test]
    fn rejects_smuggling_shapes() {
        for raw in [
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\n".as_slice(),
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice(),
            b"GET / HTTP/1.1\r\nHost : a\r\n\r\n".as_slice(),
            b"GET / HTTP/1.1\r\nHost: a\r\n X: y\r\n\r\n".as_slice(),
        ] {
            assert!(
                parse_request_head(raw, &limits()).is_err(),
                "accepted {:?}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    #[test]
    fn requires_host_for_http11_origin_form() {
        let raw = b"GET / HTTP/1.1\r\n\r\n";
        assert!(parse_request_head(raw, &limits()).is_err());
        let raw = b"GET / HTTP/1.0\r\n\r\n";
        assert!(parse_request_head(raw, &limits()).is_ok());
    }

    #[test]
    fn absolute_form_synthesizes_host() {
        let raw = b"GET http://example.com/a HTTP/1.1\r\n\r\n";
        let (head, _) = parse_request_head(raw, &limits()).unwrap().unwrap();
        assert_eq!(head.headers.get::<Host>().unwrap().as_str(), "example.com");
    }

    #[test]
    fn increments_and_partial_heads() {
        let raw = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
        assert!(parse_request_head(&raw[..10], &limits()).unwrap().is_none());
        assert!(parse_request_head(raw, &limits()).unwrap().is_some());
    }

    #[test]
    fn chunked_decoder_round_trip() {
        struct Noop;
        impl crate::body::ChannelWaker for Noop {
            fn wake(&self) {}
        }
        let pinned = std::sync::Arc::new(crate::body::PinnedBudget::new(1024, 1024));
        let channel = crate::body::BodyChannel::inbound(
            1024,
            pinned,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            std::sync::Arc::new(Noop),
        );
        let mut decoder = ChunkedDecoder::new();
        let mut input =
            bytes::BytesMut::from(&b"5\r\nhello\r\n6\r\n world\r\n0\r\nX-T: 1\r\n\r\n"[..]);
        let status = decoder.feed(&mut input, &channel, 1024).unwrap();
        assert_eq!(status, ChunkedStatus::Done);
        assert!(input.is_empty());
        assert_eq!(channel.available(), 11);
        assert_eq!(decoder.trailers().get_raw("x-t").unwrap().as_bytes(), b"1");
    }

    #[test]
    fn response_framing_uses_until_close_when_undelimited() {
        let raw = b"HTTP/1.0 200 OK\r\n\r\n";
        let (head, _) = parse_response_head(raw, &limits(), &Method::Get)
            .unwrap()
            .unwrap();
        assert_eq!(head.framing, BodyFraming::UntilClose);
        assert!(!head.keep_alive);
    }
}

#[cfg(test)]
mod fuzz_like_tests {
    use super::{parse_request_head, parse_response_head};
    use crate::{Limits, Method};

    proptest::proptest! {
        #[test]
        fn arbitrary_head_bytes_never_panic(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..2048)
        ) {
            let _ = parse_request_head(&bytes, &Limits::default());
            let _ = parse_response_head(&bytes, &Limits::default(), &Method::Get);
        }
    }
}
