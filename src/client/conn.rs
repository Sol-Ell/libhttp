use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;

use crate::body::{BodyChannel, BodySink, NoopWaker, PinnedBudget, Terminal};
use crate::h1::{
    BodyFraming, ChunkedDecoder, ChunkedStatus, RequestBodyEncoding, ResponseHead, encode_chunk,
    encode_last_chunk, encode_request_head, parse_response_head,
};
use crate::header::HeaderMap;
use crate::tls::TlsClientConfig;
use crate::{Error, Limits, Method, TimeoutKind, Uri, Version};

use super::ClientInner;
use super::pool::PoolKey;

pub(crate) enum ClientStream {
    Plain(TcpStream),
    // TLS is added by the TLS milestone; keeping the variant makes the body pump stable.
    #[allow(dead_code)]
    Tls(Box<dyn ReadWrite + Send>),
}

pub(crate) trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

impl ClientStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }

    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn shutdown(&mut self) {
        match self {
            Self::Plain(stream) => {
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }
            Self::Tls(_) => {}
        }
    }
}

pub(crate) struct ClientConnection {
    stream: ClientStream,
    key: PoolKey,
    read_buf: BytesMut,
    method: Method,
    version: Version,
    framing: BodyFraming,
    remaining: u64,
    chunked: Option<ChunkedDecoder>,
    pub(crate) keep_alive: bool,
    peer: Option<SocketAddr>,
    local: Option<SocketAddr>,
    secure: bool,
}

impl ClientConnection {
    pub(crate) fn connect(
        uri: &Uri,
        tls: Option<&TlsClientConfig>,
        timeouts: &crate::Timeouts,
    ) -> Result<Self, Error> {
        let host = uri.host();
        let port = uri.port().unwrap_or(if uri.is_https() { 443 } else { 80 });
        let addresses: Vec<SocketAddr> =
            (host, port).to_socket_addrs().map_err(Error::Io)?.collect();
        if addresses.is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no addresses resolved",
            )));
        }
        let mut last_error = None;
        let mut stream = None;
        for address in addresses {
            match TcpStream::connect_timeout(&address, timeouts.connect) {
                Ok(candidate) => {
                    stream = Some(candidate);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let stream = stream.ok_or_else(|| {
            Error::Io(last_error.unwrap_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "connection failed")
            }))
        })?;
        stream.set_read_timeout(Some(timeouts.body_idle))?;
        stream.set_write_timeout(Some(timeouts.write_stall))?;
        stream.set_nodelay(true)?;
        let peer = stream.peer_addr().ok();
        let local = stream.local_addr().ok();
        let secure = uri.is_https();
        let key = PoolKey {
            scheme: uri.scheme().to_owned(),
            host: host.to_owned(),
            port,
        };
        if secure && tls.is_none() {
            return Err(Error::InvalidConfig("https requires a TlsClientConfig"));
        }
        let stream = if secure {
            #[cfg(feature = "tls")]
            {
                use rustls::pki_types::ServerName;
                let server_name = ServerName::try_from(host.to_owned())
                    .map_err(|_| Error::InvalidConfig("invalid TLS server name"))?;
                let connection = rustls::ClientConnection::new(
                    tls.expect("checked").rustls_config(),
                    server_name,
                )
                .map_err(|error| {
                    Error::Tls(crate::TlsError::with_source(
                        "TLS client initialization failed",
                        error,
                    ))
                })?;
                ClientStream::Tls(Box::new(rustls::StreamOwned::new(connection, stream)))
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(Error::InvalidConfig("this build has no TLS feature"));
            }
        } else {
            ClientStream::Plain(stream)
        };
        Ok(Self {
            stream,
            key,
            read_buf: BytesMut::with_capacity(16 * 1024),
            method: Method::Get,
            version: Version::Http11,
            framing: BodyFraming::None,
            remaining: 0,
            chunked: None,
            keep_alive: false,
            peer,
            local,
            secure,
        })
    }

    pub(crate) fn key(&self) -> &PoolKey {
        &self.key
    }

    pub(crate) fn peer(&self) -> Option<SocketAddr> {
        self.peer
    }

    pub(crate) fn local(&self) -> Option<SocketAddr> {
        self.local
    }

    pub(crate) fn is_secure(&self) -> bool {
        self.secure
    }

    pub(crate) fn write_all(&mut self, data: &[u8], timeout: TimeoutKind) -> Result<(), Error> {
        let mut offset = 0;
        while offset < data.len() {
            match self.stream.write(&data[offset..]) {
                Ok(0) => {
                    return Err(Error::Io(std::io::Error::from(
                        std::io::ErrorKind::WriteZero,
                    )));
                }
                Ok(written) => offset += written,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut =>
                {
                    return Err(Error::Timeout(timeout));
                }
                Err(error) => return Err(Error::Io(error)),
            }
        }
        Ok(())
    }

    pub(crate) fn send_buffered(&mut self, request: &super::ClientRequest) -> Result<(), Error> {
        self.method = request.method.clone();
        let body = request.body.as_bytes();
        let encoding = if body.is_empty() {
            RequestBodyEncoding::Empty
        } else {
            RequestBodyEncoding::ContentLength(body.len() as u64)
        };
        let mut head = Vec::with_capacity(256);
        encode_request_head(
            &mut head,
            &request.method,
            &request.uri.path_and_query(),
            Version::Http11,
            &request.headers,
            encoding,
            &request.uri.host_header(),
            false,
        )?;
        self.write_all(&head, TimeoutKind::WriteStall)?;
        if !body.is_empty() {
            self.write_all(body, TimeoutKind::WriteStall)?;
        }
        Ok(())
    }

    pub(crate) fn send_chunked_head(
        &mut self,
        request: &super::ClientRequest,
    ) -> Result<(), Error> {
        self.method = request.method.clone();
        let mut head = Vec::with_capacity(256);
        encode_request_head(
            &mut head,
            &request.method,
            &request.uri.path_and_query(),
            Version::Http11,
            &request.headers,
            RequestBodyEncoding::Chunked,
            &request.uri.host_header(),
            false,
        )?;
        self.write_all(&head, TimeoutKind::WriteStall)
    }

    pub(crate) fn read_response_head(
        &mut self,
        registry: &crate::header::HeaderRegistry,
    ) -> Result<ResponseHead, Error> {
        let limits = Limits::default();
        loop {
            match parse_response_head(&self.read_buf, &limits, &self.method) {
                Ok(Some((mut head, consumed))) => {
                    let _ = self.read_buf.split_to(consumed);
                    if head.status.is_informational() {
                        continue;
                    }
                    head.decode_headers(registry)?;
                    self.version = head.version;
                    self.keep_alive = head.keep_alive;
                    self.framing = head.framing;
                    self.remaining = match head.framing {
                        BodyFraming::ContentLength(length) => length,
                        _ => 0,
                    };
                    self.chunked =
                        matches!(head.framing, BodyFraming::Chunked).then(ChunkedDecoder::new);
                    return Ok(head);
                }
                Ok(None) => {
                    if !self.read_more(TimeoutKind::HeaderRead)? {
                        return Err(Error::Closed);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn read_more(&mut self, timeout: TimeoutKind) -> Result<bool, Error> {
        let mut buffer = [0_u8; 16 * 1024];
        match self.stream.read(&mut buffer) {
            Ok(0) => Ok(false),
            Ok(read) => {
                self.read_buf.extend_from_slice(&buffer[..read]);
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                self.read_more(timeout)
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                Err(Error::Timeout(timeout))
            }
            Err(error) => Err(Error::Io(error)),
        }
    }

    pub(crate) fn spawn_body_pump(
        self,
        channel: Arc<BodyChannel>,
        trailers: Arc<std::sync::OnceLock<HeaderMap>>,
        client: Arc<ClientInner>,
    ) {
        let _ = std::thread::Builder::new()
            .name("libhttp-client-body".to_owned())
            .spawn(move || pump_body(self, channel, trailers, client));
    }

    fn read_body_bytes(&mut self, buffer: &mut [u8], timeout: TimeoutKind) -> Result<usize, Error> {
        if !self.read_buf.is_empty() {
            let take = self.read_buf.len().min(buffer.len());
            buffer[..take].copy_from_slice(&self.read_buf[..take]);
            let _ = self.read_buf.split_to(take);
            return Ok(take);
        }
        let mut temp = [0_u8; 16 * 1024];
        match self.stream.read(&mut temp) {
            Ok(0) => Ok(0),
            Ok(read) => {
                let take = read.min(buffer.len());
                buffer[..take].copy_from_slice(&temp[..take]);
                if take < read {
                    self.read_buf.extend_from_slice(&temp[take..read]);
                }
                Ok(take)
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                self.read_body_bytes(buffer, timeout)
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                Err(Error::Timeout(timeout))
            }
            Err(error) => Err(Error::Io(error)),
        }
    }
}

fn pump_body(
    mut conn: ClientConnection,
    channel: Arc<BodyChannel>,
    trailers: Arc<std::sync::OnceLock<HeaderMap>>,
    client: Arc<ClientInner>,
) {
    let result = (|| -> Result<(), Error> {
        match conn.framing {
            BodyFraming::None => {}
            BodyFraming::ContentLength(_) => {
                let mut buffer = [0_u8; 16 * 1024];
                while conn.remaining > 0 {
                    let read = conn.read_body_bytes(&mut buffer, TimeoutKind::BodyIdle)?;
                    if read == 0 {
                        return Err(Error::Closed);
                    }
                    let take = read.min(conn.remaining as usize);
                    channel.write(&buffer[..take])?;
                    conn.remaining -= take as u64;
                }
            }
            BodyFraming::Chunked => {
                let pinned = Arc::new(PinnedBudget::new(1 << 20, 1 << 20));
                let decoded = BodyChannel::inbound(
                    64 * 1024,
                    pinned,
                    Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                    Arc::new(NoopWaker),
                );
                let mut decoder = conn.chunked.take().unwrap_or_else(ChunkedDecoder::new);
                loop {
                    while let Some(chunk) = decoded.take() {
                        channel.write(&chunk)?;
                    }
                    if decoder.is_done() {
                        let _ = trailers.set(decoder.trailers().clone());
                        break;
                    }
                    match decoder.feed(
                        &mut conn.read_buf,
                        &decoded,
                        Limits::default().max_trailer_bytes,
                    )? {
                        ChunkedStatus::Done => {
                            let _ = trailers.set(decoder.trailers().clone());
                            break;
                        }
                        ChunkedStatus::NeedMore => {
                            if !conn.read_more(TimeoutKind::BodyIdle)? {
                                return Err(Error::Closed);
                            }
                        }
                    }
                }
                while let Some(chunk) = decoded.take() {
                    channel.write(&chunk)?;
                }
            }
            BodyFraming::UntilClose => {
                let mut buffer = [0_u8; 16 * 1024];
                loop {
                    let read = conn.read_body_bytes(&mut buffer, TimeoutKind::BodyIdle)?;
                    if read == 0 {
                        break;
                    }
                    channel.write(&buffer[..read])?;
                }
            }
        }
        Ok(())
    })();
    match result {
        Ok(()) => {
            channel.mark_eof();
            if conn.keep_alive && !channel.is_closed() && !client.is_shutdown() {
                client.return_connection(conn);
            } else {
                conn.stream.shutdown();
            }
        }
        Err(error) => {
            let terminal = match &error {
                Error::Timeout(kind) => Terminal::Timeout(*kind),
                Error::Canceled | Error::Closed => Terminal::Canceled,
                _ => Terminal::Canceled,
            };
            channel.mark_terminal(terminal);
            conn.stream.shutdown();
            client.report(&error);
        }
    }
}

pub(crate) struct RequestSink {
    conn: Arc<Mutex<ClientConnection>>,
}

impl RequestSink {
    pub(crate) fn new(conn: Arc<Mutex<ClientConnection>>) -> Self {
        Self { conn }
    }

    fn write_chunk(&self, data: &[u8]) -> Result<usize, Error> {
        let take = data.len().min(16 * 1024);
        let mut framed = Vec::with_capacity(take + 16);
        encode_chunk(&mut framed, &data[..take]);
        self.conn
            .lock()
            .unwrap()
            .write_all(&framed, TimeoutKind::WriteStall)?;
        Ok(take)
    }
}

impl BodySink for RequestSink {
    fn write(&self, data: &[u8]) -> Result<usize, Error> {
        self.write_chunk(data)
    }

    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    fn finish(&self, trailers: Option<HeaderMap>) -> Result<(), Error> {
        let mut tail = Vec::new();
        encode_last_chunk(&mut tail, trailers.as_ref())?;
        self.conn
            .lock()
            .unwrap()
            .write_all(&tail, TimeoutKind::WriteStall)
    }
}

pub(crate) fn connection_key(uri: &Uri) -> PoolKey {
    PoolKey {
        scheme: uri.scheme().to_owned(),
        host: uri.host().to_owned(),
        port: uri.port().unwrap_or(if uri.is_https() { 443 } else { 80 }),
    }
}

#[allow(dead_code)]
fn _duration(_: Duration) {}
