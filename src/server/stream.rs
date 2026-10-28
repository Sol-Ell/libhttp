use std::io::{Read, Write};
use std::sync::Arc;

use bytes::BytesMut;
use mio::net::TcpStream;

use crate::net;
use crate::{Error, TlsError};

pub(crate) struct ReadResult {
    pub(crate) bytes: usize,
    pub(crate) eof: bool,
}

pub(crate) enum ServerStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(TlsServerStream),
}

impl ServerStream {
    pub(crate) fn plain(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }

    #[cfg(feature = "tls")]
    pub(crate) fn tls(stream: TcpStream, config: Arc<rustls::ServerConfig>) -> Result<Self, Error> {
        let connection = rustls::ServerConnection::new(config).map_err(|error| {
            Error::Tls(TlsError::with_source(
                "TLS server initialization failed",
                error,
            ))
        })?;
        Ok(Self::Tls(TlsServerStream {
            tcp: stream,
            connection,
        }))
    }

    pub(crate) fn read_plain(&mut self, out: &mut BytesMut) -> Result<ReadResult, Error> {
        match self {
            Self::Plain(stream) => {
                let mut buffer = [0_u8; 16 * 1024];
                match net::read(stream, &mut buffer) {
                    Ok(0) => Ok(ReadResult {
                        bytes: 0,
                        eof: true,
                    }),
                    Ok(read) => {
                        out.extend_from_slice(&buffer[..read]);
                        Ok(ReadResult {
                            bytes: read,
                            eof: false,
                        })
                    }
                    Err(error) => Err(Error::Io(error)),
                }
            }
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.read_plain(out),
        }
    }

    pub(crate) fn write_plain(&mut self, data: &[u8]) -> Result<usize, Error> {
        match self {
            Self::Plain(stream) => net::send(stream, data).map_err(Error::Io),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.write_plain(data),
        }
    }

    pub(crate) fn flush(&mut self) -> Result<(), Error> {
        match self {
            Self::Plain(_) => Ok(()),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.flush(),
        }
    }

    pub(crate) fn wants_write(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.connection.wants_write(),
        }
    }

    pub(crate) fn is_handshaking(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.connection.is_handshaking(),
        }
    }

    pub(crate) fn shutdown(&mut self) {
        match self {
            Self::Plain(stream) => {
                let _ = stream.shutdown(std::net::Shutdown::Write);
            }
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.shutdown(),
        }
    }

    pub(crate) fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => &stream.tcp,
        }
    }

    pub(crate) fn tcp_mut(&mut self) -> &mut TcpStream {
        match self {
            Self::Plain(stream) => stream,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => &mut stream.tcp,
        }
    }
}

#[cfg(feature = "tls")]
pub(crate) struct TlsServerStream {
    tcp: TcpStream,
    connection: rustls::ServerConnection,
}

#[cfg(feature = "tls")]
impl TlsServerStream {
    fn read_plain(&mut self, out: &mut BytesMut) -> Result<ReadResult, Error> {
        let mut result = ReadResult {
            bytes: 0,
            eof: false,
        };
        loop {
            match self.connection.read_tls(&mut self.tcp) {
                Ok(0) => {
                    result.eof = true;
                    break;
                }
                Ok(_) => {
                    self.connection.process_new_packets().map_err(|error| {
                        Error::Tls(TlsError::with_source("TLS record processing failed", error))
                    })?;
                }
                Err(error) if net::is_would_block(&error) => break,
                Err(error) => return Err(Error::Io(error)),
            }
        }
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            match self.connection.reader().read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    out.extend_from_slice(&buffer[..read]);
                    result.bytes += read;
                }
                Err(error) if net::is_would_block(&error) => break,
                Err(error) => return Err(Error::Io(error)),
            }
        }
        self.flush()?;
        Ok(result)
    }

    fn write_plain(&mut self, data: &[u8]) -> Result<usize, Error> {
        let written = self.connection.writer().write(data).map_err(Error::Io)?;
        self.flush()?;
        Ok(written)
    }

    fn flush(&mut self) -> Result<(), Error> {
        while self.connection.wants_write() {
            match self.connection.write_tls(&mut self.tcp) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if net::is_would_block(&error) => break,
                Err(error) => return Err(Error::Io(error)),
            }
        }
        Ok(())
    }

    fn shutdown(&mut self) {
        self.connection.send_close_notify();
        let _ = self.flush();
        let _ = self.tcp.shutdown(std::net::Shutdown::Write);
    }
}
