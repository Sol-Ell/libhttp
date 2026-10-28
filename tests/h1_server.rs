use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use libhttp::{Error, Handler, Runtime, StatusCode, Version};

fn start(
    handler: impl Handler,
) -> (
    libhttp::ServerHandle,
    Runtime,
    std::thread::JoinHandle<Result<(), Error>>,
    u16,
) {
    let runtime = Runtime::builder().reactors(1).workers(2).build().unwrap();
    let server = runtime
        .server()
        .bind("127.0.0.1:0")
        .unwrap()
        .build()
        .unwrap();
    let handle = server.handle();
    let addr = handle.local_addrs()[0];
    let join = std::thread::spawn(move || server.run(handler));
    (handle, runtime, join, addr.port())
}

fn read_response(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                if buffer.windows(4).any(|window| window == b"\r\n\r\n")
                    && buffer.len() >= 4
                    && (buffer.ends_with(b"hello") || buffer.ends_with(b"\r\n\r\n"))
                {
                    // The first test closes the connection; keep reading until EOF or timeout.
                }
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(error) => panic!("read failed: {error}"),
        }
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

#[test]
fn serves_a_get_and_shuts_down() {
    let (handle, runtime, join, port) = start(
        |_req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            res.set_status(StatusCode::OK)
                .header("content-type", "text/plain")?;
            res.set_body("hello");
            Ok(())
        },
    );

    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let started = std::time::Instant::now();
    let response = read_response(&mut stream);
    let _ = started;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.to_ascii_lowercase().contains("content-length: 5"));
    assert!(response.ends_with("hello"), "{response}");

    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn echoes_a_post_body_and_keeps_alive() {
    let (handle, runtime, join, port) = start(
        |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            let body = req.body().read_to_end(1024)?;
            res.set_status(StatusCode::OK);
            res.set_body(body);
            Ok(())
        },
    );

    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhello")
        .unwrap();
    let first = read_response(&mut stream);
    assert!(first.starts_with("HTTP/1.1 200 OK"), "{first}");
    assert!(first.ends_with("hello"), "{first}");

    stream
        .write_all(
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nworld",
        )
        .unwrap();
    let second = read_response(&mut stream);
    assert!(second.ends_with("world"), "{second}");

    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn version_is_http11() {
    let (handle, runtime, join, port) = start(
        |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            assert_eq!(req.version(), Version::Http11);
            res.set_status(StatusCode::NO_CONTENT);
            Ok(())
        },
    );
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let started = std::time::Instant::now();
    let response = read_response(&mut stream);
    let _ = started;
    assert!(
        response.starts_with("HTTP/1.1 204 No Content"),
        "{response}"
    );
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}
