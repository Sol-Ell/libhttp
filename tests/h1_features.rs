use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use libhttp::{Handler, Runtime, StatusCode};

fn start(
    handler: impl Handler,
) -> (
    libhttp::ServerHandle,
    Runtime,
    std::thread::JoinHandle<Result<(), libhttp::Error>>,
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
    let port = handle.local_addrs()[0].port();
    let join = std::thread::spawn(move || server.run(handler));
    (handle, runtime, join, port)
}

fn request(port: u16, bytes: &[u8]) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(bytes).unwrap();
    let mut output = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => output.extend_from_slice(&buffer[..read]),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

#[test]
fn streams_a_chunked_response() {
    let (handle, runtime, join, port) = start(
        |_req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            res.body().write_all(b"hello ")?;
            res.body().write_all(b"world")?;
            res.body().finish()?;
            Ok(())
        },
    );
    let response = request(
        port,
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(
        response
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
    );
    assert!(response.contains("6\r\nhello \r\n"), "{response}");
    assert!(response.contains("5\r\nworld\r\n"), "{response}");
    assert!(response.ends_with("0\r\n\r\n"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn reads_a_chunked_request_and_trailers() {
    let (handle, runtime, join, port) = start(
        |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            let body = req.body().read_to_end(1024)?;
            let trailer = req
                .trailers()
                .and_then(|trailers| trailers.get_raw("x-check"))
                .map(|value| value.as_bytes().to_vec())
                .unwrap_or_default();
            res.set_body(format!(
                "{}:{}",
                String::from_utf8_lossy(&body),
                String::from_utf8_lossy(&trailer)
            ));
            Ok(())
        },
    );
    let response = request(
        port,
        b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nTrailer: X-Check\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\nX-Check: yes\r\n\r\n",
    );
    assert!(response.ends_with("hello:yes"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn pipelined_requests_are_answered_in_order() {
    let (handle, runtime, join, port) = start(
        |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            let path = req.target().path().to_vec();
            res.set_body(path);
            Ok(())
        },
    );
    let response = request(
        port,
        b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\nGET /two HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    let one = response.find("200 OK").unwrap();
    let two = response[one + 1..].find("200 OK").unwrap() + one + 1;
    assert!(response[one..two].contains("/one"), "{response}");
    assert!(response[two..].contains("/two"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn sends_continue_before_body_read() {
    let (handle, runtime, join, port) = start(
        |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            let body = req.body().read_to_end(1024)?;
            res.set_body(body);
            Ok(())
        },
    );
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut interim = [0_u8; 25];
    stream.read_exact(&mut interim).unwrap();
    assert!(String::from_utf8_lossy(&interim).starts_with("HTTP/1.1 100 Continue"));
    stream.write_all(b"hello").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.ends_with("hello"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn malformed_head_gets_a_400_and_handler_panic_gets_a_500() {
    let (handle, runtime, join, port) = start(
        |_req: &mut libhttp::server::Request, _res: &mut libhttp::server::Response| {
            panic!("handler panic");
        },
    );
    let response = request(port, b"GET / HTTP/1.1\r\nHost : bad\r\n\r\n");
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();

    let (handle, runtime, join, port) = start(
        |_req: &mut libhttp::server::Request, _res: &mut libhttp::server::Response| {
            panic!("handler panic");
        },
    );
    let response = request(
        port,
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 500"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn rejects_a_body_over_the_configured_limit() {
    let runtime = Runtime::builder().reactors(1).workers(1).build().unwrap();
    let server = runtime
        .server()
        .bind("127.0.0.1:0")
        .unwrap()
        .limits(libhttp::Limits {
            max_body_bytes: Some(4),
            ..libhttp::Limits::default()
        })
        .build()
        .unwrap();
    let handle = server.handle();
    let port = handle.local_addrs()[0].port();
    let join = std::thread::spawn(move || {
        server.run(
            |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
                let _ = req.body().read_to_end(1024);
                res.set_body("ok");
                Ok(())
            },
        )
    });
    let response = request(
        port,
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\nConnection: close\r\n\r\n0123456789",
    );
    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn deferred_fallback_runs_on_drop() {
    let (handle, runtime, join, port) = start(
        |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            let (request, response) = req.defer_with_fallback(res, |response, _failure| {
                response.set_status(StatusCode::SERVICE_UNAVAILABLE);
                response.set_body("fallback");
                Ok(())
            })?;
            drop(request);
            drop(response);
            Ok(())
        },
    );
    let response = request(
        port,
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    assert!(response.ends_with("fallback"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn head_has_no_body() {
    let (handle, runtime, join, port) = start(
        |_req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            res.set_body("should not be sent");
            Ok(())
        },
    );
    let response = request(
        port,
        b"HEAD / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(!response.contains("should not be sent"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn header_read_timeout_closes_idle_connection() {
    let runtime = Runtime::builder().reactors(1).workers(1).build().unwrap();
    let server = runtime
        .server()
        .bind("127.0.0.1:0")
        .unwrap()
        .timeouts(libhttp::Timeouts {
            header_read: Duration::from_millis(100),
            ..libhttp::Timeouts::default()
        })
        .build()
        .unwrap();
    let handle = server.handle();
    let port = handle.local_addrs()[0].port();
    let join = std::thread::spawn(move || {
        server
            .run(|_req: &mut libhttp::server::Request, _res: &mut libhttp::server::Response| Ok(()))
    });
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut byte = [0_u8; 1];
    let read = stream.read(&mut byte).unwrap_or(0);
    assert_eq!(read, 0);
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}
