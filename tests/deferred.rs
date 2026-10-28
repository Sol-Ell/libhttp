use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use libhttp::Runtime;

#[test]
fn deferred_response_is_sent_later() {
    let runtime = Runtime::builder().reactors(1).workers(2).build().unwrap();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Arc::new(std::sync::Mutex::new(release_rx));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let started_tx = std::sync::Arc::new(std::sync::Mutex::new(started_tx));
    let server = runtime
        .server()
        .bind("127.0.0.1:0")
        .unwrap()
        .build()
        .unwrap();
    let handle = server.handle();
    let port = handle.local_addrs()[0].port();
    let join = std::thread::spawn(move || {
        let handler = move |req: &mut libhttp::server::Request,
                            res: &mut libhttp::server::Response| {
            let (deferred_req, deferred_res) = req.defer(res)?;
            started_tx.lock().unwrap().send(()).unwrap();
            let release_rx = release_rx.clone();
            std::thread::spawn(move || {
                let _ = deferred_req;
                let _ = release_rx.lock().unwrap().recv();
                let mut response = deferred_res;
                response.set_status(libhttp::StatusCode::OK);
                response.set_body("deferred");
                response.send().unwrap();
            });
            Ok(())
        };
        server.run(handler)
    });

    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let mut buffer = [0_u8; 1];
    assert!(stream.read(&mut buffer).is_err());
    release_tx.send(()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.contains("deferred"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn deferred_timeout_sends_gateway_timeout() {
    let runtime = Runtime::builder().reactors(1).workers(2).build().unwrap();
    let server = runtime
        .server()
        .bind("127.0.0.1:0")
        .unwrap()
        .timeouts(libhttp::Timeouts {
            deferred: Some(Duration::from_millis(100)),
            ..libhttp::Timeouts::default()
        })
        .build()
        .unwrap();
    let handle = server.handle();
    let port = handle.local_addrs()[0].port();
    let join = std::thread::spawn(move || {
        server.run(
            |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
                let (_request, response) = req.defer(res)?;
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(500));
                    let _ = response.send_body("too late");
                });
                Ok(())
            },
        )
    });
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 504"), "{response}");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}
