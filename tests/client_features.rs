use libhttp::client::ClientRequest;
use libhttp::header::HeaderMap;
use libhttp::server::Response;
use libhttp::{Runtime, Uri};

fn start(
    handler: impl libhttp::Handler,
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

#[test]
fn client_reads_chunked_body_and_trailers() {
    let (handle, runtime, join, port) =
        start(|_req: &mut libhttp::server::Request, res: &mut Response| {
            res.body().write_all(b"part one ")?;
            res.body().write_all(b"part two")?;
            let mut trailers = HeaderMap::new();
            trailers.append_raw("x-check", "ok").unwrap();
            res.body().finish_with_trailers(trailers)?;
            Ok(())
        });
    let client = runtime.client().build().unwrap();
    let uri: Uri = format!("http://127.0.0.1:{port}/").parse().unwrap();
    let mut response = client.send(ClientRequest::get(uri)).unwrap();
    let body = response.body().read_to_end(1024).unwrap();
    assert_eq!(body, b"part one part two");
    let trailers = response.trailers().expect("trailers");
    assert_eq!(trailers.get_raw("x-check").unwrap().as_bytes(), b"ok");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn client_rejects_oversized_response_body() {
    let (handle, runtime, join, port) =
        start(|_req: &mut libhttp::server::Request, res: &mut Response| {
            res.body().write_all(b"0123456789")?;
            res.body().finish()?;
            Ok(())
        });
    let client = runtime
        .client()
        .max_response_body_bytes(Some(4))
        .build()
        .unwrap();
    let uri: Uri = format!("http://127.0.0.1:{port}/").parse().unwrap();
    let mut response = client.send(ClientRequest::get(uri)).unwrap();
    let error = response.body().read_to_end(1024).unwrap_err();
    assert!(
        matches!(error, libhttp::Error::BodyTooLarge { .. }),
        "{error:?}"
    );
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn client_header_timeout_is_reported() {
    let (handle, runtime, join, port) =
        start(|_req: &mut libhttp::server::Request, res: &mut Response| {
            std::thread::sleep(std::time::Duration::from_millis(500));
            res.set_body("late");
            Ok(())
        });
    let client = runtime
        .client()
        .timeouts(libhttp::Timeouts {
            header_read: std::time::Duration::from_millis(100),
            ..libhttp::Timeouts::default()
        })
        .build()
        .unwrap();
    let uri: Uri = format!("http://127.0.0.1:{port}/").parse().unwrap();
    let error = match client.send(ClientRequest::get(uri)) {
        Ok(_) => panic!("response should time out"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            libhttp::Error::Timeout(libhttp::TimeoutKind::HeaderRead)
        ),
        "{error:?}"
    );
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn client_streams_a_large_upload() {
    let (handle, runtime, join, port) =
        start(|req: &mut libhttp::server::Request, res: &mut Response| {
            let body = req.body().read_to_end(200_000)?;
            res.set_body(body.len().to_string());
            Ok(())
        });
    let client = runtime.client().build().unwrap();
    let uri: Uri = format!("http://127.0.0.1:{port}/").parse().unwrap();
    let payload = vec![b'z'; 100_000];
    let mut response = client
        .send_with(ClientRequest::post(uri), move |body| {
            body.write_all(&payload)?;
            body.finish()
        })
        .unwrap();
    let body = response.body().read_to_end(64).unwrap();
    assert_eq!(body, b"100000");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}
