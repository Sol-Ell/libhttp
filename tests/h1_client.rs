use std::time::Duration;

use libhttp::client::ClientRequest;
use libhttp::{Handler, Runtime, StatusCode, Uri};

fn start_server(
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

#[test]
fn client_get_and_post() {
    let (handle, runtime, join, port) = start_server(
        |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            if req.method() == libhttp::Method::Post {
                let body = req.body().read_to_end(1024)?;
                res.set_body(body);
            } else {
                res.set_status(StatusCode::OK);
                res.header("content-type", "text/plain")?;
                res.set_body("hello from server");
            }
            Ok(())
        },
    );

    let client = runtime.client().build().unwrap();
    let uri: Uri = format!("http://127.0.0.1:{port}/").parse().unwrap();
    let mut response = client.send(ClientRequest::get(uri.clone())).unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.body().read_to_end(1024).unwrap();
    assert_eq!(body, b"hello from server");

    let mut response = client
        .send(ClientRequest::post(uri).body("uploaded"))
        .unwrap();
    let body = response.body().read_to_end(1024).unwrap();
    assert_eq!(body, b"uploaded");

    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn client_streaming_upload() {
    let (handle, runtime, join, port) = start_server(
        |req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            let body = req.body().read_to_end(1024)?;
            res.set_body(body);
            Ok(())
        },
    );
    let client = runtime.client().build().unwrap();
    let uri: Uri = format!("http://127.0.0.1:{port}/").parse().unwrap();
    let mut response = client
        .send_with(ClientRequest::post(uri), |body| {
            body.write_all(b"chunk one ")?;
            body.write_all(b"chunk two")?;
            body.finish()
        })
        .unwrap();
    let body = response.body().read_to_end(1024).unwrap();
    assert_eq!(body, b"chunk one chunk two");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn client_fetch_callback() {
    let (handle, runtime, join, port) = start_server(
        |_req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            res.set_body("callback");
            Ok(())
        },
    );
    let client = runtime.client().build().unwrap();
    let uri: Uri = format!("http://127.0.0.1:{port}/").parse().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    client
        .fetch(ClientRequest::get(uri), move |result| {
            let body = match result {
                Ok(response) => response.body().read_to_end(1024).unwrap_or_default(),
                Err(error) => {
                    tx.send(Err(error.to_string())).ok();
                    return Ok(());
                }
            };
            tx.send(Ok(body)).ok();
            Ok(())
        })
        .unwrap();
    let body = rx.recv_timeout(Duration::from_secs(3)).unwrap().unwrap();
    assert_eq!(body, b"callback");
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}

#[test]
fn client_connection_is_reused() {
    let (handle, runtime, join, port) = start_server(
        |_req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
            res.set_body("x");
            Ok(())
        },
    );
    let client = runtime.client().build().unwrap();
    let uri: Uri = format!("http://127.0.0.1:{port}/").parse().unwrap();
    for _ in 0..3 {
        let mut response = client.send(ClientRequest::get(uri.clone())).unwrap();
        let _ = response.body().read_to_end(16).unwrap();
    }
    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}
