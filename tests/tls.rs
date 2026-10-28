use libhttp::client::ClientRequest;
use libhttp::tls::{TlsClientConfig, TlsServerConfig};
use libhttp::{Runtime, Uri};

#[test]
fn h1_over_tls() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_pem = certified.cert.pem();
    let key_pem = certified.signing_key.serialize_pem();

    let runtime = Runtime::builder().reactors(1).workers(2).build().unwrap();
    let server = runtime
        .server()
        .bind("127.0.0.1:0")
        .unwrap()
        .tls(TlsServerConfig::from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap())
        .build()
        .unwrap();
    let handle = server.handle();
    let port = handle.local_addrs()[0].port();
    let join = std::thread::spawn(move || {
        server.run(
            |_req: &mut libhttp::server::Request, res: &mut libhttp::server::Response| {
                res.set_body("secure hello");
                Ok(())
            },
        )
    });

    let client = runtime
        .client()
        .tls(TlsClientConfig::with_roots_pem(cert_pem.as_bytes()).unwrap())
        .build()
        .unwrap();
    let uri: Uri = format!("https://localhost:{port}/").parse().unwrap();
    let mut response = client.send(ClientRequest::get(uri)).unwrap();
    let body = response.body().read_to_end(1024).unwrap();
    assert_eq!(body, b"secure hello");

    handle.shutdown();
    join.join().unwrap().unwrap();
    runtime.handle().shutdown();
    runtime.handle().wait().unwrap();
}
