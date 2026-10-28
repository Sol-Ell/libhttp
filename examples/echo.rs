use libhttp::Runtime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::builder().build()?;
    let server = runtime.server().bind("127.0.0.1:8080")?.build()?;
    println!("listening on {:?}", server.handle().local_addrs());
    server.run(
        |request: &mut libhttp::server::Request, response: &mut libhttp::server::Response| {
            let body = request.body().read_to_end(8 * 1024 * 1024)?;
            response.set_body(body);
            Ok(())
        },
    )?;
    Ok(())
}
