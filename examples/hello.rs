use libhttp::Runtime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::builder().build()?;
    let server = runtime.server().bind("127.0.0.1:8080")?.build()?;
    println!("listening on {:?}", server.handle().local_addrs());
    server.run(
        |_request: &mut libhttp::server::Request, response: &mut libhttp::server::Response| {
            response.header("content-type", "text/plain; charset=utf-8")?;
            response.body().write_all(b"hello from libhttp\n")?;
            response.body().finish()?;
            Ok(())
        },
    )?;
    Ok(())
}
