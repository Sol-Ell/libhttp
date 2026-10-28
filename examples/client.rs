use std::io::Write;

use libhttp::client::ClientRequest;
use libhttp::{Client, Uri};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().build()?;
    let uri: Uri = "http://example.com/".parse()?;
    let mut response = client.send(ClientRequest::get(uri))?;
    println!("status: {}", response.status());
    let body = response.body().read_to_end(1 << 20)?;
    std::io::stdout().write_all(&body)?;
    Ok(())
}
