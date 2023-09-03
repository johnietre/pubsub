use std::collections::{HashMap, HashSet};
use std::net::{TcpListener, TcpStream};

fn main() {
    println!("Hello, world!");
}

struct Server<'a> {
    ln: TcpListener,
    channels: HashMap<String, ServerChannel<'a>>,
}

struct ServerClient<'a> {
    server: &'a Server<'a>,
    conn: TcpStream,
    subs: HashSet<String>,
    pubs: HashSet<String>,
}

struct ServerChannel<'a> {
    name: String,
    server: &'a Server<'a>,
    subs: HashSet<ServerClient<'a>>,
    // The number of publishers the channel has
    nums_pubs: i32,
}
