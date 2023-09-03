use std::collections::{HashMap, HashSet};
use std::net::TcpStream;

struct Client {
    conn: TcpStream,
    subs: HashMap<String, Channel>,
    pubs: HashMap<String, Channel>,
    all_chan_names: HashSet<String>,

    msg_queue_len: u32,
    discard_on_full: bool,

    timeout_chans: (),
}
