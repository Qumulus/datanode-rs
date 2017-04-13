//! A distributed hierarchical data distribution thingy

#![recursion_limit="128"]

#![feature(associated_consts)]
#![feature(conservative_impl_trait)]
#![feature(custom_derive)]

extern crate bincode;
extern crate bytes;
extern crate env_logger;
extern crate futures;
extern crate futures_cpupool;
#[macro_use] extern crate log;
extern crate mioco;
extern crate rand;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate threadpool;
extern crate time;
extern crate tokio_core;
extern crate tokio_io;

pub mod client;
pub mod cluster;
pub mod command;
pub mod delegate;
pub mod listener;
pub mod manager;
pub mod node;
#[macro_use] pub mod path;
pub mod replica;
pub mod shell;
pub mod server;
pub mod store;
pub mod value;
pub mod zone;

fn main() {
    env_logger::init().unwrap();

    println!("Qumulus v0.0.1");

    let args: Vec<_> = std::env::args().collect();

    if args.len() != 2 {
        println!("Usage: {} <ID>", &args[0]);
        println!("Missing ID. ID must be provided as an IP:port string.");
        println!("This is used as the listening address as well as the data directory.");

        return;
    }

    let id_str = &args[1];
    let id: replica::Replica = id_str.parse().unwrap();

    println!("  ID / address: {:?}", &id);

    let store = store::fs::FS::spawn(&("data_".to_string() + id_str));
    let mut manager = manager::Manager::spawn(id.clone(), store);

    let path = path::Path::empty();
    manager.load(&path);
    println!("  root loaded: {}", manager.zone_loaded(&path));

    let mut api_addr = id.addr().clone();
    api_addr.set_port(id.addr().port() + 1);

    println!("Listening addresses:");
    println!("  Peer: {}", id.addr());
    println!("  API: {}", &api_addr);

    let server = server::Server::new(manager.clone(), api_addr);
    server.listen();

    //let peer_listener = peer_listener::PeerListener::new(manager.clone(), id.addr().clone());
    //peer_listener.spawn();

    let replicas: Vec<replica::Replica> = match std::env::var("CLUSTER") {
        Ok(r) => r.split(' ').map(|r| r.parse().unwrap()).collect(),
        Err(_) => vec![]
    };

    println!("Adding replicas:");

    for replica in replicas {
        println!("  {:?}", &replica);
        manager.cluster.add(replica);
    }

    let stdin = std::io::stdin();

    shell::start(manager.clone(), stdin.lock(), std::io::stdout());

    loop {
        std::thread::park();
    }
}
