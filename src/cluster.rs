//! Cluster manager. Handles Cluster and Sharding (TODO)

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
//use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use bytes::Bytes;
use bytes::BytesMut;

use bincode;
use futures::{future,Future,Sink,Stream};
use futures::future::FutureResult;
use futures::stream::BoxStream;
use futures::sync::mpsc::{UnboundedReceiver,unbounded,UnboundedSender};
use futures_cpupool::CpuPool;
use tokio_core::net::{TcpListener,TcpStream};
use tokio_core::reactor::{Core,Handle};
use tokio_io::codec::length_delimited::Framed;

use manager::ManagerHandle;
use node::NodeTree;
use path::Path;
use replica::Replica;

const NOT_RUNNING: &str = "Cluster process not running";

/// The Cluster process is implemented as an event loop on a single thread. The public interface is
/// via `ClusterHandle`.
pub struct Cluster {
    handle: Handle,
    id: Replica,
    manager: ManagerHandle,
    peers: HashMap<Replica, Peer>,
    replicas: Vec<Replica>,
    thread_pool: CpuPool
}

/// A handle to the Cluster process. This is the shareable public interface.
#[derive(Clone)]
pub struct ClusterHandle {
    tx: UnboundedSender<ClusterCall>
}

/// Intra-Cluster Messages
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ClusterMessage {
    /// Data to be merged for Path
    Merge(Path, NodeTree)
}

pub struct Peer {
    up: bool,
    tx: UnboundedSender<ClusterMessage>
}

pub struct Server {
}

/// A pre-handle to the Cluster process. This is needed because both the Manager and Cluster
/// processes need handles to each other.
pub struct ClusterPreHandle {
    pub handle: ClusterHandle,
    rx: UnboundedReceiver<ClusterCall>
}

/// Used for dispatching calls via message passing.
#[derive(Debug)]
pub enum ClusterCall {
    Add(Replica),
    Replicate(Path, NodeTree),
    Sync
}

impl ClusterHandle {
    /// Add a new Replica to cluster
    pub fn add(&self, replica: Replica) {
        //UnboundedReceiver::send(&self.tx, ClusterCall::Add(replica));
        (&self.tx).send(ClusterCall::Add(replica)).expect(NOT_RUNNING);
    }

    /// Syncs all Zones with lastest Replica list
    pub fn sync(&self) {
        (&self.tx).send(ClusterCall::Sync).expect(NOT_RUNNING);
    }

    pub fn replicate(&self, path: &Path, data: NodeTree) {
        (&self.tx).send(ClusterCall::Replicate(path.clone(), data)).expect(NOT_RUNNING);
    }

    /// Creates a noop ClusterHandle for testing
    #[cfg(test)]
    pub fn test_handle() -> ClusterHandle {
        ClusterHandle {
            tx: channel().0
        }
    }
}

/// Cluster and Manger need handles to each other.
impl ClusterPreHandle {
    pub fn new() -> ClusterPreHandle {
        let (tx, rx) = unbounded();

        ClusterPreHandle {
            handle: ClusterHandle {
                tx: tx
            },
            rx: rx
        }
    }

    /// Start the Cluster "process".
    pub fn spawn(self, id: Replica, manager: ManagerHandle) {
        thread::Builder::new().name("Cluster".into()).spawn(move || {
            let core = Core::new().unwrap();
            let handle = core.handle();

            let cluster = Cluster {
                handle: handle,
                id: id,
                manager: manager,
                peers: HashMap::new(),
                replicas: vec![],
                thread_pool: CpuPool::new(10)
            };

            cluster.run(core, self.rx);

        }).expect("Cluster spawn failed");
    }
}

impl Cluster {
    /*
    pub fn new(handle: ClusterPreHandle, manager: ManagerHandle) -> Cluster {
        Cluster {
            manager: manager,
            replicas: vec![],
            rx: handle.rx
        }
        */


    pub fn run(self, mut core: Core, rx: UnboundedReceiver<ClusterCall>) {
        let handle = self.handle.clone();
        let addr = self.id.addr().clone();

        let this = Rc::new(RefCell::new(self));
        let server = Server::spawn(&addr, &handle, this.clone());

        let message_loop = rx.for_each(|call| {
let mut this = this.borrow_mut();
            match call {
                ClusterCall::Add(replica) => this.add(replica),
                ClusterCall::Replicate(path, data) => this.replicate(path, data),
                ClusterCall::Sync => this.sync()
            }

            Ok(())
        });

        core.run(message_loop).unwrap();
    }

    /// Handles a message from the cluster
    pub fn handle_cluster_message(&self, msg: ClusterMessage) -> impl Future<Item = (), Error = ()>
        //where F: Future<Item = ClusterMessage>
    {
println!("in handle_cluster_message");
use futures;
        let thread_pool = self.thread_pool.clone();
        let manager = self.manager.clone();

        match msg {
            ClusterMessage::Merge(path, data) => {
                thread_pool.spawn_fn(move|| -> FutureResult<(),()> {
println!("loading {:?}...", &path);
let zone = manager.load(&path);

zone.merge_no_replicate(data);
println!("merged (async) {:?}", &path);
                    future::ok(())
                }).map_err(|_|())
            }
        }
    }

    /// Add a new Replica to Cluster
    pub fn add(&mut self, replica: Replica) {
        if self.replicas.contains(&replica) {
            return;
        }

        self.replicas.push(replica.clone());

        let peer = Peer::spawn(replica.addr(), &self.handle);

        self.peers.insert(replica, peer);

        // TODO: sync?
    }

    /// Replicates data to all replicas
    pub fn replicate(&self, path: Path, data: NodeTree) {
        // TODO: shard
        // for now, replicate to all replicas
        let message = ClusterMessage::Merge(path.clone(), data);

        for (_addr, peer) in &self.peers {
println!("need to replicate {:?} {:?}", _addr, &path);
peer.send(message.clone());
        }
    }

    /// Make sure each Zone has the latest list of Replicas
    pub fn sync(&self) {
        self.manager.store.each_zone(|path| {
println!("TODOOOO");
            //let zone = self.manager.load(&path);

            //zone.sync_replicas(self.replicas.clone());
        })
    }
}

impl Peer {
    pub fn spawn(addr: &SocketAddr, handle: &Handle) -> Peer {
        let (tx, rx) = unbounded();

println!("connecting");
        let connect = TcpStream::connect(addr, handle).and_then(|stream| {
println!("connected {:?} <-> {:?}", stream.local_addr(), stream.peer_addr());
            let stream = Framed::new(stream);
            let (writer, reader) = stream.split();

            /*
            let writer = rx.fold(writer, |writer, msg| {
//println!("Received message locally to be sent out {:?}", &msg);
                let limit = bincode::Infinite;
                let serialized = bincode::serialize(&msg, limit).unwrap();
                let serialized = BytesMut::from(serialized);

                writer.send(serialized).map_err(|e| {
                    println!("FFFFFFFFFFUUUUUUUUU {:?}", e);
                    ()
                })
            }).map(|_| ()).map_err(|e| {
                println!("FFFFFFFFFFFFFFFFFFFFFFFUUU {:?}", e);
                ()
            });
            */
use std::io::{Error, ErrorKind};

            let writer = rx.map(|msg| {
                let limit = bincode::Infinite;
                let serialized = bincode::serialize(&msg, limit).unwrap();

                BytesMut::from(serialized)
            })
                .map_err(|_| Error::new(ErrorKind::Other, ":("))
                .forward(writer)
                .map_err(|e| {
                    println!("FFFFFFFFFFUUUUUUUUU {:?}", e);
                    ()
                }).map(|_| ());

            let reader = reader.for_each(|msg| {
println!("reader got {:?}", msg);
                Ok(())
            }).map(|_| {
                println!("disconnected?");
                ()
            }).map_err(|_| ());

            // throw away errors
            reader.select(writer).then(|_| Ok(()))
        }).map_err(|e| {
println!("Could not connect :( {:?}", e);
            ()
        });

        handle.spawn(connect);

        Peer {
            up: false,
            tx: tx
        }
    }

    pub fn send(&self, msg: ClusterMessage) {
        (&self.tx).send(msg).unwrap();
    }
}

impl Server {
    pub fn spawn(addr: &SocketAddr, handle: &Handle, cluster: Rc<RefCell<Cluster>>) -> Server {
        let socket = TcpListener::bind(addr, handle).unwrap();
println!("Listening on: {}", addr);

        // This is a single-threaded server, so we can just use Rc and RefCell to
        // store the map of all connections we know about.
        //let connections = Rc::new(RefCell::new(HashMap::new()));

let handle_clone = handle.clone();
        let server = socket.incoming().for_each(move |(stream, addr)| {
println!("New Connection: {}", addr);
            let stream: Framed<TcpStream, Bytes> = Framed::new(stream);
            let (writer, reader) = stream.split();

let cluster = cluster.clone();
            let reader = reader.map_err(|_| ()).and_then(|msg| {
                bincode::deserialize::<ClusterMessage>(msg.as_ref())
                    .map_err(|_| {
                        println!("FFFFFFFFFFFFFFFFFF");
                        ()
                    })
            }).and_then(move|msg| {
//match msg {
    //Err(err) => {
//println!("TODO: drop this connection");
//println!("err {}:", err.description());
                        //error!("err {}:", err.description());
                        //Err(StoreError::ReadError(Box::new(err)))
//Ok(())
//Err(())
//use path::Path;
//let msg = ClusterMessage::Merge(Path::empty(), Default::default());
//let cluster = cluster.borrow_mut();
                        //cluster.handle_cluster_message(msg)
//future::err(())
//future::ok::<(),()>(())
                    //},
                    //Ok(msg) => {
let cluster = cluster.borrow_mut();
                        cluster.handle_cluster_message(msg)
                    //}
            //}
/*
                let msg = bincode::deserialize::<ClusterMessage>(msg.as_ref());
println!("reader got {:?}", &msg);
let cluster = cluster.borrow_mut();

if let Ok(msg) = msg {
    cluster.handle_cluster_message(msg);
                return Ok(())
    //return cluster.handle_cluster_message(msg).map_err(|_| Err(()));
}

match msg {
                    Err(err) => {
println!("TODO: drop this connection");
//println!("err {}:", err.description());
                        //error!("err {}:", err.description());
                        //Err(StoreError::ReadError(Box::new(err)))
//Ok(())
k(())
                    },
                    Ok(msg) => {
                        cluster.handle_cluster_message(msg);
//Ok(())
future::ok(())
                    }
                }
            //}).for_each(|msg| {
//println!("got msg {:?}", msg);
                //Ok(())
*/
            }).map(|_| ()).map_err(|_| ()).for_each(|_| Ok(()));

let reader = reader.then(|_| { println!("DCCCCCCC"); Ok(()) });
            handle_clone.spawn(reader);
Ok(())
        }).map_err(|_| ());

        handle.spawn(server);

        Server {}
    }
}

#[test]
fn test_cluster() {
    let replicas = vec![
        "127.0.0.1:1000".parse().unwrap(),
        "127.0.0.1:1001".parse().unwrap(),
        "127.0.0.1:1002".parse().unwrap()
    ];

    use manager;

    let manager = manager::ManagerHandle::test_handle();
    let handle = ClusterPreHandle::new();
    let mut cluster = Cluster::new(handle, manager);

    cluster.add("127.0.0.1:1000".parse().unwrap());
    cluster.add("127.0.0.1:1001".parse().unwrap());
    cluster.add("127.0.0.1:1002".parse().unwrap());

    assert_eq!(cluster.replicas, replicas);
}
