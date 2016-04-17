//! Zone registry, dispatches commands and spawns Zones

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

use node::External;
use path::Path;
use zone::{Zone, ZoneHandle};

#[derive(Clone)]
pub struct ManagerHandle {
    tx: Sender<(Sender<Box<Any + Send>>, ManagerCall)>
}

pub enum ManagerCall {
    FindNearest(Path),
    Find(Path),
    List,
    Load(Path),
    ZoneLoaded(Path)
}

pub struct Manager {
    active: BTreeMap<Path, ZoneHandle>,
    rx: Receiver<(Sender<Box<Any + Send>>, ManagerCall)>,
    tx: Sender<(Sender<Box<Any + Send>>, ManagerCall)>
}

impl ManagerHandle {
    pub fn find(&self, path: &Path) -> Option<ZoneHandle> {
        self.call(ManagerCall::Find(path.clone()))
    }

    pub fn find_nearest(&self, path: &Path) -> (Path, ZoneHandle) {
        self.call(ManagerCall::FindNearest(path.clone()))
    }

    pub fn load(&self, path: &Path) -> ZoneHandle {
        self.call(ManagerCall::Load(path.clone()))
    }

    pub fn send_externals(&self, prefix: &Path, externals: Vec<External>) {
        // TODO: concurrency here is bad
        for mut external in externals {
            // TODO: zone may be remote
            let mut path = prefix.clone();

            path.append(&mut external.path);

            let zone = self.load(&path);

            zone.merge(external.parent_vis, external.node);
        }
    }

    pub fn zone_loaded(&self, path: &Path) -> bool {
        self.call(ManagerCall::ZoneLoaded(path.clone()))
    }

    pub fn list(&self) -> Vec<(Path, ZoneHandle)> {
        self.call(ManagerCall::List)
    }

    /// Generic function to call a function on the underlying Manager through message passing
    fn call<T: Any>(&self, call: ManagerCall) -> T {
        let (tx, rx) = channel();

        self.tx.send((tx, call)).unwrap();

        let result = rx.recv().unwrap();

        *result.downcast::<T>().unwrap()
    }
}

impl Manager {
    pub fn spawn() -> ManagerHandle {
        let manager = Manager::new();
        let handle = manager.handle();

        thread::spawn(move|| {
            manager.message_loop();
        });

        handle
    }

    pub fn new() -> Manager {
        let (tx, rx) = channel();

        Manager { active: BTreeMap::new(), tx: tx, rx: rx }
    }

    fn handle(&self) -> ManagerHandle {
        ManagerHandle { tx: self.tx.clone() }
    }

    fn message_loop(mut self) {
        loop {
            let (reply, call) = self.rx.recv().unwrap();

            let result: Box<Any + Send> = match call {
                ManagerCall::Find(path) => Box::new(self.find(&path)),
                ManagerCall::FindNearest(path) => Box::new(self.find_nearest(&path)),
                ManagerCall::List => Box::new(self.list()),
                ManagerCall::Load(path) => Box::new(self.load(&path)),
                ManagerCall::ZoneLoaded(path) => Box::new(self.zone_loaded(&path))
            };

            reply.send(result).unwrap();
        }
    }

    pub fn load(&mut self, path: &Path) -> ZoneHandle {
        if let Some(zone) = self.active.get(path) {
            return zone.clone();
        }

        let zone = Zone::spawn(self.handle(), path);

        self.active.insert(path.clone(), zone.clone());

        zone
    }

    pub fn zone_loaded(&self, path: &Path) -> bool {
        self.active.contains_key(path)
    }

    /// Find the exact `Zone` specified by `path`
    pub fn find(&self, path: &Path) -> Option<ZoneHandle> {
        self.active.get(path).cloned()
    }

    /// Find the 'closest' `Zone` that would be able to satisfy a call to `path`
    pub fn find_nearest(&self, path: &Path) -> (Path, ZoneHandle) {
        // TODO: probably could be more efficient
        let mut probe = path.clone();

        loop {
            if let Some(found) = self.active.get(&probe) {
                return (probe, found.clone())
            }

            probe.pop(); // crash if no root node
        }
    }

    /// List all active zones
    pub fn list(&self) -> Vec<(Path, ZoneHandle)> {
        self.active.iter().map( |(p, z)| (p.clone(), z.clone()) ).collect()
    }
}

#[test]
fn test_find_nearest() {
    let mut manager = Manager::new();

    let root        = Path::new(vec![]);
    let moo         = Path::new(vec!["moo".into()]);
    let moo_cow     = Path::new(vec!["moo".into(), "cow".into()]);
    let moo_cow_cow = Path::new(vec!["moo".into(), "cow".into(), "cow".into()]);

    manager.load(&root);
    assert_eq!(manager.find_nearest(&moo).0, root);
    assert_eq!(manager.find_nearest(&moo_cow).0, root);
    assert_eq!(manager.find_nearest(&moo_cow_cow).0, root);

    manager.load(&moo_cow);
    assert_eq!(manager.find_nearest(&moo).0, root);
    assert_eq!(manager.find_nearest(&moo_cow).0, moo_cow);
    assert_eq!(manager.find_nearest(&moo_cow_cow).0, moo_cow);

    manager.load(&moo);
    assert_eq!(manager.find_nearest(&moo).0, moo);
    assert_eq!(manager.find_nearest(&moo_cow).0, moo_cow);
    assert_eq!(manager.find_nearest(&moo_cow_cow).0, moo_cow);
}
