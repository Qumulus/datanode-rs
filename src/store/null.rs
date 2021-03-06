//! A null store that loads emppty data and ignores writes. For test use only

use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

use super::*;
use path::Path;
use zone::ZoneHandle;

pub struct Null {
    rx: Receiver<StoreCall>,
    tx: Sender<StoreCall>,
}

impl Null {
    /// Start the Store "process".
    pub fn spawn() -> StoreHandle {
        let store = Null::new();
        let handle = store.handle();

        thread::spawn(move|| {
            store.message_loop();
        });

        handle
    }

    pub fn new() -> Null {
        let (tx, rx) = channel();

        Null { tx: tx, rx: rx }
    }

    /// Return a handle to Store "process".
    fn handle(&self) -> StoreHandle {
        StoreHandle { tx: self.tx.clone() }
    }

    fn message_loop(self) {
        loop {
            let call = self.rx.recv().unwrap();

            match call {
                StoreCall::List(reply) => self.list(reply),
                StoreCall::Load(zone, path) => self.load(zone, &path),
                StoreCall::LoadData(path, tx) => self.load_data(path, tx),
                StoreCall::RequestWrite(zone) => self.request_write(zone),
                StoreCall::Write(zone, path, data) => self.write(zone, &path, &data)
            }
        }
    }

    /// Lists all Zone Paths stored locally
    pub fn list(&self, _: Sender<Path>) {
    }

    /// Loads data for a `Zone` asynchronously, notifying its handle when done. Will always load an
    /// empty data set.
    pub fn load(&self, zone: ZoneHandle, _: &Path) {
        zone.loaded(Default::default());
    }

    /// Asynchronously load and send `ZoneData` for `Path` to channel.
    pub fn load_data(&self, _path: Path, tx: Sender<Option<ZoneData>>) {
        tx.send(Some(Default::default())).is_ok(); // ignore if caller goes away
    }

    /// Request for notification to write data. Never gonna happen.
    pub fn request_write(&self, _: ZoneHandle) {
    }

    /// Write data for a `Zone` asynchronously, notifying its handle when done.
    /// Not happening either.
    pub fn write(&self, _: ZoneHandle, _: &Path, _: &Vec<u8>) {
    }

}
