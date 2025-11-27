use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
type Version = u64;
#[derive(Debug)]
pub enum KVError {
    TransactionAborted,
}
type Result<T> = std::result::Result<T, KVError>;
pub struct KVStore {
    data: Mutex<BTreeMap<String, BTreeMap<Version, Option<String>>>>,
    next_version: AtomicU64,
    active_version: AtomicU64,
}
impl KVStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(BTreeMap::new()),
            next_version: AtomicU64::new(1),
            active_version: AtomicU64::new(0),
        })
    }
    pub fn begin(self: &Arc<Self>) -> Transaction {
        let snapshot = self.active_version.load(Ordering::Acquire);
        Transaction {
            store: Arc::clone(self),
            snapshot_version: snapshot,
            writes: BTreeMap::new(),
            aborted: false,
        }
    }
    fn read_at(&self, key: &str, version: Version) -> Option<String> {
        let data = self.data.lock().unwrap();
        let versions = data.get(key)?;
        versions
            .iter()
            .rev()
            .find(|&(v, _)| *v <= version)
            .and_then(|(_, v)| v.clone())
    }
}
pub struct Transaction {
    store: Arc<KVStore>,
    snapshot_version: Version,
    writes: BTreeMap<String, Option<String>>,
    aborted: bool,
}
impl Transaction {
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        if self.aborted {
            return Err(KVError::TransactionAborted);
        }
        if let Some(val) = self.writes.get(key) {
            return Ok(val.clone());
        }
        Ok(self.store.read_at(key, self.snapshot_version))
    }
    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        if self.aborted {
            return Err(KVError::TransactionAborted);
        }
        self.writes.insert(key, Some(value));
        Ok(())
    }
    pub fn delete(&mut self, key: String) -> Result<()> {
        if self.aborted {
            return Err(KVError::TransactionAborted);
        }
        self.writes.insert(key, None);
        Ok(())
    }
    pub fn commit(self) -> Result<()> {
        if self.aborted || self.writes.is_empty() {
            return Ok(());
        }

        let new_version = self.store.next_version.fetch_add(1, Ordering::AcqRel);

        {
            let mut data = self.store.data.lock().unwrap();
            for (key, value_opt) in self.writes {
                let entry = data.entry(key).or_default();
                entry.insert(new_version, value_opt);
            }
        }
        self.store.active_version.store(new_version, Ordering::Release);
        Ok(())
    }
    pub fn rollback(&mut self) -> Result<()> {
        self.writes.clear();
        self.aborted = true;
        Ok(())
    }
}
fn main() {
    let store = KVStore::new();
    let store1 = Arc::clone(&store);
    let t1 = thread::spawn(move || {
        let mut tx = store1.begin();
        tx.put("name".to_owned(), "Hlib Voznenko".to_owned()).unwrap();
        tx.put("code".to_owned(), "044-off".to_owned()).unwrap();
        thread::sleep(Duration::from_millis(200));
        tx.commit().unwrap();
    });
    let store2 = Arc::clone(&store);
    let t2 = thread::spawn(move || {
        let tx = store2.begin();
        thread::sleep(Duration::from_millis(50));
        println!("Читання до коміту — name: {:?}", tx.get("name").unwrap());
        println!("Читання до коміту — code: {:?}", tx.get("code").unwrap());
    });
    t1.join().unwrap();
    t2.join().unwrap();

    let final_tx = store.begin();
    println!("Фінальний стан сховища:");
    println!("  name = {:?}", final_tx.get("name").unwrap());
    println!("  code = {:?}", final_tx.get("code").unwrap());
}