use std::{sync::{Arc, Mutex}, collections::{HashMap, BTreeSet}, time::Duration};

use bytes::Bytes;
use tokio::{sync::{Notify, broadcast}, time::Instant};

#[derive(Debug)]
struct Entry {
  data: Bytes,
  expires_at: Option<Instant>,
}

#[derive(Debug)]
struct State {
  entries: HashMap<String, Entry>,
  pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
  expirations: BTreeSet<(Instant, String)>,
  shutdown: bool,
}

#[derive(Debug)]
struct Shared {
  state: Mutex<State>,
  background_task: Notify
}

#[derive(Debug, Clone)]
pub struct Db {
  shared: Arc<Shared>,
}

impl Db {
  pub fn new() -> Db {
    let shared = Arc::new(Shared {
      state: Mutex::new(State {
        entries: HashMap::new(),
        pub_sub: HashMap::new(),
        expirations: BTreeSet::new(),
        shutdown: false
      }),
      background_task: Notify::new(),
    });

    tokio::spawn(purge_expired_tasks(shared.clone()));

    Db { shared }
  }

  pub fn get(&self, key: &str) -> Option<Bytes> {
    let state = self.shared.state.lock().unwrap();
    state.entries.get(key).map(|entry| entry.data.clone())
  }

  pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
    let mut state = self.shared.state.lock().unwrap();
    let mut notify = false;

    let expires_at = expire.map(|duration| {
      let when = Instant::now() + duration;

      notify = state.next_expiration().map(|expiration| expiration > when).unwrap_or(true);
      state.expirations.insert((when, key.clone()));
      when
    });

    let prev = state.entries.insert(key.clone(), Entry {
      data: value,
      expires_at,
    });

    if let Some(prev) = prev {
      if let Some(when) = prev.expires_at {
        state.expirations.remove(&(when, key));
      }
    }

    drop(state);

    if notify {
      self.shared.background_task.notify_one();
    }
  }

  pub fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
    use std::collections::hash_map::Entry;

    let mut state = self.shared.state.lock().unwrap();
    match state.pub_sub.entry(key) {
        Entry::Occupied(e) => e.get().subscribe(),
        Entry::Vacant(e) => {
          let (tx, rx) = broadcast::channel(1024);
          e.insert(tx);
          rx
        },
    }
  }

  pub fn publish(&self, key: &str, value: Bytes) -> usize {
    let state = self.shared.state.lock().unwrap();

    state.pub_sub.get(key).map(|tx| tx.send(value).unwrap_or(0)).unwrap_or(0)
  }

  fn shutdown_purge_task(&self) {
    let mut state = self.shared.state.lock().unwrap();
    state.shutdown = true;

    drop(state);
    self.shared.background_task.notify_one();
  }
}

impl Shared {
  fn purge_expired_keys(&self) -> Option<Instant> {
    let mut state = self.state.lock().unwrap();

    if state.shutdown {
      return None;
    }

    let state = &mut *state;
    let now = Instant::now();

    while let Some(&(when, ref key)) = state.expirations.iter().next() {
      if when > now {
        return Some(when);
      }

      state.entries.remove(key);
      state.expirations.remove(&(when, key.clone()));
    }

    None
  }
}