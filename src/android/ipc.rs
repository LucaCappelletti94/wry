// Copyright 2020-2023 Tauri Programme within The Commons Conservancy
// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: MIT

//! Runs each webview's IPC handler on the looper thread that created the webview.

use std::{
  collections::{HashMap, VecDeque},
  io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write},
  os::{fd::AsFd, unix::net::UnixStream},
  sync::{Arc, LazyLock, Mutex},
};

use http::Request;
use ndk::looper::{FdEvent, ThreadLooper};

static ROUTES: LazyLock<Mutex<HashMap<String, Arc<Route>>>> = LazyLock::new(Default::default);

#[derive(Default)]
struct Queue {
  closed: bool,
  requests: VecDeque<Request<String>>,
}

struct Route {
  queue: Mutex<Queue>,
  /// Written by Java threads.
  signal: UnixStream,
  /// Polled by the owner looper.
  wait: UnixStream,
}

impl Route {
  fn new() -> IoResult<Self> {
    let (signal, wait) = UnixStream::pair()?;
    signal.set_nonblocking(true)?;
    wait.set_nonblocking(true)?;
    Ok(Self {
      queue: Mutex::default(),
      signal,
      wait,
    })
  }

  fn wake(&self) -> IoResult<()> {
    loop {
      match (&self.signal).write(&[1]) {
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == ErrorKind::Interrupted => continue,
        // A full buffer means the owner is already signalled.
        Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(()),
        Err(error) => return Err(error),
      }
    }
  }

  fn drain(&self) {
    let mut buffer = [0_u8; 64];
    loop {
      match (&self.wait).read(&mut buffer) {
        Ok(0) => return,
        Ok(_) => continue,
        Err(error) if error.kind() == ErrorKind::Interrupted => continue,
        Err(_) => return,
      }
    }
  }

  /// Cancels queued requests and signals the owner to drop the handler.
  fn close(&self) {
    {
      let mut queue = self.queue.lock().unwrap();
      queue.closed = true;
      queue.requests.clear();
    }
    if let Err(_error) = self.wake() {
      #[cfg(feature = "tracing")]
      tracing::warn!("Failed to signal Android IPC teardown: {_error}");
    }
  }

  /// Runs the handler on every queued request and returns whether the route is still open.
  fn deliver(&self, handler: &dyn Fn(Request<String>)) -> bool {
    loop {
      let request = {
        let mut queue = self.queue.lock().unwrap();
        if queue.closed {
          return false;
        }
        match queue.requests.pop_front() {
          Some(request) => request,
          None => return true,
        }
      };
      #[cfg(feature = "tracing")]
      let _span = tracing::info_span!(parent: None, "wry::ipc::handle").entered();
      handler(request);
    }
  }
}

/// Routes requests for `id` to `handler` on the thread that owns `looper`, replacing any
/// previous handler for `id`.
pub(super) fn register(
  id: &str,
  looper: &ThreadLooper,
  handler: Box<dyn Fn(Request<String>)>,
) -> IoResult<()> {
  let route = Arc::new(Route::new()?);
  let owned = route.clone();
  let failed = FdEvent::ERROR | FdEvent::HANGUP | FdEvent::INVALID;
  looper
    .add_fd_with_callback(route.wait.as_fd(), FdEvent::INPUT, move |_, events| {
      owned.drain();
      if !events.intersects(failed) && owned.deliver(&*handler) {
        return true;
      }
      owned.close();
      // Remove the fd while `owned` still holds it open, since ndk drops the closure on return.
      if let Some(looper) = ThreadLooper::for_thread() {
        let _ = looper.as_foreign().remove_fd(owned.wait.as_fd());
      }
      false
    })
    .map_err(|_| IoError::other("Failed to register the Android IPC wake socket"))?;
  let replaced = ROUTES.lock().unwrap().insert(id.to_owned(), route);
  if let Some(replaced) = replaced {
    replaced.close();
  }
  Ok(())
}

/// Queues `request` for the handler registered under `id`.
pub(super) fn dispatch(id: &str, request: Request<String>) -> IoResult<()> {
  let route = ROUTES.lock().unwrap().get(id).cloned().ok_or_else(|| {
    IoError::new(
      ErrorKind::NotFound,
      "No Android IPC handler for this webview",
    )
  })?;
  {
    let mut queue = route.queue.lock().unwrap();
    if queue.closed {
      return Err(IoError::new(
        ErrorKind::ConnectionAborted,
        "The Android IPC handler was removed",
      ));
    }
    queue.requests.push_back(request);
  }
  route.wake()
}

/// Cancels queued requests for `id` and drops its handler on the owner thread.
pub(super) fn destroy(id: &str) {
  let route = ROUTES.lock().unwrap().remove(id);
  if let Some(route) = route {
    route.close();
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::{
    cell::Cell,
    rc::Rc,
    sync::{
      atomic::{AtomicBool, Ordering},
      mpsc::{Receiver, Sender, channel},
    },
    thread::{self, JoinHandle, ThreadId},
    time::{Duration, Instant},
  };

  const WAIT: Duration = Duration::from_secs(5);

  fn request(body: &str) -> Request<String> {
    Request::builder()
      .uri("http://ipc.test/")
      .body(body.to_owned())
      .unwrap()
  }

  fn recv<T>(receiver: &Receiver<T>) -> T {
    receiver.recv_timeout(WAIT).expect("owner thread responds")
  }

  /// Reports the thread that drops it, held through an `Rc` so handlers stay non-`Send`.
  struct DropProbe(Sender<ThreadId>);

  impl Drop for DropProbe {
    fn drop(&mut self) {
      let _ = self.0.send(thread::current().id());
    }
  }

  /// A thread that runs `setup` and then polls its looper until joined.
  struct Owner {
    id: ThreadId,
    stop: Arc<AtomicBool>,
    thread: JoinHandle<()>,
  }

  impl Owner {
    fn spawn(setup: impl FnOnce(&ThreadLooper) + Send + 'static) -> Self {
      let stop = Arc::new(AtomicBool::new(false));
      let stopped = stop.clone();
      let (ready, ready_rx) = channel();
      let thread = thread::spawn(move || {
        let looper = ThreadLooper::prepare();
        setup(&looper);
        ready.send(thread::current().id()).unwrap();
        let deadline = Instant::now() + WAIT;
        while !stopped.load(Ordering::Acquire) {
          assert!(
            Instant::now() < deadline,
            "owner loop exceeded its deadline"
          );
          let _ = looper.poll_once_timeout(Duration::from_millis(10));
        }
      });
      let id = recv(&ready_rx);
      Self { id, stop, thread }
    }

    fn join(self) {
      self.stop.store(true, Ordering::Release);
      self.thread.join().expect("owner thread exits cleanly");
    }
  }

  fn recording_handler(
    probe: Sender<ThreadId>,
    delivered: Sender<(ThreadId, String)>,
  ) -> Box<dyn Fn(Request<String>)> {
    let probe = Rc::new(DropProbe(probe));
    Box::new(move |request| {
      let _ = &probe;
      delivered
        .send((thread::current().id(), request.into_body()))
        .unwrap();
    })
  }

  #[test]
  fn requests_reach_their_own_handler_in_order_on_the_owner_thread() {
    let (alpha_tx, alpha_rx) = channel();
    let (beta_tx, beta_rx) = channel();
    let (probe, _dropped) = channel();
    let owner = Owner::spawn(move |looper| {
      register(
        "ipc-test-order-alpha",
        looper,
        recording_handler(probe.clone(), alpha_tx),
      )
      .unwrap();
      register(
        "ipc-test-order-beta",
        looper,
        recording_handler(probe, beta_tx),
      )
      .unwrap();
    });

    for (id, body) in [
      ("ipc-test-order-alpha", "a1"),
      ("ipc-test-order-beta", "b1"),
      ("ipc-test-order-alpha", "a2"),
    ] {
      dispatch(id, request(body)).unwrap();
    }

    assert_eq!(recv(&alpha_rx), (owner.id, "a1".to_owned()));
    assert_eq!(recv(&alpha_rx), (owner.id, "a2".to_owned()));
    assert_eq!(recv(&beta_rx), (owner.id, "b1".to_owned()));
    destroy("ipc-test-order-alpha");
    destroy("ipc-test-order-beta");
    owner.join();
  }

  #[test]
  fn destroy_drops_the_handler_on_the_owner_thread_and_closes_the_route() {
    let id = "ipc-test-destroy";
    let (delivered, delivered_rx) = channel();
    let (probe, dropped) = channel();
    let owner = Owner::spawn(move |looper| {
      register(id, looper, recording_handler(probe, delivered)).unwrap();
    });

    dispatch(id, request("ping")).unwrap();
    assert_eq!(recv(&delivered_rx).1, "ping");
    destroy(id);

    assert_eq!(recv(&dropped), owner.id);
    assert!(dispatch(id, request("late")).is_err());
    owner.join();
  }

  #[test]
  fn destroy_cancels_requests_queued_behind_a_running_handler() {
    let id = "ipc-test-cancel";
    let (started, started_rx) = channel();
    let (release, release_rx) = channel::<()>();
    let (delivered, delivered_rx) = channel();
    let (probe, dropped) = channel();
    let owner = Owner::spawn(move |looper| {
      let probe = Rc::new(DropProbe(probe));
      let handler = move |request: Request<String>| {
        let _ = &probe;
        let body = request.into_body();
        if body == "hold" {
          started.send(()).unwrap();
          release_rx.recv_timeout(WAIT).unwrap();
        }
        delivered.send(body).unwrap();
      };
      register(id, looper, Box::new(handler)).unwrap();
    });

    dispatch(id, request("hold")).unwrap();
    recv(&started_rx);
    dispatch(id, request("queued")).unwrap();
    destroy(id);
    release.send(()).unwrap();

    assert_eq!(recv(&dropped), owner.id);
    assert_eq!(delivered_rx.try_iter().collect::<Vec<_>>(), ["hold"]);
    owner.join();
  }

  #[test]
  fn replacement_drops_the_old_handler_and_cancels_its_queue() {
    let id = "ipc-test-replace";
    let (started, started_rx) = channel();
    let (release, release_rx) = channel::<()>();
    let (old_delivered, old_rx) = channel();
    let (new_delivered, new_rx) = channel();
    let (old_probe, old_dropped) = channel();
    let (new_probe, new_dropped) = channel();
    let owner = Owner::spawn(move |looper| {
      let probe = Rc::new(DropProbe(old_probe));
      let replacement = Cell::new(Some(recording_handler(new_probe, new_delivered)));
      let handler = move |request: Request<String>| {
        let _ = &probe;
        let body = request.into_body();
        if body == "replace" {
          started.send(()).unwrap();
          release_rx.recv_timeout(WAIT).unwrap();
          let looper = ThreadLooper::for_thread().unwrap();
          register(id, &looper, replacement.take().unwrap()).unwrap();
        }
        old_delivered.send(body).unwrap();
      };
      register(id, looper, Box::new(handler)).unwrap();
    });

    dispatch(id, request("replace")).unwrap();
    recv(&started_rx);
    dispatch(id, request("stale")).unwrap();
    release.send(()).unwrap();
    assert_eq!(recv(&old_dropped), owner.id);

    dispatch(id, request("fresh")).unwrap();
    assert_eq!(recv(&new_rx), (owner.id, "fresh".to_owned()));
    assert_eq!(old_rx.try_iter().collect::<Vec<_>>(), ["replace"]);

    destroy(id);
    assert_eq!(recv(&new_dropped), owner.id);
    owner.join();
  }
}
