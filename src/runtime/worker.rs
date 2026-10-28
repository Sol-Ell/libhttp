use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use mio::Waker;

use super::slab::ConnId;

/// Wakes a reactor and queues a connection for maintenance.
pub(crate) struct ReactorWaker {
    waker: Waker,
    ready: Mutex<VecDeque<ConnId>>,
}

impl ReactorWaker {
    pub(crate) fn new(waker: Waker) -> Self {
        Self {
            waker,
            ready: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn wake(&self) {
        let _ = self.waker.wake();
    }

    pub(crate) fn wake_conn(&self, conn: ConnId) {
        self.ready.lock().unwrap().push_back(conn);
        let _ = self.waker.wake();
    }

    pub(crate) fn drain_ready(&self) -> Vec<ConnId> {
        let mut ready = self.ready.lock().unwrap();
        ready.drain(..).collect()
    }
}

pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

/// Creates the shared queue used by reactors and the worker pool.
pub(crate) fn channel(capacity: usize) -> (Sender<Job>, Receiver<Job>) {
    bounded(capacity.max(1))
}

/// A bounded worker pool with a single shared queue.
pub(crate) struct WorkerPool {
    handles: Mutex<Vec<JoinHandle<()>>>,
    shutdown: AtomicBool,
}

impl WorkerPool {
    pub(crate) fn new(
        workers: usize,
        receiver: Receiver<Job>,
        wakers: Vec<std::sync::Arc<ReactorWaker>>,
    ) -> Self {
        let workers = workers.max(1);
        let mut handles = Vec::with_capacity(workers);
        for index in 0..workers {
            let receiver = receiver.clone();
            let wakers = wakers.clone();
            let handle = std::thread::Builder::new()
                .name(format!("libhttp-worker-{index}"))
                .spawn(move || {
                    while let Ok(job) = receiver.recv() {
                        let _ = catch_unwind(AssertUnwindSafe(job));
                        for waker in &wakers {
                            waker.wake();
                        }
                    }
                })
                .expect("spawn worker thread");
            handles.push(handle);
        }
        Self {
            handles: Mutex::new(handles),
            shutdown: AtomicBool::new(false),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub(crate) fn join(&self) {
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.join();
    }
}

pub(crate) fn try_dispatch(sender: &Sender<Job>, job: Job) -> Result<(), Job> {
    match sender.try_send(job) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(job)) | Err(TrySendError::Disconnected(job)) => Err(job),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[test]
    fn runs_jobs_on_worker_threads() {
        let (tx, rx) = channel(8);
        let pool = WorkerPool::new(1, rx, Vec::new());
        let done = Arc::new(AtomicUsize::new(0));
        let task_done = done.clone();
        assert!(
            try_dispatch(
                &tx,
                Box::new(move || {
                    task_done.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .is_ok()
        );
        for _ in 0..100 {
            if done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(done.load(Ordering::SeqCst), 1);
        drop(tx);
        pool.join();
    }

    #[test]
    fn full_queue_returns_the_job() {
        let (tx, rx) = channel(1);
        let pool = WorkerPool::new(1, rx, Vec::new());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        assert!(
            try_dispatch(
                &tx,
                Box::new(move || {
                    started_tx.send(()).unwrap();
                    let _ = release_rx.recv();
                }),
            )
            .is_ok()
        );
        // Wait until the first job is running, then the queue is empty again; enqueue one
        // blocker and verify the next one is rejected.
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(try_dispatch(&tx, Box::new(|| {})).is_ok());
        let second = try_dispatch(&tx, Box::new(|| {}));
        assert!(second.is_err());
        release_tx.send(()).unwrap();
        drop(tx);
        pool.join();
    }

    #[test]
    fn panic_in_a_job_does_not_kill_the_worker() {
        let (tx, rx) = channel(4);
        let pool = WorkerPool::new(1, rx, Vec::new());
        assert!(try_dispatch(&tx, Box::new(|| panic!("boom"))).is_ok());
        let done = Arc::new(AtomicUsize::new(0));
        let task_done = done.clone();
        assert!(
            try_dispatch(
                &tx,
                Box::new(move || {
                    task_done.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .is_ok()
        );
        for _ in 0..100 {
            if done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(done.load(Ordering::SeqCst), 1);
        drop(tx);
        pool.join();
    }
}
