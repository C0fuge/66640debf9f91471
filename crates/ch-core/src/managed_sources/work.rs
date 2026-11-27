//! Bounded actual blocking work; no caller, path, filesystem or source authority.
use super::Error;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::{sync::{Semaphore, oneshot}, task::JoinHandle};

const LIMIT: u32 = 4;
#[derive(Default)]
struct State { closed: bool, jobs: Vec<JoinHandle<()>> }

pub(crate) struct OwnedJobs { state: Mutex<State>, slots: Arc<Semaphore> }

impl OwnedJobs {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { state: Mutex::default(), slots: Arc::new(Semaphore::new(LIMIT as usize)) })
    }

    pub(crate) async fn run<R, F>(&self, work: F) -> Result<R, Error>
    where R: Send + 'static, F: FnOnce() -> R + Send + 'static {
        let (send, recv) = oneshot::channel();
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.closed { return Err(Error::Closed); }
            let slot = Arc::clone(&self.slots).try_acquire_owned().map_err(|_| Error::Busy)?;
            state.jobs.retain(|job| !job.is_finished());
            state.jobs.push(tokio::task::spawn_blocking(move || {
                let _slot = slot;
                let result = work();
                let _ = send.send(result);
            }));
        }
        recv.await.map_err(|_| Error::Task)
    }

    pub(crate) fn close(&self) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).closed = true;
    }

    pub(crate) async fn drain(&self) -> Result<(), Error> {
        let jobs = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.closed = true;
            std::mem::take(&mut state.jobs)
        };
        let mut failed = false;
        for job in jobs { failed |= job.await.is_err(); }
        // Covers a previously cancelled drainer; never close the semaphore or
        // interpret an acquire failure as proof that blocking work has settled.
        let _all = Arc::clone(&self.slots).acquire_many_owned(LIMIT).await.map_err(|_| Error::Task)?;
        if failed { Err(Error::Task) } else { Ok(()) }
    }
}
