use std::fmt;
use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;

/// Shared slot holding a chunk's file handle.
///
/// A chunk's in-memory state can exist before its file does: when a full
/// chunk is closed, the successor chunk is constructed immediately on the
/// caller thread, while its file is created later by the flush worker, after
/// the predecessor chunk has been made durable. The worker fills the slot
/// once the file exists on disk.
///
/// Cloning a `FileSlot` shares the underlying slot, so every clone of a
/// chunk observes the file handle once it is published.
#[derive(Clone)]
pub(crate) struct FileSlot {
    inner: Arc<Slot>,
}

struct Slot {
    state: Mutex<SlotState>,
    filled: Condvar,
}

enum SlotState {
    /// The file has not been created yet.
    Pending,
    /// The file exists and the handle is available.
    Ready(Arc<File>),
    /// The file will never be created; holds the causing error.
    Failed(io::ErrorKind, String),
}

impl FileSlot {
    /// Creates a slot that already holds a file handle.
    pub(crate) fn ready(f: Arc<File>) -> Self {
        Self::with_state(SlotState::Ready(f))
    }

    /// Creates an empty slot to be filled later via [`FileSlot::set`].
    pub(crate) fn pending() -> Self {
        Self::with_state(SlotState::Pending)
    }

    fn with_state(state: SlotState) -> Self {
        Self {
            inner: Arc::new(Slot {
                state: Mutex::new(state),
                filled: Condvar::new(),
            }),
        }
    }

    /// Publishes the file handle and wakes all waiting readers.
    pub(crate) fn set(&self, f: Arc<File>) {
        let mut state = self.inner.state.lock().unwrap();
        debug_assert!(
            matches!(*state, SlotState::Pending),
            "FileSlot must be set at most once"
        );
        *state = SlotState::Ready(f);
        self.inner.filled.notify_all();
    }

    /// Marks the slot as permanently unavailable and wakes all waiting
    /// readers, which then observe the error instead of blocking forever.
    ///
    /// Has no effect if the handle was already published.
    pub(crate) fn fail(&self, err: &io::Error) {
        let mut state = self.inner.state.lock().unwrap();
        if matches!(*state, SlotState::Pending) {
            *state = SlotState::Failed(err.kind(), err.to_string());
            self.inner.filled.notify_all();
        }
    }

    /// Returns the file handle if it is already available.
    pub(crate) fn get(&self) -> Option<Arc<File>> {
        match &*self.inner.state.lock().unwrap() {
            SlotState::Ready(f) => Some(f.clone()),
            _ => None,
        }
    }

    /// Blocks until the file handle is available and returns it.
    ///
    /// The wait is bounded: the flush worker creates the file after
    /// processing a finite prefix of its FIFO queue. If the worker fails
    /// before creating the file, the slot is poisoned and this returns the
    /// worker's error.
    pub(crate) fn wait(&self) -> Result<Arc<File>, io::Error> {
        let mut state = self.inner.state.lock().unwrap();
        loop {
            match &*state {
                SlotState::Ready(f) => return Ok(f.clone()),
                SlotState::Failed(kind, msg) => {
                    return Err(io::Error::new(
                        *kind,
                        format!("chunk file was never created: {}", msg),
                    ));
                }
                SlotState::Pending => {
                    state = self.inner.filled.wait(state).unwrap();
                }
            }
        }
    }
}

impl fmt::Debug for FileSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self.inner.state.lock().unwrap() {
            SlotState::Pending => f.write_str("FileSlot(Pending)"),
            SlotState::Ready(file) => {
                f.debug_tuple("FileSlot").field(file).finish()
            }
            SlotState::Failed(kind, msg) => f
                .debug_struct("FileSlot")
                .field("failed_kind", kind)
                .field("failed_error", msg)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use crate::chunk::file_slot::FileSlot;

    #[test]
    fn test_ready_slot_returns_file() -> Result<(), io::Error> {
        let f = Arc::new(tempfile::tempfile()?);
        let slot = FileSlot::ready(f.clone());

        assert!(Arc::ptr_eq(&f, &slot.get().unwrap()));
        assert!(Arc::ptr_eq(&f, &slot.wait()?));

        Ok(())
    }

    #[test]
    fn test_pending_slot_wakes_waiter_on_set() -> Result<(), io::Error> {
        let slot = FileSlot::pending();
        assert!(slot.get().is_none());

        let waiter = {
            let slot = slot.clone();
            std::thread::spawn(move || slot.wait())
        };

        let f = Arc::new(tempfile::tempfile()?);
        slot.set(f.clone());

        let got = waiter.join().unwrap()?;
        assert!(Arc::ptr_eq(&f, &got));

        Ok(())
    }

    #[test]
    fn test_failed_slot_returns_error() {
        let slot = FileSlot::pending();
        slot.fail(&io::Error::new(io::ErrorKind::StorageFull, "no space"));

        let err = slot.wait().unwrap_err();
        assert_eq!(io::ErrorKind::StorageFull, err.kind());
        assert!(err.to_string().contains("no space"));
        assert!(slot.get().is_none());
    }

    #[test]
    fn test_fail_does_not_override_ready() -> Result<(), io::Error> {
        let f = Arc::new(tempfile::tempfile()?);
        let slot = FileSlot::ready(f.clone());
        slot.fail(&io::Error::other("late failure"));

        assert!(Arc::ptr_eq(&f, &slot.wait()?));

        Ok(())
    }
}
