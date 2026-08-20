use std::sync::Condvar;
use std::sync::Mutex;

/// Bounds how many bytes of queued write requests may be resident at once.
///
/// The request channel limits the number of queued requests but not their
/// size, so on its own it lets a fast appender hand a slow flush worker an
/// unbounded amount of encoded data.
#[derive(Debug)]
pub(crate) struct QueuedBytes {
    limit: usize,
    used: Mutex<usize>,
    released: Condvar,
}

impl QueuedBytes {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            used: Mutex::new(0),
            released: Condvar::new(),
        }
    }

    /// Blocks until `size` more bytes fit under the limit, then reserves them.
    ///
    /// A request bigger than the whole limit is admitted once nothing else is
    /// queued, because it could never fit otherwise.
    pub(crate) fn acquire(&self, size: usize) {
        let mut used = self.used.lock().unwrap();

        while *used > 0 && *used + size > self.limit {
            used = self.released.wait(used).unwrap();
        }

        *used += size;
    }

    /// Returns `size` reserved bytes and wakes every waiting sender.
    pub(crate) fn release(&self, size: usize) {
        let mut used = self.used.lock().unwrap();
        *used -= size;
        self.released.notify_all();
    }

    /// Returns every reserved byte, for when the worker stops processing.
    pub(crate) fn release_all(&self) {
        let mut used = self.used.lock().unwrap();
        *used = 0;
        self.released.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        *self.used.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use crate::wal::queued_bytes::QueuedBytes;

    #[test]
    fn test_acquire_and_release_track_used_bytes() {
        let queued = QueuedBytes::new(100);

        queued.acquire(30);
        assert_eq!(30, queued.used());

        queued.acquire(70);
        assert_eq!(100, queued.used());

        queued.release(30);
        assert_eq!(70, queued.used());

        queued.release_all();
        assert_eq!(0, queued.used());
    }

    #[test]
    fn test_acquire_admits_a_request_bigger_than_the_limit() {
        let queued = QueuedBytes::new(10);

        queued.acquire(4096);

        assert_eq!(4096, queued.used());
    }

    #[test]
    fn test_acquire_blocks_until_bytes_are_released() {
        let queued = Arc::new(QueuedBytes::new(100));
        queued.acquire(80);

        let releaser = {
            let queued = queued.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                queued.release(80);
            })
        };

        let started_at = Instant::now();
        queued.acquire(60);
        let waited = started_at.elapsed();

        releaser.join().unwrap();
        assert!(waited >= Duration::from_millis(40), "waited {waited:?}");
        assert_eq!(60, queued.used());
    }
}
