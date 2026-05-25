use std::fs::File;
use std::fs::OpenOptions;
use std::io;

use codeq::error_context_ext::ErrorContextExt;
use fs2::FileExt;
use log::info;

use crate::Config;

#[derive(Debug)]
pub struct WalLock {
    path: String,
    f: File,
}

impl WalLock {
    pub(crate) const LOCK_FILE_NAME: &'static str = "LOCK";

    pub(crate) fn new(config: &Config) -> Result<Self, io::Error> {
        let path = Self::lock_path(config);

        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .context(|| format!("create lock file '{}'", path))?;

        f.try_lock_exclusive().map_err(|e| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "Directory '{}' is already locked by another process, \
                    shutdown other process to continue; \
                    error:({})",
                    config.dir, e
                ),
            )
        })?;

        info!("Directory lock acquired: {}", path);

        Ok(Self { path, f })
    }

    pub(crate) fn lock_path(config: &Config) -> String {
        format!("{}/{}", config.dir, Self::LOCK_FILE_NAME)
    }
}

impl Drop for WalLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.f);
        info!("Directory lock released: {}", self.path);
    }
}

#[cfg(test)]
mod tests {
    use crate::Config;
    use crate::WalLock;

    #[test]
    fn test_lock_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config::new(temp_dir.path().to_str().unwrap());

        let lf = WalLock::new(&config).unwrap();
        println!("Directory locked successfully");

        let lf2 = WalLock::new(&config);
        assert!(lf2.is_err());

        drop(lf);
        let _lf2 = WalLock::new(&config).unwrap();
        println!("Directory locked successfully after dropping first lock");
    }
}
