use crate::app::config::RexConfig;
use anyhow::Error;
use parking_lot::{RwLock, RwLockReadGuard};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct ConfigManager {
    path: PathBuf,
    cfg: Arc<RwLock<RexConfig>>,
    started: AtomicBool,
}

/// Loads and maybe in the future watches
/// the local cfg file for changes.. this lets us do that
/// if wanted
impl ConfigManager {

    fn reload(&self) -> Result<(), Error> {
        let cfg = RexConfig::load(&self.path)?;
        *self.cfg.write() = cfg;

        Ok(())
    }

    pub fn new(cfg_path: PathBuf) -> ConfigManager {
        ConfigManager {
            path: cfg_path,
            cfg: Arc::new(RwLock::new(RexConfig::default())),
            started: AtomicBool::new(false),
        }
    }

    /// Loads initial config
    /// TODO an ongoing config watcher for testing?
    pub fn start(&self) -> Result<(), Error> {
        self.reload()?;
        self.started.store(true, Ordering::Release);

        Ok(())
    }

    /// Get an immutable read for the current config
    pub fn get(&self) -> RwLockReadGuard<'_, RexConfig> {
        if !self.started.load(Ordering::Acquire) {
            panic!("ConfigManager not started yet but fetching config");
        }

        self.cfg.read()
    }
}