use std::collections::BTreeMap;
use std::sync::Arc;

use rakko_core::config::Config;

use crate::details::DetailCache;
use crate::lag::LagHistory;
use crate::runtime::ProfileRuntime;
use crate::samples::SampleCache;
use crate::scan::ScanStore;

pub struct AppState {
    pub profiles: BTreeMap<String, Arc<ProfileRuntime>>,
    pub scans: ScanStore,
    pub samples: SampleCache,
    pub details: DetailCache,
    pub lag: LagHistory,
    /// Offsets per partition per scan step.
    pub scan_window: usize,
}

impl AppState {
    pub fn new(config: Config, scan_window: usize) -> Arc<Self> {
        let profiles = config
            .profiles
            .into_iter()
            .map(|p| (p.name.clone(), Arc::new(ProfileRuntime::new(p))))
            .collect();
        Arc::new(Self {
            profiles,
            scans: ScanStore::default(),
            samples: SampleCache::default(),
            details: DetailCache::default(),
            lag: LagHistory::default(),
            scan_window,
        })
    }

    pub fn profile(&self, name: &str) -> Option<Arc<ProfileRuntime>> {
        self.profiles.get(name).cloned()
    }
}
