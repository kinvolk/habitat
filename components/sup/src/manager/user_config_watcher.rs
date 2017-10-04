// Copyright (c) 2017 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::Builder as ThreadBuilder;

use super::file_watcher::{Callbacks, default_file_watcher};

use hcore::fs::USER_CONFIG_FILE;
use hcore::service::ServiceGroup;
use manager::service::Service;

static LOGKEY: &'static str = "UCM";

// This trait exists to ease the testing of functions that receive a Service.  Creating Services
// requires a lot of ceremony, so we work around this with this trait.
pub trait Serviceable {
    fn name(&self) -> &String;
    fn path(&self) -> &PathBuf;
    fn service_group(&self) -> &ServiceGroup;
}

impl Serviceable for Service {
    fn name(&self) -> &String {
        &self.pkg.name
    }

    fn path(&self) -> &PathBuf {
        &self.pkg.svc_path
    }

    fn service_group(&self) -> &ServiceGroup {
        &self.service_group
    }
}


pub struct UserConfigWatcher {
    // TODO use a HashSet? we don't really need the boolean:
    // if the key is present, it's true, otherwise it's false.
    states: HashMap<String, Arc<AtomicBool>>,
}

impl UserConfigWatcher {
    pub fn new() -> Self {
        Self { states: HashMap::new() }
    }

    pub fn add<T: Serviceable>(&mut self, service: &T) {
        self.states.entry(service.name().clone()).or_insert_with(
            || {
                let have_events = Arc::new(AtomicBool::new(false));
                Worker::run(&service.path(), Arc::clone(&have_events));

                have_events
            },
        );
        outputln!(preamble service.service_group(), "Watching {}", USER_CONFIG_FILE);
    }

    pub fn have_events_for<T: Serviceable>(&self, service: &T) -> bool {
        if let Some(val) = self.states.get(service.name()) {
            let val = val.load(Ordering::Relaxed);

            val
        } else {
            false
        }
    }

    pub fn reset_events_for<T: Serviceable>(&mut self, service: &T) {
        if let Some(val) = self.states.get_mut(service.name()) {
            val.store(false, Ordering::Relaxed);
        }
    }
}

struct UserConfigCallbacks {
    have_events: Arc<AtomicBool>,
}

impl Callbacks for UserConfigCallbacks {
    fn file_appeared(&mut self, _: &Path) {
        self.have_events.store(true, Ordering::Relaxed);
    }

    fn file_modified(&mut self, _: &Path) {
        self.have_events.store(true, Ordering::Relaxed);
    }

    fn file_disappeared(&mut self, _: &Path) {
        self.have_events.store(true, Ordering::Relaxed);
    }
}

struct Worker;

impl Worker {
    // starts a new thread with the file watcher on the service's user.toml file
    pub fn run(service_path: &Path, have_events: Arc<AtomicBool>) {
        let path = service_path.join(USER_CONFIG_FILE);
        Self::setup_watcher(path, have_events);
    }

    fn setup_watcher(path: PathBuf, have_events: Arc<AtomicBool>) {
        ThreadBuilder::new()
            .name(format!("user-config-watcher-{}", path.display()))
            .spawn(move || {
                debug!(
                    "UserConfigWatcher({}) worker thread starting",
                    path.display()
                );
                let callbacks = UserConfigCallbacks { have_events: have_events };
                let mut file_watcher = match default_file_watcher(&path, callbacks) {
                    Ok(w) => w,
                    Err(e) => {
                        outputln!(
                            "UserConfigWatcher({}) could not start notifier, ending thread ({})",
                            path.display(),
                            e
                        );
                        return;
                    }
                };

                if let Err(e) = file_watcher.run() {
                    outputln!(
                        "UserConfigWatcher({}) could not run notifier, ending thread ({})",
                        path.display(),
                        e
                    );
                    return;
                };
            })
            .expect("starting user-config-watcher worker thread");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::{remove_file, File};
    use std::io::Write;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::thread;
    use std::time::{Duration, Instant};

    use tempdir::TempDir;

    #[test]
    fn no_events_at_first() {
        let service = TestService::default();
        let mut ucm = UserConfigWatcher::new();
        ucm.add(&service);

        assert!(!ucm.have_events_for(&service));
    }

    #[test]
    fn events_present_after_adding_config() {
        let service = TestService::default();
        let mut ucm = UserConfigWatcher::new();
        ucm.add(&service);

        File::create(service.path().join(USER_CONFIG_FILE)).expect("creating file");

        assert!(wait_for_events(&ucm, &service));
    }

    #[test]
    fn events_present_after_changing_config() {
        let service = TestService::default();
        let file_path = service.path().join(USER_CONFIG_FILE);
        let mut ucm = UserConfigWatcher::new();

        ucm.add(&service);
        let mut file = File::create(&file_path).expect("creating file");

        ucm.reset_events_for(&service);

        file.write_all(b"42").expect(USER_CONFIG_FILE);

        assert!(wait_for_events(&ucm, &service));
    }

    #[test]
    fn events_present_after_removing_config() {
        let service = TestService::default();
        let file_path = service.path().join(USER_CONFIG_FILE);
        let mut ucm = UserConfigWatcher::new();

        ucm.add(&service);
        File::create(&file_path).expect("creating file");

        // Allow the watcher to notice that a file was created.
        thread::sleep(Duration::from_millis(100));

        ucm.reset_events_for(&service);

        remove_file(&file_path).expect("removing file");

        assert!(wait_for_events(&ucm, &service));
    }

    fn wait_for_events<T: Serviceable>(ucm: &UserConfigWatcher, service: &T) -> bool {
        let start = Instant::now();
        let timeout = Duration::from_millis(1000);

        while start.elapsed() < timeout {
            if ucm.have_events_for(service) {
                return true;
            }

            thread::sleep(Duration::from_millis(10));
        }

        false
    }

    struct TestService {
        name: String,
        path: PathBuf,
        service_group: ServiceGroup,
    }

    impl Serviceable for TestService {
        fn name(&self) -> &String {
            &self.name
        }

        fn path(&self) -> &PathBuf {
            &self.path
        }

        fn service_group(&self) -> &ServiceGroup {
            &self.service_group
        }
    }

    impl Default for TestService {
        fn default() -> Self {
            Self {
                name: String::from("foo"),
                path: TempDir::new("user-config-watcher")
                    .expect("creating temp dir")
                    .into_path(),
                service_group: ServiceGroup::from_str("foo.bar@yoyodine").unwrap(),
            }
        }
    }
}
