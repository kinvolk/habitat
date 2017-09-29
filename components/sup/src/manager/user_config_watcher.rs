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
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::thread::Builder as ThreadBuilder;

use super::file_watcher::{Callbacks, default_file_watcher};

use hcore::service::ServiceGroup;
use manager::service::Service;

static LOGKEY: &'static str = "UCW";

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


// WorkerState contains the channels the worker uses to communicate with the Watcher.
struct WorkerState {
    // This channel is used by the watcher to be notified when a worker has events.
    have_events: Receiver<()>,
}

type ServiceName = String;
pub struct UserConfigWatcher {
    states: HashMap<ServiceName, WorkerState>,
}

impl UserConfigWatcher {
    pub fn new() -> Self {
        Self { states: HashMap::new() }
    }

    /// Adds a service to the User Config Watcher, thereby starting a watcher thread.
    pub fn add<T: Serviceable>(&mut self, service: &T) -> io::Result<()> {
        // It isn't possible to use the `or_insert_with` function here because it can't have a
        // return value, which we need to return the error from `Worker::run`.
        if let None = self.states.get(service.name()) {
            // Establish bi-directional communication with the worker by creating two channels.
            let (events_tx, events_rx) = channel();

            Worker::run(&service.path(), events_tx)?;

            outputln!(preamble service.service_group(), "Watching user.toml");

            let state = WorkerState {
                have_events: events_rx,
            };

            self.states.insert(service.name().to_owned(), state);
        }

        Ok(())
    }

    /// Checks whether the watcher for the specified service has observed any events, thereby
    /// consuming them.
    pub fn have_events_for<T: Serviceable>(&self, service: &T) -> bool {
        if let Some(state) = self.states.get(service.name()) {

            let rx = &state.have_events;

            match rx.try_recv() {
                Ok(()) => {
                    return true;
                }
                Err(TryRecvError::Empty) => return false,
                Err(TryRecvError::Disconnected) => {
                    debug!("UserConfigWatcher worker has died; restarting...");
                    return false;
                }
            }
        }

        false
    }
}

struct UserConfigCallbacks {
    have_events: Sender<()>,
}

impl Callbacks for UserConfigCallbacks {
    fn file_appeared(&mut self, _: &Path) {
        if let Err(e) = self.have_events.send(()) {
            debug!("Worker could not notify Manager of event: {}", e);
        }
    }

    fn file_modified(&mut self, _: &Path) {
        if let Err(e) = self.have_events.send(()) {
            debug!("Worker could not notify Manager of event: {}", e);
        }
    }

    fn file_disappeared(&mut self, _: &Path) {
        if let Err(e) = self.have_events.send(()) {
            debug!("Worker could not notify Manager of event: {}", e);
        }
    }
}

struct Worker;

impl Worker {
    // starts a new thread with the file watcher tracking the service's user-config file
    pub fn run(
        service_path: &Path,
        have_events: Sender<()>,
    ) -> io::Result<()> {
        let path = service_path.join("user.toml");

        Self::setup_watcher(path, have_events)?;

        Ok(())
    }

    fn setup_watcher(
        path: PathBuf,
        have_events: Sender<()>,
    ) -> io::Result<()> {
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
            })?;

        Ok(())
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
        ucm.add(&service).expect("adding service");

        assert!(!ucm.have_events_for(&service));
    }

    #[test]
    fn events_present_after_adding_config() {
        let service = TestService::default();
        let mut ucm = UserConfigWatcher::new();
        ucm.add(&service).expect("adding service");

        File::create(service.path().join("user.toml")).expect("creating file");

        assert!(wait_for_events(&ucm, &service));
    }

    #[test]
    fn events_present_after_changing_config() {
        let service = TestService::default();
        let file_path = service.path().join("user.toml");
        let mut ucm = UserConfigWatcher::new();

        ucm.add(&service).expect("adding service");
        let mut file = File::create(&file_path).expect("creating file");

        file.write_all(b"42").expect("writing to user.toml");

        assert!(wait_for_events(&ucm, &service));
    }

    #[test]
    fn events_present_after_removing_config() {
        let service = TestService::default();
        let file_path = service.path().join("user.toml");
        let mut ucm = UserConfigWatcher::new();

        ucm.add(&service).expect("adding service");
        File::create(&file_path).expect("creating file");

        // Allow the watcher to notice that a file was created.
        thread::sleep(Duration::from_millis(100));

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
