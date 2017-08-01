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

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread::Builder as ThreadBuilder;
use std::time::Duration;

use butterfly::member::Member;
use config::GOSSIP_DEFAULT_PORT;
use error::{Error, Result};
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};

const WATCHER_DELAY_MS: u64 = 2_000;
static LOGKEY: &'static str = "PW";

pub struct PeerWatcher {
    path: PathBuf,
    have_events: Arc<AtomicBool>,
}

struct WatcherData<W: Watcher> {
    watcher: W,
    rx: Receiver<DebouncedEvent>,
    abs_path: PathBuf,
}

impl PeerWatcher {
    pub fn run<P>(path: P) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        Self::run_with::<RecommendedWatcher, P>(path)
    }

    fn run_with<W, P>(path: P) -> Result<Self>
    where
        P: Into<PathBuf>,
        W: Watcher,
    {
        let path = path.into();
        let have_events = Self::setup_watcher::<W>(path.clone())?;

        Ok(PeerWatcher {
            path: path,
            have_events: have_events,
        })
    }

    fn setup_watcher<W>(path: PathBuf) -> Result<Arc<AtomicBool>>
    where
        W: Watcher,
    {
        let (abs_path, dir_path) = Self::watcher_paths(path)?;
        let have_events = Arc::new(AtomicBool::new(abs_path.is_file()));
        let have_events_for_thread = Arc::clone(&have_events);
        ThreadBuilder::new()
            .name(format!("peer-watcher-{}", abs_path.display()))
            .spawn(move || {
                debug!("PeerWatcher({}) thread starting", abs_path.display());
                loop {
                    let watcher_data = match Self::create_watcher::<W>(&abs_path,
                                                                       &dir_path) {
                        Some(wd) => wd,
                        None => return
                    };
                    Self::watcher_event_loop(watcher_data,
                                             &have_events_for_thread);
                }
            })?;
        Ok(have_events)
    }

    fn create_watcher<W>(abs_path: &PathBuf,
                         dir_path: &PathBuf) -> Option<(WatcherData<W>)>
    where
        W: Watcher,
    {
        let (tx, rx) = channel();
        let mut watcher_data = match W::new(tx, Duration::from_millis(WATCHER_DELAY_MS)) {
            Ok(w) => WatcherData::<W> {
                watcher: w,
                rx: rx,
                abs_path: abs_path.clone(),
            },
            Err(err) => {
                outputln!(
                    "PeerWatcher({}) could not start notifier, ending thread ({})",
                    abs_path.display(),
                    err
                );
                return None
            },
        };
        match watcher_data.watcher.watch(&dir_path, RecursiveMode::NonRecursive) {
            Ok(_) => Some(watcher_data),
            Err(err) => {
                outputln!(
                    "PeerWatcher({}) could not start fs watching, ending thread ({})",
                    watcher_data.abs_path.display(),
                    err
                );
                None
            },
        }
    }

    fn watcher_event_loop<W>(watcher_data: WatcherData<W>,
                             have_events: &Arc<AtomicBool>)
    where
        W: Watcher,
    {
        while let Ok(event) = watcher_data.rx.recv() {
            let our_event = match event {
                DebouncedEvent::NoticeWrite(ref p) |
                DebouncedEvent::NoticeRemove(ref p) |
                DebouncedEvent::Create(ref p) |
                DebouncedEvent::Write(ref p) |
                DebouncedEvent::Chmod(ref p) |
                DebouncedEvent::Remove(ref p) => {
                    *p == watcher_data.abs_path
                },
                DebouncedEvent::Rename(ref p1, ref p2) => {
                    (*p1 == watcher_data.abs_path) || (*p2 == watcher_data.abs_path)
                },
                DebouncedEvent::Rescan => false,
                DebouncedEvent::Error(_, _) => false,
            };
            outputln!(
                "PeerWatcher({}) file system event: {:?}, ignored: {}",
                watcher_data.abs_path.display(),
                event,
                !our_event
            );
            if our_event {
                have_events.store(true, Ordering::Relaxed);
            }
        }
        outputln!(
            "PeerWatcher({}) fs watching died, restarting",
            watcher_data.abs_path.display()
        );
    }

    fn watcher_paths(p: PathBuf) -> Result<(PathBuf, PathBuf)> {
        let abs_path = if p.is_absolute() {
            p.clone()
        } else {
            let cwd = env::current_dir().map_err(|e| sup_error!(Error::Io(e)))?;
            cwd.join(p)
        };
        let parent = match abs_path.parent() {
            Some(p) => {
                if !p.is_dir() {
                    return Err(sup_error!(Error::PeerWatcherDirNotFound(p.display().to_string())));
                }
                p.to_owned()
            },
            None => return Err(sup_error!(Error::PeerWatcherFileIsRoot))
        };
        match abs_path.file_name() {
            Some(_) => Ok((abs_path, parent)),
            None => Err(sup_error!(Error::PeerWatcherDirNotFound(parent.display().to_string()))),
        }
    }

    pub fn has_fs_events(&self) -> bool {
        self.have_events.load(Ordering::Relaxed)
    }

    pub fn get_members(&self) -> Result<Vec<Member>> {
        if !self.path.is_file() {
            self.have_events.store(false, Ordering::Relaxed);
            return Ok(Vec::new());
        }
        let file = File::open(&self.path)
            .map_err(|err| {
                return sup_error!(Error::Io(err));
            })?;
        let reader = BufReader::new(file);
        let mut members: Vec<Member> = Vec::new();
        for line in reader.lines() {
            if let Ok(peer) = line {
                let peer_addr = if peer.find(':').is_some() {
                    peer
                } else {
                    format!("{}:{}", peer, GOSSIP_DEFAULT_PORT)
                };
                let addrs: Vec<SocketAddr> = match peer_addr.to_socket_addrs() {
                    Ok(addrs) => addrs.collect(),
                    Err(e) => {
                        outputln!("Failed to resolve peer: {}", peer_addr);
                        return Err(sup_error!(Error::NameLookup(e)));
                    }
                };
                let addr: SocketAddr = addrs[0];
                let mut member = Member::default();
                member.set_address(format!("{}", addr.ip()));
                member.set_swim_port(addr.port() as i32);
                member.set_gossip_port(addr.port() as i32);
                members.push(member);
            }
        };
        self.have_events.store(false, Ordering::Relaxed);
        Ok(members)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{DirBuilder, File, OpenOptions};
    use tempdir::TempDir;
    use super::PeerWatcher;
    use butterfly::member::Member;
    use config::GOSSIP_DEFAULT_PORT;
    use std::io::Write;
    use error::Error;

    #[test]
    fn no_file() {
        let tmpdir = TempDir::new("peerwatchertest").unwrap();
        let path = tmpdir.path().join("no_such_file");
        let watcher = PeerWatcher::run(path).unwrap();

        assert_eq!(false, watcher.has_fs_events());
        assert_eq!(watcher.get_members().unwrap(), vec![]);
    }

    #[test]
    fn empty_file() {
        let tmpdir = TempDir::new("peerwatchertest").unwrap();
        let path = tmpdir.path().join("empty_file");
        File::create(&path).unwrap();
        let watcher = PeerWatcher::run(path).unwrap();

        assert_eq!(true, watcher.has_fs_events());
        assert_eq!(watcher.get_members().unwrap(), vec![]);
    }

    #[test]
    fn with_file() {
        let tmpdir = TempDir::new("peerwatchertest").unwrap();
        let path = tmpdir.path().join("some_file");
        let mut file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path.clone())
            .unwrap();
        let watcher = PeerWatcher::run(path).unwrap();
        writeln!(file, "1.2.3.4:5").unwrap();
        writeln!(file, "4.3.2.1").unwrap();
        let mut member1 = Member::default();
        member1.set_id(String::new());
        member1.set_address(String::from("1.2.3.4"));
        member1.set_swim_port(5 as i32);
        member1.set_gossip_port(5 as i32);
        let mut member2 = Member::default();
        member2.set_id(String::new());
        member2.set_address(String::from("4.3.2.1"));
        member2.set_swim_port(GOSSIP_DEFAULT_PORT as i32);
        member2.set_gossip_port(GOSSIP_DEFAULT_PORT as i32);
        let expected_members = vec![member1, member2];
        let mut members = watcher.get_members().unwrap();
        for mut member in &mut members {
            member.set_id(String::new());
        }
        assert_eq!(expected_members, members);
    }

    #[test]
    fn bad_path_root() {
        match PeerWatcher::run("/") {
            Err(e) => {
                match e.err {
                    Error::PeerWatcherFileIsRoot => (),
                    wrong => panic!("Unexpected error returned {:?}", wrong),
                }
            },
            Ok(_) => panic!("Watcher should fail to run"),
        }
    }

    #[test]
    fn bad_path_not_a_dir() {
        let tmpdir = TempDir::new("peerwatchertest").unwrap();
        let wrong_path = tmpdir.path().join("should_be_a_dir_but_is_a_file");
        let bogus_path = wrong_path.join("bogus_file");
        File::create(&wrong_path).unwrap();
        match PeerWatcher::run(&bogus_path) {
            Err(e) => {
                match e.err {
                    Error::PeerWatcherDirNotFound(_) => (),
                    wrong => panic!("Unexpected error returned {:?}", wrong),
                }
            },
            Ok(_) => panic!("Watcher should fail to run"),
        }
    }

    #[test]
    fn ignore_watched_dir() {
        let tmpdir = TempDir::new("peerwatchertest").unwrap();
        let ignored_path = tmpdir.path().join("should_be_ignored");
        DirBuilder::new().create(&ignored_path).unwrap();
        let watcher = PeerWatcher::run(&ignored_path).unwrap();
        assert_eq!(false, watcher.has_fs_events());
        assert_eq!(watcher.get_members().unwrap(), vec![]);
    }
}
