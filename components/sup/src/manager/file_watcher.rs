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
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::ffi::OsString;
use std::mem::swap;
use std::path::{Component, Path, PathBuf};
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;

use error::{Error, Result, SupError};
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};

const WATCHER_DELAY_MS: u64 = 2_000;
static LOGKEY: &'static str = "PW";

// Callbacks are attached to events.
pub trait Callbacks {
    fn listening_for_events(&mut self);
    fn stopped_listening(&mut self);
    fn file_appeared(&mut self, real_path: &Path);
    fn file_modified(&mut self, real_path: &Path);
    fn file_disappeared(&mut self, real_path: &Path);
    fn error(&mut self, err: &SupError) -> bool;
    fn continue_looping(&mut self) -> bool;
}

#[derive(Clone, Debug, Default)]
struct DirFileName {
    directory: PathBuf,
    file_name: OsString,
}

pub struct FileWatcher<C: Callbacks> {
    // The directory and filename to watch
    dir_file_name: DirFileName,
    pub callbacks: C,
}

impl DirFileName {
    // split_path separates the dirname from the basename.
    fn split_path(path: PathBuf) -> Option<Self> {
        let parent = match path.parent() {
            None => return None,
            Some(p) => p,
        };
        let file_name = match path.file_name() {
            None => return None,
            Some(f) => f,
        };
        Some(Self {
            directory: parent.to_owned(),
            file_name: file_name.to_owned(),
        })
    }

    fn as_path(&self) -> PathBuf {
        self.directory.join(&self.file_name)
    }
}

// TODO: handle mount events, we could use libc crate to get the
// select function that we could use to watch /proc/self/mountinfo for
// exceptional events - such event means that something was mounted or
// unmounted. For this to work, we would need to keep a mount state of
// the directories we are interested in and compare it to the current
// status in mountinfo, when some change there happens.

#[derive(Debug)]
struct SplitPath {
    directory: PathBuf,
    file_name: Option<OsString>,
}

impl SplitPath {
    fn push(&mut self, path: OsString) -> DirFileName {
        match self.file_name {
            Some(ref file_name) => self.directory.push(file_name),
            None => (),
        }
        self.file_name = Some(path.clone());

        DirFileName {
            directory: self.directory.clone(),
            file_name: path,
        }
    }
}

// TODO(asymmetric): document this
#[derive(Debug)]
struct ProcessPathArgs {
    path: PathBuf,
    path_rest: VecDeque<OsString>,
    index: u32,
    prev: Option<PathBuf>,
}

#[derive(Debug)]
struct ChainLinkInfo {
    path: PathBuf,
    prev: Option<PathBuf>,
}

#[derive(Debug)]
struct PathsActionData {
    dir_file_name: DirFileName,
    args: ProcessPathArgs,
}

#[derive(Debug)]
struct Common {
    path: PathBuf,
    dir_file_name: DirFileName,
    prev: Option<PathBuf>,
    next: Option<PathBuf>,
    // TODO: That was needed to make sure that the generated process
    // args with lower index will overwrite the generated process args
    // with higher index. Several generated process args can be
    // generated when several files or directories are removed. Not
    // sure if we need it anymore, since we have a simple chain of
    // watches, so the most recent removal event should happen for the
    // element in chain with lowest index.
    index: u32,
    path_rest: VecDeque<OsString>,
}

impl Common {
    fn get_process_path_args(&self) -> ProcessPathArgs {
        let mut path_rest = VecDeque::new();
        path_rest.push_back(self.dir_file_name.file_name.clone());
        path_rest.extend(self.path_rest.iter().cloned());
        ProcessPathArgs {
            path: self.dir_file_name.directory.clone(),
            path_rest: path_rest,
            index: self.index,
            prev: self.prev.clone(),
        }
    }

    fn get_chain_link_info(&self) -> ChainLinkInfo {
        ChainLinkInfo {
            path: self.path.clone(),
            prev: self.prev.clone(),
        }
    }

    fn get_paths_action_data(&self) -> PathsActionData {
        PathsActionData {
            dir_file_name: self.dir_file_name.clone(),
            args: self.get_process_path_args(),
        }
    }
}

// TODO(asymmetric): document
struct CommonGenerator {
    prev: Option<PathBuf>,
    keep_prev: bool,
    path: PathBuf,
    split_path: SplitPath,
    index: u32,
    path_rest: VecDeque<OsString>,
}

impl CommonGenerator {
    fn new(args: ProcessPathArgs) -> Self {
        let split_path = SplitPath {
            directory: args.path.clone(),
            file_name: None,
        };
        Self {
            prev: args.prev,
            keep_prev: false,
            path: args.path,
            split_path: split_path,
            index: args.index,
            path_rest: args.path_rest,
        }
    }

    fn keep_previous(&mut self) {
        self.keep_prev = true;
    }

    fn set_path(&mut self, path: PathBuf) {
        self.path = path;
        self.split_path = SplitPath {
            directory: self.path.clone(),
            file_name: None,
        };
    }

    fn prepend_to_path_rest(&mut self, mut path_rest: VecDeque<OsString>) {
        path_rest.extend(self.path_rest.drain(..));
        self.path_rest = path_rest;
    }

    // get_new_common extracts a new common component from the `path_rest` vec.
    fn get_new_common(&mut self) -> Option<Common> {
        if let Some(component) = self.path_rest.pop_front() {
            let prev = self.prev.clone();

            // TODO(asymmetric): what is going on here?
            if self.keep_prev {
                self.keep_prev = false;
            } else {
                self.prev = Some(self.path.clone());
            }
            self.path.push(&component);
            let path = self.path.clone();
            let dir_file_name = self.split_path.push(component);
            let index = self.index;
            self.index += 1;
            Some(Common {
                path: path,
                dir_file_name: dir_file_name,
                prev: prev,
                next: None,
                index: index,
                path_rest: self.path_rest.clone(),
            })
        } else {
            None
        }
    }
}

#[derive(Debug)]
enum WatchedFile {
    Regular(Common),
    MissingRegular(Common),
    Symlink(Common),
    Directory(Common),
    MissingDirectory(Common),
}

impl WatchedFile {
    fn get_common(&self) -> &Common {
        match self {
            &WatchedFile::Regular(ref c) |
            &WatchedFile::MissingRegular(ref c) |
            &WatchedFile::Symlink(ref c) |
            &WatchedFile::Directory(ref c) |
            &WatchedFile::MissingDirectory(ref c) => c,
        }
    }

    fn get_mut_common(&mut self) -> &mut Common {
        match self {
            &mut WatchedFile::Regular(ref mut c) |
            &mut WatchedFile::MissingRegular(ref mut c) |
            &mut WatchedFile::Symlink(ref mut c) |
            &mut WatchedFile::Directory(ref mut c) |
            &mut WatchedFile::MissingDirectory(ref mut c) => c,
        }
    }

    fn steal_common(self) -> Common {
        match self {
            WatchedFile::Regular(c) |
            WatchedFile::MissingRegular(c) |
            WatchedFile::Symlink(c) |
            WatchedFile::Directory(c) |
            WatchedFile::MissingDirectory(c) => c,
        }
    }
}

// Similar to std::fs::canonicalize, but without resolving symlinks.
//
// I'm not sure if this is entirely correct, consider:
//
// pwd # displays /some/abs/path
// mkdir -p foo/bar
// ln -s foo/bar baz
// realpath baz/.. # displays /some/abs/path/foo
// cd baz/.. # stays in the same directory instead of going to foo
//
// Basically, realpath says that "baz/.." == "foo" and cd says that
// "baz/.." == ".".
//
// I went here with the "cd" way. Likely less surprising.
fn simplify_abs_path(abs_path: &PathBuf) -> PathBuf {
    let mut simple = PathBuf::new();
    for c in abs_path.components() {
        match c {
            Component::CurDir => (),
            Component::ParentDir => {simple.pop();}
            _ => simple.push(c.as_os_str()),
        };
    }
    simple
}

// TODO(asymmetric): what is this?
#[derive(Debug)]
struct PathProcessState {
    // start_path is the place in the filesystem tree where watching starts.
    start_path: PathBuf,
    // TODO: Figure out if we can perform loop detection without this
    // hash map, but only using whatever data we have in Paths.
    symlink_loop_catcher: HashMap</* symlink path: */PathBuf, /* path + path_rest */PathBuf>,
    real_file: Option<PathBuf>,
}

// TODO(asymmetric): Document the difference between PathsAction and EventAction.
// EventActions are high-level actions to be performed in response to filesystem events.
#[derive(Debug)]
enum EventAction {
    Ignore,
    PlainChange(PathBuf),
    RestartWatching,
    AddRegular(PathsActionData),
    DropRegular(PathsActionData),
    AddDirectory(PathsActionData),
    DropDirectory(PathsActionData),
    RewireSymlink(PathsActionData),
    DropSymlink(PathsActionData),
    SettlePath(PathBuf),
}


// Lower-level actions, created to execute `EventAction`s.
#[derive(Debug)]
enum PathsAction {
    NotifyFileAppeared(PathBuf),
    NotifyFileModified(PathBuf),
    NotifyFileDisappeared(PathBuf),
    DropWatch(PathBuf),
    AddPathToSettle(PathBuf),
    SettlePath(PathBuf),
    ProcessPathAfterSettle(ProcessPathArgs),
    RestartWatching
}

// Paths holds the state with regards to watching.
#[derive(Debug)]
struct Paths {
    // TODO(asymmetric): why do we need paths and dirs?
    paths: HashMap<PathBuf, WatchedFile>,
    dirs: HashMap</*watched directory: */PathBuf, /* watched files count: */ u32>,
    process_state: PathProcessState,
    // Filled in notice remove, drained in remove and rename.
    paths_to_settle: HashSet<PathBuf>,
    process_args_after_settle: Option<ProcessPathArgs>,
}

// TODO this could be rename to BranchStatus
#[derive(Debug)]
enum BranchResult {
    AlreadyExists,
    NewInOldDirectory(ChainLinkInfo),
    NewInNewDirectory(ChainLinkInfo, PathBuf),
}

// TODO document this.
#[derive(Debug)]
enum LeafResult {
    NewInOldDirectory(ChainLinkInfo),
    NewInNewDirectory(ChainLinkInfo, PathBuf),
}

#[derive(Debug)]
enum ProcessPathStatus {
    Executed(Vec<PathBuf>),
    NotExecuted(ProcessPathArgs),
}

impl Paths {
    fn new(simplified_abs_path: &PathBuf) -> Self {
        Self {
            paths: HashMap::new(),
            dirs: HashMap::new(),
            process_state: PathProcessState {
                start_path: simplified_abs_path.clone(),
                symlink_loop_catcher: HashMap::new(),
                real_file: None,
            },
            paths_to_settle: HashSet::new(),
            process_args_after_settle: None,
        }
    }

    // generate_watch_paths returns a list of paths to watch, based on the configured `start_path`.
    fn generate_watch_paths(&mut self) -> Vec<PathBuf> {
        let process_args = Self::path_for_processing(&self.process_state.start_path);

        self.process_path(process_args)
    }

    // Given a path, path_for_processing separates the root from the rest, and stores them in a
    // ProcessPathArgs struct.
    fn path_for_processing(simplified_abs_path: &PathBuf) -> ProcessPathArgs {
        // path holds the `/` component of a path, or the path prefix on Windows (e.g. `C:`).
        let mut path = PathBuf::new();
        // path_rest holds all other components of a path.
        let mut path_rest = VecDeque::new();

        // components are substrings between path separators ('/' or '\')
        for component in simplified_abs_path.components() {
            match component {
                Component::Prefix(_) | Component::RootDir => {
                    path.push(component.as_os_str().to_owned());
                }
                Component::Normal(c) => path_rest.push_back(c.to_owned()),
                // Respectively the `.`. and `..` components of a path.
                Component::CurDir | Component::ParentDir => panic!("the path should be simplified"),
            };
        }

        ProcessPathArgs {
            path: path,
            path_rest: path_rest,
            index: 0,
            prev: None,
        }
    }

    // Navigates through each of the components of the watched paths, deciding what action to
    // take for each of them.
    fn process_path(&mut self, args: ProcessPathArgs) -> Vec<PathBuf> {
        let mut common_generator = CommonGenerator::new(args);
        let mut new_watches = Vec::new();

        self.process_state.real_file = None;

        while let Some(common) = common_generator.get_new_common() {
            let dir_file_name = common.dir_file_name.clone();

            match common.path.symlink_metadata() {
                Err(_) => {
                    let leaf_result = if common.path_rest.is_empty() {
                        // The final file is missing.
                        self.add_missing_regular(common)
                    } else {
                        self.add_missing_directory(common)
                    };
                    self.handle_leaf_result(leaf_result, &mut new_watches);
                    break;
                }
                Ok(metadata) => {
                    let file_type = metadata.file_type();
                    if !file_type.is_symlink() {
                        if common.path_rest.is_empty() {
                            let leaf_result = if file_type.is_file() {
                                self.process_state.real_file = Some(common.path.clone());
                                self.add_regular(common)
                            } else {
                                // We probably found a directory where
                                // we expected a file.
                                self.add_missing_regular(common)
                            };
                            self.handle_leaf_result(leaf_result, &mut new_watches);
                            break;
                        }
                        if file_type.is_dir() {
                            let branch_result = self.get_or_add_directory(common);
                            self.handle_branch_result(
                                &mut common_generator,
                                branch_result,
                                &mut new_watches,
                            );
                            continue;
                        }

                        // Not a symlink, not a dir, and not a last
                        // component - this means that we got some
                        // file in the middle of the path.
                        let leaf_result = self.add_missing_directory(common);
                        self.handle_leaf_result(leaf_result, &mut new_watches);
                        break;
                    }
                }
            }

            let target = match dir_file_name.as_path().read_link() {
                Ok(target) => target,
                Err(_) => {
                    let leaf_result = if common.path_rest.is_empty() {
                        self.add_missing_regular(common)
                    } else {
                        self.add_missing_directory(common)
                    };
                    self.handle_leaf_result(leaf_result, &mut new_watches);
                    break;
                }
            };
            let target_path = if target.is_absolute() {
                target
            } else {
                dir_file_name.directory.join(target)
            };
            let simplified_target = simplify_abs_path(&target_path);
            let process_args = Self::path_for_processing(&simplified_target);
            if self.symlink_loop(
                &common.path,
                &common.path_rest,
                &process_args.path,
                &process_args.path_rest,
            ) {
                // Symlink loop, nothing to watch here - hopefully
                // later some symlink will be rewired to the real
                // file.
                break;
            }

            let branch_result = self.get_or_add_symlink(common);
            self.handle_branch_result(
                &mut common_generator,
                branch_result,
                &mut new_watches,
            );
            common_generator.set_path(process_args.path);
            common_generator.prepend_to_path_rest(process_args.path_rest);
        }

        return new_watches;
    }

    fn handle_leaf_result(&mut self, leaf_result: LeafResult, new_watches: &mut Vec<PathBuf>) {
        match leaf_result {
            LeafResult::NewInNewDirectory(chain_link_info, directory) => {
                new_watches.push(directory);
                self.setup_chain_link(chain_link_info);
            }
            LeafResult::NewInOldDirectory(chain_link_info) => {
                self.setup_chain_link(chain_link_info);
            }
        }
    }

    fn handle_branch_result(
        &mut self,
        common_generator: &mut CommonGenerator,
        branch_result: BranchResult,
        new_watches: &mut Vec<PathBuf>,
    ) {
        match branch_result {
            BranchResult::AlreadyExists => {
                common_generator.keep_previous();
            }
            BranchResult::NewInNewDirectory(chain_link_info, directory) => {
                new_watches.push(directory);
                self.setup_chain_link(chain_link_info);
            }
            BranchResult::NewInOldDirectory(chain_link_info) => {
                self.setup_chain_link(chain_link_info);
            }
        }
    }

    fn setup_chain_link(&mut self, chain_link_info: ChainLinkInfo) {
        if let Some(previous) = chain_link_info.prev {
            if let Some(previous_watched_file) = self.get_mut_watched_file(&previous) {
                let previous_common = previous_watched_file.get_mut_common();
                previous_common.next = Some(chain_link_info.path);
            }
        }
    }

    // Checks whether the path is present in the list of watched paths.
    fn get_watched_file(&self, path: &PathBuf) -> Option<&WatchedFile> {
        self.paths.get(path)
    }

    fn get_mut_watched_file(&mut self, path: &PathBuf) -> Option<&mut WatchedFile> {
        self.paths.get_mut(path)
    }

    fn get_mut_directory(&mut self, path: &PathBuf) -> Option<&mut u32> {
        self.dirs.get_mut(path)
    }

    fn drop_watch(&mut self, path: &PathBuf) -> Option<PathBuf> {
        // TODO(asymmetric): when can we get None here? Explain.
        if let Some(watched_file) = self.paths.remove(path) {
            let common = watched_file.steal_common();
            let unwatch_directory;
            let dir_path = common.dir_file_name.directory;
            {
                let count = self.get_mut_directory(&dir_path).expect(
                    "expected directory for the watched file"
                );
                *count -= 1;
                unwatch_directory = *count == 0;
            }
            self.process_state.symlink_loop_catcher.remove(path);
            if unwatch_directory {
                self.dirs.remove(&dir_path);
                return Some(dir_path);
            }
        }

        None
    }

    fn symlink_loop(
        &mut self,
        path: &PathBuf,
        path_rest: &VecDeque<OsString>,
        new_path: &PathBuf,
        new_path_rest: &VecDeque<OsString>
    ) -> bool {
        let mut merged_path_rest = new_path_rest.clone();
        merged_path_rest.extend(path_rest.iter().cloned());
        let mut merged_path = new_path.clone();
        merged_path.extend(merged_path_rest);
        match self.process_state.symlink_loop_catcher.entry(path.clone()) {
            Entry::Occupied(o) => *o.get() == merged_path,
            Entry::Vacant(v) => {
                v.insert(merged_path);
                false
            }
        }
    }

    // Returns true, if the directory did not exist before, so it
    // needs to be watched now.
    fn add_regular(&mut self, common: Common) -> LeafResult {
        self.add_leaf_watched_file(WatchedFile::Regular(common))
    }

    // Returns true, if the directory did not exist before, so it
    // needs to be watched now.
    fn add_missing_regular(&mut self, common: Common) -> LeafResult {
        self.add_leaf_watched_file(WatchedFile::MissingRegular(common))
    }

    // Returns true, if the directory did not exist before, so it
    // needs to be watched now.
    fn add_missing_directory(&mut self, common: Common) -> LeafResult {
        self.add_leaf_watched_file(WatchedFile::MissingDirectory(common))
    }

    fn add_leaf_watched_file(&mut self, watched_file: WatchedFile) -> LeafResult {
        let dir_file_name = watched_file.get_common()
            .dir_file_name
            .clone();
        let needs_watch = self.add_dir(&dir_file_name);
        let chain_link_info = match self.paths.entry(dir_file_name.as_path()) {
            Entry::Occupied(mut o) => {
                // This likely should not happen I think.
                o.insert(watched_file);
                o.get().get_common().get_chain_link_info()
            }
            Entry::Vacant(v) => {
                v.insert(watched_file).get_common().get_chain_link_info()
            }
        };
        if needs_watch {
            LeafResult::NewInNewDirectory(chain_link_info, dir_file_name.directory)
        } else {
            LeafResult::NewInOldDirectory(chain_link_info)
        }
    }

    fn get_or_add_directory(&mut self, common: Common) -> BranchResult {
        self.get_or_add_branch_watched_file(WatchedFile::Directory(common))
    }

    fn get_or_add_symlink(&mut self, common: Common) -> BranchResult {
        self.get_or_add_branch_watched_file(WatchedFile::Symlink(common))
    }

    fn get_or_add_branch_watched_file(&mut self, watched_file: WatchedFile) -> BranchResult {
        if self.paths.contains_key(&watched_file.get_common().path) {
            return BranchResult::AlreadyExists;
        }
        let dir_file_name = watched_file.get_common()
            .dir_file_name
            .clone();
        let needs_watch = self.add_dir(&dir_file_name);
        let chain_link_info = watched_file.get_common().get_chain_link_info();
        self.paths.insert(dir_file_name.as_path(), watched_file);
        if needs_watch {
            BranchResult::NewInNewDirectory(chain_link_info, dir_file_name.directory)
        } else {
            BranchResult::NewInOldDirectory(chain_link_info)
        }
    }

    // Returns true, if the directory did not exist before, so it
    // needs to be watched now.
    fn add_dir(&mut self, dir_file_name: &DirFileName) -> bool {
        match self.dirs.entry(dir_file_name.directory.clone()) {
            Entry::Occupied(mut o) => {
                *o.get_mut() += 1;
                false
            }
            Entry::Vacant(v) => {
                v.insert(1);
                true
            }
        }
    }

    fn add_path_to_settle(&mut self, path: PathBuf) {
        self.paths_to_settle.insert(path);
    }

    fn settle_path(&mut self, path: PathBuf) {
        self.paths_to_settle.remove(&path);
    }

    fn set_process_args(&mut self, args: ProcessPathArgs) {
        if match self.process_args_after_settle {
            Some(ref old_args) => args.index < old_args.index,
            None => true,
        } {
            self.process_args_after_settle = Some(args)
        }
    }

    fn process_path_or_defer_if_unsettled(&mut self) -> Option<Vec<PathBuf>> {
        let mut process_args = None;
        swap(&mut process_args, &mut self.process_args_after_settle);
        let (directories, new_args) = match process_args {
            Some(args) => match self.process_path_if_settled(args) {
                ProcessPathStatus::Executed(v) => (Some(v), None),
                ProcessPathStatus::NotExecuted(a) => (None, Some(a)),
            },
            None => (None, None)
        };
        self.process_args_after_settle = new_args;
        directories
    }

    fn process_path_if_settled(&mut self, args: ProcessPathArgs) -> ProcessPathStatus {
        if self.paths_to_settle.is_empty() {
            ProcessPathStatus::Executed(self.process_path(args))
        } else {
            ProcessPathStatus::NotExecuted(args)
        }
    }

    fn reset(&mut self) -> Vec<PathBuf> {
        self.paths.clear();
        let mut dirs_to_unwatch = Vec::new();
        dirs_to_unwatch.extend(self.dirs.drain().map(|i| i.0));
        self.paths_to_settle.clear();
        self.process_args_after_settle = None;
        self.process_state.symlink_loop_catcher.clear();
        dirs_to_unwatch
    }
}

// WatcherData holds all the information a file watcher needs to work.
struct WatcherData<W: Watcher> {
    // The watcher itself.
    watcher: W,
    // A channel for receiving events.
    rx: Receiver<DebouncedEvent>,
    // The paths to watch.
    paths: Paths,
}

impl<C: Callbacks> FileWatcher<C> {
    pub fn new<P>(path: P, callbacks: C) -> Result<Self>
    where
        P: Into<PathBuf>
    {
        let dir_file_name = Self::watcher_path(path.into())?;

        Ok(Self {
            dir_file_name: dir_file_name,
            callbacks: callbacks,
        })
    }

    fn watcher_path(p: PathBuf) -> Result<DirFileName> {
        let abs_path = if p.is_absolute() {
            p.clone()
        } else {
            let cwd = env::current_dir().map_err(|e| sup_error!(Error::Io(e)))?;
            cwd.join(p)
        };
        let simplified_abs_path = simplify_abs_path(&abs_path);
        match DirFileName::split_path(simplified_abs_path) {
            Some(dir_file_name) => Ok(dir_file_name),
            None => Err(sup_error!(Error::FileWatcherFileIsRoot)),
        }
    }

    pub fn run(&mut self) -> Result<()> {
        // RecommendedWatcher automatically selects the best implementation for the platform.
        self.run_with::<RecommendedWatcher>()
    }

    fn run_with<W: Watcher>(&mut self) -> Result<()> {
        let watcher_data = self.create_watcher_data::<W>()?;
        self.callbacks.listening_for_events();
        if let Some(ref path) = watcher_data.paths.process_state.real_file {
            self.callbacks.file_appeared(path);
        }
        self.watcher_event_loop(watcher_data);
        self.callbacks.stopped_listening();
        Ok(())
    }

    // create_watcher_data creates a watcher and runs it on the configured directories.
    fn create_watcher_data<W: Watcher>(&mut self) -> Result<WatcherData<W>> {
        let (mut w, rx) = match self.create_watcher::<W>() {
            Ok((w, rx)) => (w, rx),
            Err(err) => return Err(err),
        };

        // Initialize the Paths struct, which will hold all state relative to file watching.
        let mut paths = Paths::new(&self.dir_file_name.as_path());

        // Generate list of paths to watch.
        let directories = paths.generate_watch_paths();

        // Start watcher on each path.
        for directory in directories {
            if let Err(err) = w.watch(
                &directory,
                RecursiveMode::NonRecursive,
            ) {
                // TODO: use callback to ask whether to try again or
                // bail out.
                //
                // TODO: Probably the error callback should return an
                // enum like TryAgain, StartFromScratch, BailOut
                return Err(sup_error!(Error::NotifyError(err)));
            }
        }
        Ok(WatcherData::<W> {
            watcher: w,
            rx: rx,
            paths: paths,
        })
    }

    // create_watcher initializes a watcher.
    // It returns a watcher and a receiver side of a channel through which events are sent.
    // The watcher is a delayed watcher, which means that events will not be delivered instantly,
    // but after the delay has expired. This allows it to only receive complete events, at the cost
    // of being less responsive.
    fn create_watcher<W: Watcher>(&mut self) -> Result<(W, Receiver<DebouncedEvent>)> {
        loop {
            let (tx, rx) = channel();
            match W::new(tx, Duration::from_millis(WATCHER_DELAY_MS)) {
                Ok(w) => return Ok((w, rx)),
                Err(err) => {
                    let sup_err = sup_error!(Error::NotifyError(err));
                    if self.callbacks.error(&sup_err) {
                        continue;
                    }
                    return Err(sup_err);
                }
            };
        }
    }

    fn watcher_event_loop<W: Watcher>(&mut self, mut watcher_data: WatcherData<W>) {
        while let Ok(event) = watcher_data.rx.recv() {
            self.handle_event(&mut watcher_data, event);
            if !self.callbacks.continue_looping() {
                break;
            }
        }
        // TODO: handle the error?
    }

    fn handle_event<W: Watcher>(
        // TODO(asymmetric): does this need to be mut?
        &mut self,
        watcher_data: &mut WatcherData<W>,
        event: DebouncedEvent,
    ) -> bool {
        let paths = &mut watcher_data.paths;
        let watcher = &mut watcher_data.watcher;
        let mut actions = VecDeque::new();

        // Gather the high-level actions.
        actions.extend(Self::get_paths_actions(paths, event));

        // Perform lower-level actions.
        while let Some(action) = actions.pop_front()  {
            match action {
                PathsAction::NotifyFileAppeared(p) => {
                    self.callbacks.file_appeared(p.as_path());
                }
                PathsAction::NotifyFileModified(p) => {
                    self.callbacks.file_modified(p.as_path());
                }
                PathsAction::NotifyFileDisappeared(p) => {
                    self.callbacks.file_disappeared(p.as_path());
                }
                PathsAction::DropWatch(p) => {
                    if let Some(dir_path) = paths.drop_watch(&p) {
                        // TODO: Handle error.
                        watcher.unwatch(dir_path);
                    }
                }
                PathsAction::AddPathToSettle(p) => {
                    paths.add_path_to_settle(p);
                }
                PathsAction::SettlePath(p) => {
                    paths.settle_path(p);
                    actions.extend(Self::handle_process_path(paths, watcher));
                }
                PathsAction::ProcessPathAfterSettle(args) => {
                    paths.set_process_args(args);
                    actions.extend(Self::handle_process_path(paths, watcher));
                }
                PathsAction::RestartWatching => {
                    actions.clear();
                    if let Some(ref path) = paths.process_state.real_file {
                        actions.push_back(PathsAction::NotifyFileDisappeared(path.clone()));
                    }
                    for directory in paths.reset() {
                        // TODO: Handle error.
                        watcher.unwatch(&directory);
                    }
                    let process_args = Paths::path_for_processing(&paths.process_state.start_path);
                    actions.push_back(PathsAction::ProcessPathAfterSettle(process_args));
                }
            }
        }
        false
    }

    fn handle_process_path<W: Watcher>(paths: &mut Paths, watcher: &mut W) -> Vec<PathsAction> {
        let mut actions = Vec::new();
        match paths.process_path_or_defer_if_unsettled() {
            None => (),
            Some(directories) => {
                for directory in directories {
                    if let Err(_) = watcher.watch(
                        &directory,
                        RecursiveMode::NonRecursive,
                    ) {
                        // TODO: send some error
                    }
                }
                if let Some(ref path) = paths.process_state.real_file {
                    actions.push(PathsAction::NotifyFileAppeared(path.clone()));
                }
            }
        }
        actions
    }

    // Maps `EventAction`s to one or more lower-level `PathsAction`s .
    fn get_paths_actions(paths: &Paths, event: DebouncedEvent) -> Vec<PathsAction> {
        let mut actions = Vec::new();
        for event_action in Self::get_event_actions(paths, event) {
            match event_action {
                EventAction::Ignore => (),
                EventAction::PlainChange(p) => {
                    actions.push(PathsAction::NotifyFileModified(p));
                }
                EventAction::RestartWatching => {
                    actions.push(PathsAction::RestartWatching);
                }
                EventAction::AddRegular(pad) => {
                    let path = pad.dir_file_name.as_path();
                    actions.push(PathsAction::DropWatch(path.clone()));
                    actions.push(PathsAction::ProcessPathAfterSettle(pad.args));
                }
                EventAction::DropRegular(pad) => {
                    actions.extend(Self::drop_common(paths, pad));
                }
                EventAction::AddDirectory(pad) => {
                    let path = pad.dir_file_name.as_path();
                    actions.push(PathsAction::DropWatch(path.clone()));
                    actions.push(PathsAction::ProcessPathAfterSettle(pad.args));
                }
                EventAction::DropDirectory(pad) => {
                    actions.extend(Self::drop_common(paths, pad));
                }
                EventAction::RewireSymlink(pad) => {
                    let path = pad.dir_file_name.as_path();
                    actions.extend(Self::drop_common(paths, pad));
                    actions.push(PathsAction::SettlePath(path));
                }
                EventAction::DropSymlink(pad) => {
                    actions.extend(Self::drop_common(paths, pad));
                }
                EventAction::SettlePath(p) => {
                    actions.push(PathsAction::SettlePath(p));
                }
            };
        }
        actions
    }

    fn drop_common(paths: &Paths, pad: PathsActionData) -> Vec<PathsAction> {
        let mut actions = Vec::new();
        let path = pad.dir_file_name.as_path();
        actions.push(PathsAction::AddPathToSettle(path.clone()));
        let mut path_to_drop = Some(path);
        while let Some(path) = path_to_drop {
            let maybe_watched_file = paths.get_watched_file(&path);
            actions.push(PathsAction::DropWatch(path));
            path_to_drop = if let Some(watched_file) = maybe_watched_file {
                watched_file.get_common().next.clone()
            } else {
                None
            };
        }
        if let Some(ref path) = paths.process_state.real_file {
            actions.push(PathsAction::NotifyFileDisappeared(path.clone()));
        }
        actions.push(PathsAction::ProcessPathAfterSettle(pad.args));
        actions
    }

    // Maps filesystem events to high-level actions.
    // Every time there's a change to a file, we check whether that file is one of those we're
    // interested in. If it isn't, we ignore the event.
    fn get_event_actions(paths: &Paths, event: DebouncedEvent) -> Vec<EventAction> {
        // Usual actions on files and resulting events (assuming that
        // a and b are in the same directory which we watch)
        // touch a - Create(a)
        // ln -sf foo a (does not matter if symlink a did exist before)- Create(a)
        // mkdir a - Create(a)
        // mv a b (does not matter if b existed or not) - NoticeRemove(a), Rename(a, b)
        // mv ../a . - Create(a)
        // mv a .. - NoticeRemove(a), Remove(a)
        // rm a - NoticeRemove(a), Remove(a)
        // echo foo >a (assuming a did exist earlier) - NoticeWrite(a), Write(a)
        let event_action = match event {
            // DebouncedEvent::Write event will handle that.
            DebouncedEvent::NoticeWrite(_) => EventAction::Ignore,
            // These happen for regular files, just check if it
            // affects the file we are watching.
            //
            // TODO: I wonder if we should watch Chmod events for
            // directories too. Maybe some permission changes can
            // cause the directory to be unwatchable.
            DebouncedEvent::Write(ref p) |
            DebouncedEvent::Chmod(ref p) => {
                match paths.get_watched_file(p) {
                    Some(&WatchedFile::Regular(_)) => EventAction::PlainChange(p.clone()),
                    _ => EventAction::Ignore,
                }
            }
            DebouncedEvent::NoticeRemove(ref p) => {
                Self::handle_notice_remove_event(paths, p)
            }
            DebouncedEvent::Remove(p) => {
                Self::handle_remove_event(paths, p)
            }
            DebouncedEvent::Create(ref p) => {
                match paths.get_watched_file(p) {
                    None => EventAction::Ignore,
                    Some(&WatchedFile::MissingRegular(ref c)) => EventAction::AddRegular(c.get_paths_action_data()),
                    // Create event for an already existing file or
                    // directory should not happen, restart watching.
                    Some(&WatchedFile::Regular(_)) | Some(&WatchedFile::Directory(_)) => EventAction::RestartWatching,
                    Some(&WatchedFile::Symlink(ref c)) => EventAction::RewireSymlink(c.get_paths_action_data()),
                    Some(&WatchedFile::MissingDirectory(ref c)) => EventAction::AddDirectory(c.get_paths_action_data()),
                }
            }
            DebouncedEvent::Rename(from, to) => {
                let mut events = Vec::new();
                // Rename is annoying in that it does not come
                // together with NoticeRemove of the destination file
                // (it is preceded with NoticeRemove of the source
                // file only), so we just going to emulate it and then
                // settle the destination path.
                events.push(Self::handle_notice_remove_event(paths, &to));
                events.push(EventAction::SettlePath(to));
                events.push(Self::handle_remove_event(paths, from));
                return events;
            }
            DebouncedEvent::Rescan => EventAction::RestartWatching,
            DebouncedEvent::Error(_, _) => EventAction::RestartWatching,
        };
        vec![event_action]
    }

    fn handle_notice_remove_event(paths: &Paths, p: &PathBuf) -> EventAction {
        match paths.get_watched_file(p) {
            None => EventAction::Ignore,
            // Our directory was removed, moved elsewhere or
            // replaced. I discovered replacement scenario while
            // working on this code. Consider:
            //
            // mkdir a
            // touch a/foo
            // mkdir -p test/a
            // mv a test
            //
            // This will replace the empty directory "test/a" with
            // "a", so the file foo will be now in "test/a/foo".
            Some(&WatchedFile::Directory(ref c)) => EventAction::DropDirectory(c.get_paths_action_data()),
            // This happens when we expected p to be a file, but it
            // was something else and that thing just got removed.
            Some(&WatchedFile::MissingRegular(_)) => EventAction::Ignore,
            Some(&WatchedFile::Regular(ref c)) => EventAction::DropRegular(c.get_paths_action_data()),
            Some(&WatchedFile::Symlink(ref c)) => EventAction::DropSymlink(c.get_paths_action_data()),
            // This happens when we expected p to be a directory, but
            // it was something else and that thing just got removed.
            Some(&WatchedFile::MissingDirectory(_)) => EventAction::Ignore,
        }
    }

    fn handle_remove_event(paths: &Paths, path: PathBuf) -> EventAction {
        match paths.get_watched_file(&path) {
            // We should have dropped the watch of this file in
            // NoticeRemove, so this should not happen - restart
            // watching.
            Some(&WatchedFile::Symlink(_)) | Some(&WatchedFile::Directory(_)) | Some(&WatchedFile::Regular(_)) => EventAction::RestartWatching,
            // TODO document when this can happen
            _ => EventAction::SettlePath(path),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::{DirBuilder, File};
    use std::os::unix::fs::symlink;
    use std::path::Path;

    use error::SupError;
    use tempdir::TempDir;

    use super::{Callbacks, FileWatcher};


    struct TestCallbacks {
        temp_dir: TempDir,
        pub appeared_events: i32,
        pub disappeared_events: i32,
    }

    const FILENAME: &str = "peer-watch-file";
    const DATA_DIR_NAME: &str = "..data";
    const TEMP_DATA_DIR_NAME: &str = "..data_tmp";
    const APPEARED_EVENTS_THRESHOLD: i32 = 2;
    const DISAPPEARED_EVENTS_THRESHOLD: i32 = 1;

    impl Callbacks for TestCallbacks {
        fn file_appeared(&mut self, real_path: &Path) {
            self.appeared_events += 1;

            if self.appeared_events == 1 {
                // Create new timestamped directory.
                let new_timestamped_dir = self.temp_dir.path().join("bar");
                DirBuilder::new().create(&new_timestamped_dir).expect(
                    "creating new timestamped dir",
                );

                // Create temp symlink for the new data dir, i.e. `..data_tmp -> bar`.
                let temp_data_dir_path = self.temp_dir.path().join(TEMP_DATA_DIR_NAME);
                symlink(&new_timestamped_dir, &temp_data_dir_path).expect("creating temporary data dir symlink");

                // Create new file.
                let file_path = new_timestamped_dir.join(&FILENAME);
                File::create(&file_path).expect("creating peer-watch-file in new timestamped dir");

                // Update data to point to the new timestamped dir, using a rename which is atomic on Unix.
                fs::rename(&temp_data_dir_path, &self.temp_dir.path().join(DATA_DIR_NAME)).expect("renaming symlink");
            }
        }

        fn file_modified(&mut self, real_path: &Path) {
        }

        fn file_disappeared(&mut self, real_path: &Path) {
            self.disappeared_events += 1;
        }
        fn listening_for_events(&mut self) {
        }
        fn stopped_listening(&mut self) {
        }
        fn error(&mut self, _: &SupError) -> bool { true }

        fn continue_looping(&mut self) -> bool {
            self.appeared_events < APPEARED_EVENTS_THRESHOLD
        }
    }

    #[test]
    #[cfg(unix)]
    // Implements the steps defined in https://git.io/v5Mz1#L85-L121
    fn k8s_behaviour() {
        let temp_dir = TempDir::new("filewatchertest").expect("creating temp dir");

        let timestamped_dir = temp_dir.path().join("foo");

        DirBuilder::new().create(&timestamped_dir).expect(
            "creating timestamped dir",
        );

        // Create a file in the timestamped dir.
        let file_path = timestamped_dir.join(&FILENAME);
        File::create(&file_path).expect("creating peer-watch-file");

        // Create a data dir as a symlink to a timestamped dir, i.e. `..data -> ..foo`.
        let data_dir_path = temp_dir.path().join(DATA_DIR_NAME);
        symlink(&timestamped_dir, &data_dir_path).expect("creating data dir symlink");

        // Create a relative symlink to the file, i.e. `peer-watch-file -> ..data/peer-watch-file`.
        let file_symlink_src = data_dir_path.join(&FILENAME);
        let file_symlink_dest = temp_dir.path().join(&FILENAME);
        symlink(&file_symlink_src, &file_symlink_dest).expect("creating first file symlink");

        // Create file watcher.
        let cb = TestCallbacks {
            appeared_events: 0,
            disappeared_events: 0,
            temp_dir: temp_dir,
        };
        let mut fw = FileWatcher::new(&file_symlink_dest, cb).expect("creating file watcher");
        fw.run().expect("running file watcher");

        // Remove old timestamped dir.
        fs::remove_dir_all(timestamped_dir).unwrap();

        // The first appeared event is emitted when the watcher finds the already-existing file.
        assert_eq!(fw.callbacks.appeared_events, APPEARED_EVENTS_THRESHOLD, "appeared events");
        assert_eq!(fw.callbacks.disappeared_events, DISAPPEARED_EVENTS_THRESHOLD, "disappeared events");
    }
}
