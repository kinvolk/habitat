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

use error::{Error, Result};
use notify;
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};

const WATCHER_DELAY_MS: u64 = 2_000;
static LOGKEY: &'static str = "PW";

// Callbacks are attached to events.
pub trait Callbacks {
    fn file_appeared(&mut self, real_path: &Path);
    fn file_modified(&mut self, real_path: &Path);
    fn file_disappeared(&mut self, real_path: &Path);
}

#[derive(Clone, Debug, Default)]
struct DirFileName {
    directory: PathBuf,
    file_name: OsString,
}

impl DirFileName {
    // split_path separates the dirname from the basename.
    fn split_path(path: &PathBuf) -> Option<Self> {
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
//
// contents are basically for the process_path function
#[derive(Debug)]
struct ProcessPathArgs {
    // the beginning of the patch to process (very often it is root,
    // but not always)
    path: PathBuf,
    // the rest of the path as components ([habitat-operator, peers])
    path_rest: VecDeque<OsString>,
    // nevermind, I still need to see if I can get this thing removed
    //
    // basically describes the position in the index, used for
    // determining from where we should start reprocessing the path in
    // case of some events.
    index: u32,
    // previous path in chain, usually a parent directory, but in case
    // of symlinks it becomes complicated
    //
    // what I call chain here is a list of item we end up watching.
    // (item - directory, file or symlink)
    prev: Option<PathBuf>,
}

#[derive(Debug)]
// this struct basically tells that for path the previous item in
// chain is prev.
struct ChainLinkInfo {
    path: PathBuf,
    prev: Option<PathBuf>,
}

#[derive(Debug)]
// a struct that gets passed together with some event actions
struct PathsActionData {
    dir_file_name: DirFileName,
    args: ProcessPathArgs,
}

#[derive(Debug)]
// this is stores information about the watched item
struct Common {
    // TODO: maybe drop this? we could also use dir_file_name.as_path()
    path: PathBuf,
    dir_file_name: DirFileName,
    // previous watched item in chain, is None for the first watched item
    prev: Option<PathBuf>,
    // next watched item in chain, is None for the last watched item
    next: Option<PathBuf>,
    // TODO: That was needed to make sure that the generated process
    // args with lower index will overwrite the generated process args
    // with higher index. Several generated process args can be
    // generated when several files or directories are removed. Not
    // sure if we need it anymore, since we have a simple chain of
    // watches, so the most recent removal event should happen for the
    // element in chain with lowest index.
    index: u32,
    // this is the rest of the components that were left to process at
    // the moment we were processing this path. useful for
    // reprocessing the path, when the next item in the list was
    // removed/replaced.
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
//
// This is only used to generate the Common struct for each item we
// have in the path, so for /h-o/peers, it will generate data for /h-o
// and for /h-o/peers.
//
// TODO(krnowak): rename
struct CommonGenerator {
    // What was the last item we processed, complicated in case of
    // symlinks.
    prev: Option<PathBuf>,
    // Normally prev is just a parent directory (previous processed
    // item), but this is not the case when we are dealing with
    // symlinks. keep_prev is used then to avoid setting prev to the
    // previous processed item.
    keep_prev: bool,
    // the path for the generated Common
    //
    // TODO(krnowak): drop it? we likely have this information stored
    // in split_path, we could add a as_path() function to SplitPath.
    path: PathBuf,
    // Same as path, but splitted into dirname and basename
    split_path: SplitPath,
    // nevermind, hopefully to be dropped
    index: u32,
    // the rest of the path as components to be processed
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
            //
            // This is only used for symlinks. We want to make sure
            // that the previous item for the symlink's target is
            // either the symlink or target's parent directory if we
            // didn't watch the directory before.
            //
            // an example - we watch /a/b/c, c is a symlink to /a/x/c,
            // so after processing /a/b/c we want to have a chain like
            // /a, /a/b, /a/x, /a/x/c.
            //
            // so we use keep_prev to make sure that the proper
            // previous item in chain will be set for the paths coming
            // after following the symlink
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
            Component::ParentDir => {
                simple.pop();
            }
            _ => simple.push(c.as_os_str()),
        };
    }
    simple
}

// TODO(asymmetric): what is this?
// TODO(krnowak): this probably should just be merged into Paths
#[derive(Debug)]
struct PathProcessState {
    // start_path is the place in the filesystem tree where watching starts.
    start_path: PathBuf,
    // TODO: Figure out if we can perform loop detection without this
    // hash map, but only using whatever data we have in Paths.
    symlink_loop_catcher: HashMap<
        /* symlink path: */
        PathBuf,
        /* path + path_rest */
        PathBuf,
    >,
    real_file: Option<PathBuf>,
}

// TODO(asymmetric): Document the difference between PathsAction and EventAction.
//
// EventActions are high-level actions to be performed in response to filesystem events.
//
// Basically we translate DebouncedEvent to EventAction, and
// EventAction to a list of PathsActions
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
    RestartWatching,
}

// Paths holds the state with regards to watching.
#[derive(Debug)]
struct Paths {
    // TODO(asymmetric): why do we need paths and dirs?
    //
    // "paths" is what we are interested in: directories, symlinks,
    // files. If something happen to them, we react. "dirs" on the
    // other hand is a map of directories to use count we actually
    // watch with the os specific watcher. Usually these are parent
    // directories of the items in "paths". Use count can be greater
    // than 1 in case of symlinks (watch /a, which points to b, so we
    // end up with two paths, but only one directory to watch with use
    // count 2)
    paths: HashMap<PathBuf, WatchedFile>,
    dirs: HashMap<
        /*watched directory: */
        PathBuf,
        /* watched files count: */
        u32,
    >,
    process_state: PathProcessState,
    // Filled in notice remove, drained in remove and rename.
    paths_to_settle: HashSet<PathBuf>,
    // these args are used to pass them to process args, when
    // paths_to_settle becomes empty
    process_args_after_settle: Option<ProcessPathArgs>,
}

// TODO this could be rename to BranchStatus
//
// Both branch result and leaf result are about the status of adding
// new path to be watched. Branch is about symlinks and directories,
// leaves - about regular files, missing regular files and missing
// directories.
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
// we use this when we settle a path, so we know if we processed a
// path, because all the path were already settled.
enum ProcessPathStatus {
    // Holds a vector of new directories to watch (a result of process_path function)
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
                Component::Prefix(_) |
                Component::RootDir => {
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
            debug!("common.path: {:?}", &common.path);
            debug!("common.path_rest: {:?}", &common.path_rest);
            let dir_file_name = common.dir_file_name.clone();
            debug!("dir_file_name: {:?}", dir_file_name);

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
            debug!("target: {:?}", &target);
            let target_path = if target.is_absolute() {
                target
            } else {
                debug!("directory for target: {:?}", &dir_file_name.directory);
                dir_file_name.directory.join(target)
            };
            let simplified_target = simplify_abs_path(&target_path);
            debug!("simplified target: {:?}", &simplified_target);
            let process_args = Self::path_for_processing(&simplified_target);
            if self.symlink_loop(
                &common.path,
                &common.path_rest,
                &process_args.path,
                &process_args.path_rest,
            )
            {
                // Symlink loop, nothing to watch here - hopefully
                // later some symlink will be rewired to the real
                // file.
                break;
            }

            let branch_result = self.get_or_add_symlink(common);
            self.handle_branch_result(&mut common_generator, branch_result, &mut new_watches);
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
        //
        // We shouldn't, maybe we should warn when it happens?
        if let Some(watched_file) = self.paths.remove(path) {
            let common = watched_file.steal_common();
            let unwatch_directory;
            let dir_path = common.dir_file_name.directory;
            {
                let count = self.get_mut_directory(&dir_path).expect(
                    "expected directory for the watched file",
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
        new_path_rest: &VecDeque<OsString>,
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

    fn add_regular(&mut self, common: Common) -> LeafResult {
        self.add_leaf_watched_file(WatchedFile::Regular(common))
    }

    fn add_missing_regular(&mut self, common: Common) -> LeafResult {
        self.add_leaf_watched_file(WatchedFile::MissingRegular(common))
    }

    fn add_missing_directory(&mut self, common: Common) -> LeafResult {
        self.add_leaf_watched_file(WatchedFile::MissingDirectory(common))
    }

    fn add_leaf_watched_file(&mut self, watched_file: WatchedFile) -> LeafResult {
        let dir_file_name = watched_file.get_common().dir_file_name.clone();
        let needs_watch = self.add_dir(&dir_file_name);
        let chain_link_info = match self.paths.entry(dir_file_name.as_path()) {
            Entry::Occupied(mut o) => {
                // This likely should not happen I think.
                o.insert(watched_file);
                o.get().get_common().get_chain_link_info()
            }
            Entry::Vacant(v) => v.insert(watched_file).get_common().get_chain_link_info(),
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
        let dir_file_name = watched_file.get_common().dir_file_name.clone();
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
        }
        {
            self.process_args_after_settle = Some(args)
        }
    }

    fn process_path_or_defer_if_unsettled(&mut self) -> Option<Vec<PathBuf>> {
        let mut process_args = None;
        swap(&mut process_args, &mut self.process_args_after_settle);
        let (directories, new_args) = match process_args {
            Some(args) => {
                match self.process_path_if_settled(args) {
                    ProcessPathStatus::Executed(v) => (Some(v), None),
                    ProcessPathStatus::NotExecuted(a) => (None, Some(a)),
                }
            }
            None => (None, None),
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

pub struct FileWatcher<C: Callbacks, W: Watcher> {
    callbacks: C,
    // The watcher itself.
    watcher: W,
    // A channel for receiving events.
    rx: Receiver<DebouncedEvent>,
    // The paths to watch.
    paths: Paths,
    initial_real_file: Option<PathBuf>,
}

pub fn default_file_watcher<C, P>(
    path: P,
    callbacks: C,
) -> Result<FileWatcher<C, RecommendedWatcher>>
where
    P: Into<PathBuf>,
    C: Callbacks,
{
    return FileWatcher::<C, RecommendedWatcher>::new(path, callbacks);
}

impl<C: Callbacks, W: Watcher> FileWatcher<C, W> {
    pub fn new<P>(path: P, callbacks: C) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let (tx, rx) = channel();
        let mut watcher = W::new(tx, Duration::from_millis(WATCHER_DELAY_MS))
            .map_err(|err| sup_error!(Error::NotifyCreateError(err)))?;
        let start_path = Self::watcher_path(path.into())?;
        // Initialize the Paths struct, which will hold all state relative to file watching.
        let mut paths = Paths::new(&start_path);

        // Generate list of paths to watch.
        let directories = paths.generate_watch_paths();

        // Start watcher on each path.
        for directory in directories {
            watcher
                .watch(&directory, RecursiveMode::NonRecursive)
                .map_err(|err| sup_error!(Error::NotifyError(err)))?;
        }
        let initial_real_file = paths.process_state.real_file.clone();

        Ok(Self {
            callbacks: callbacks,
            // The watcher itself.
            watcher: watcher,
            // A channel for receiving events.
            rx: rx,
            // The paths to watch.
            paths: paths,
            initial_real_file: initial_real_file,
        })
    }

    #[allow(dead_code)]
    pub fn get_callbacks(&self) -> &C {
        &self.callbacks
    }

    // turns given path to an simplified absolute path.
    //
    // simplified means that it is without . and ..
    fn watcher_path(p: PathBuf) -> Result<PathBuf> {
        let abs_path = if p.is_absolute() {
            p.clone()
        } else {
            let cwd = env::current_dir().map_err(|e| sup_error!(Error::Io(e)))?;
            cwd.join(p)
        };
        let simplified_abs_path = simplify_abs_path(&abs_path);
        match DirFileName::split_path(&simplified_abs_path) {
            Some(_) => Ok(simplified_abs_path),
            None => Err(sup_error!(Error::FileWatcherFileIsRoot)),
        }
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            self.single_iteration()?;
        }
    }

    pub fn single_iteration(&mut self) -> Result<()> {
        if let Some(ref real_file) = self.initial_real_file {
            self.callbacks.file_appeared(real_file);
        }
        self.initial_real_file = None;
        self.rx
            .recv()
            .map_err(|e| sup_error!(Error::RecvError(e)))
            .and_then(|event| self.handle_event(event))
    }

    // TODO(asymmetric): does self need to be mut?
    //
    // Yes, we pass its members around as mutable refs.
    fn handle_event(&mut self, event: DebouncedEvent) -> Result<()> {
        let mut actions = VecDeque::new();

        // Gather the high-level actions.
        actions.extend(Self::get_paths_actions(&self.paths, event));

        debug!("in handle_event fn");
        debug!("paths: {:?}", self.paths);
        debug!("actions: {:?}", actions);
        // Perform lower-level actions.
        while let Some(action) = actions.pop_front() {
            debug!("action {:?}", action);
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
                    if let Some(dir_path) = self.paths.drop_watch(&p) {
                        match self.watcher.unwatch(dir_path) {
                            Ok(_) => (),
                            // These probably may happen when the
                            // directory was removed. Ignore them, as
                            // we wanted to drop the watch anyway.
                            Err(notify::Error::PathNotFound) |
                            Err(notify::Error::WatchNotFound) => (),
                            Err(e) => return Err(sup_error!(Error::NotifyError(e))),
                        }
                    }
                }
                PathsAction::AddPathToSettle(p) => {
                    self.paths.add_path_to_settle(p);
                }
                PathsAction::SettlePath(p) => {
                    self.paths.settle_path(p);
                    actions.extend(self.handle_process_path()?);
                }
                PathsAction::ProcessPathAfterSettle(args) => {
                    self.paths.set_process_args(args);
                    actions.extend(self.handle_process_path()?);
                }
                PathsAction::RestartWatching => {
                    actions.clear();
                    if let Some(ref path) = self.paths.process_state.real_file {
                        actions.push_back(PathsAction::NotifyFileDisappeared(path.clone()));
                    }
                    for directory in self.paths.reset() {
                        match self.watcher.unwatch(directory) {
                            Ok(_) => (),
                            // These probably may happen when the
                            // directory was removed. Ignore them, as
                            // we wanted to drop the watch anyway.
                            Err(notify::Error::PathNotFound) |
                            Err(notify::Error::WatchNotFound) => (),
                            Err(e) => return Err(sup_error!(Error::NotifyError(e))),
                        }
                    }
                    let process_args =
                        Paths::path_for_processing(&self.paths.process_state.start_path);
                    actions.push_back(PathsAction::ProcessPathAfterSettle(process_args));
                }
            }
        }
        Ok(())
    }

    fn handle_process_path(&mut self) -> Result<Vec<PathsAction>> {
        let mut actions = Vec::new();
        match self.paths.process_path_or_defer_if_unsettled() {
            None => (),
            Some(directories) => {
                for directory in directories {
                    self.watcher.watch(&directory, RecursiveMode::NonRecursive)?;
                }
                if let Some(ref path) = self.paths.process_state.real_file {
                    actions.push(PathsAction::NotifyFileAppeared(path.clone()));
                }
            }
        }
        Ok(actions)
    }

    // Maps `EventAction`s to one or more lower-level `PathsAction`s .
    fn get_paths_actions(paths: &Paths, event: DebouncedEvent) -> Vec<PathsAction> {
        let mut actions = Vec::new();
        for event_action in Self::get_event_actions(paths, event) {
            debug!("event_action: {:?}", event_action);
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
            DebouncedEvent::NoticeRemove(ref p) => Self::handle_notice_remove_event(paths, p),
            DebouncedEvent::Remove(p) => Self::handle_remove_event(paths, p),
            DebouncedEvent::Create(ref p) => {
                match paths.get_watched_file(p) {
                    None => EventAction::Ignore,
                    Some(&WatchedFile::MissingRegular(ref c)) => EventAction::AddRegular(
                        c.get_paths_action_data(),
                    ),
                    // Create event for an already existing file or
                    // directory should not happen, restart watching.
                    Some(&WatchedFile::Regular(_)) |
                    Some(&WatchedFile::Directory(_)) => EventAction::RestartWatching,
                    Some(&WatchedFile::Symlink(ref c)) => EventAction::RewireSymlink(
                        c.get_paths_action_data(),
                    ),
                    Some(&WatchedFile::MissingDirectory(ref c)) => EventAction::AddDirectory(
                        c.get_paths_action_data(),
                    ),
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
            Some(&WatchedFile::Directory(ref c)) => EventAction::DropDirectory(
                c.get_paths_action_data(),
            ),
            // This happens when we expected p to be a file, but it
            // was something else and that thing just got removed.
            Some(&WatchedFile::MissingRegular(_)) => EventAction::Ignore,
            Some(&WatchedFile::Regular(ref c)) => EventAction::DropRegular(
                c.get_paths_action_data(),
            ),
            Some(&WatchedFile::Symlink(ref c)) => EventAction::DropSymlink(
                c.get_paths_action_data(),
            ),
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
            Some(&WatchedFile::Symlink(_)) |
            Some(&WatchedFile::Directory(_)) |
            Some(&WatchedFile::Regular(_)) => EventAction::RestartWatching,
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

    use tempdir::TempDir;

    use super::{Callbacks, default_file_watcher};


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
        fn file_appeared(&mut self, _: &Path) {
            self.appeared_events += 1;

            if self.appeared_events == 1 {
                // Create new timestamped directory.
                let new_timestamped_dir = self.temp_dir.path().join("bar");
                DirBuilder::new().create(&new_timestamped_dir).expect(
                    "creating new timestamped dir",
                );

                // Create temp symlink for the new data dir, i.e. `..data_tmp -> bar`.
                let temp_data_dir_path = self.temp_dir.path().join(TEMP_DATA_DIR_NAME);
                symlink(&new_timestamped_dir, &temp_data_dir_path).expect(
                    "creating temporary data dir symlink",
                );

                // Create new file.
                let file_path = new_timestamped_dir.join(&FILENAME);
                File::create(&file_path).expect("creating peer-watch-file in new timestamped dir");

                // Update data to point to the new timestamped dir,
                // using a rename which is atomic on Unix.
                fs::rename(
                    &temp_data_dir_path,
                    &self.temp_dir.path().join(DATA_DIR_NAME),
                ).expect("renaming symlink");
            }
        }

        fn file_modified(&mut self, real_path: &Path) {
            debug!("file {:?} modified!", real_path);
        }

        fn file_disappeared(&mut self, real_path: &Path) {
            debug!("file {:?} disappeared!", real_path);
            self.disappeared_events += 1;
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
        debug!("creating file in {:?}", &file_path);
        File::create(&file_path).expect("creating peer-watch-file");

        // Create a data dir as a symlink to a timestamped dir, i.e. `..data -> ..foo`.
        let data_dir_path = temp_dir.path().join(DATA_DIR_NAME);
        debug!("symlinking {:?} -> {:?}", &data_dir_path, &timestamped_dir);
        symlink(&timestamped_dir, &data_dir_path).expect("creating data dir symlink");

        // Create a relative symlink to the file, i.e. `peer-watch-file -> ..data/peer-watch-file`.
        let file_symlink_src = data_dir_path.join(&FILENAME);
        let file_symlink_dest = temp_dir.path().join(&FILENAME);
        debug!(
            "symlinking {:?} -> {:?}",
            &file_symlink_dest,
            &file_symlink_src
        );
        symlink(&file_symlink_src, &file_symlink_dest).expect("creating first file symlink");

        // Create file watcher.
        debug!("watching {:?}", &file_path);
        let cb = TestCallbacks {
            appeared_events: 0,
            disappeared_events: 0,
            temp_dir: temp_dir,
        };
        let mut fw = default_file_watcher(&file_symlink_dest, cb).expect("creating file watcher");
        while fw.get_callbacks().appeared_events < APPEARED_EVENTS_THRESHOLD {
            fw.single_iteration().expect("iteration succeeds");
        }

        // Remove old timestamped dir.
        fs::remove_dir_all(timestamped_dir).unwrap();

        // The first appeared event is emitted when the watcher finds the already-existing file.
        assert_eq!(
            fw.callbacks.appeared_events,
            APPEARED_EVENTS_THRESHOLD,
            "appeared events"
        );
        assert_eq!(
            fw.callbacks.disappeared_events,
            DISAPPEARED_EVENTS_THRESHOLD,
            "disappeared events"
        );
    }
}
