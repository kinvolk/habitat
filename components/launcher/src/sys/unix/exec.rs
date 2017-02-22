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

use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};

use core::os;
use libc;
use protocol::Spawn;

use error::{Error, Result};
use exec::Service;

pub fn run(msg: Spawn) -> Result<Service> {
    let mut cmd = Command::new(msg.get_binary());
    let uid = os::users::get_uid_by_name(msg.get_svc_user()).ok_or(
        Error::UserNotFound(msg.get_svc_user().to_string()),
    )?;
    let gid = os::users::get_gid_by_name(msg.get_svc_group()).ok_or(
        Error::GroupNotFound(msg.get_svc_group().to_string()),
    )?;
    // we want the command to spawn processes in their own process group
    // and not the same group as the Launcher. Otherwise if a child process
    // sends SIGTERM to the group, the Launcher could be terminated.
    cmd.before_exec(|| {
        unsafe {
            libc::setpgid(0, 0);
        }
        Ok(())
    });
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .uid(uid)
        .gid(gid);
    for (key, val) in msg.get_env().iter() {
        cmd.env(key, val);
    }
    let child = cmd.spawn().map_err(Error::Spawn)?;
    Ok(Service::new(msg, child))
}
