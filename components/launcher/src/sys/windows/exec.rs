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

use std::ffi::OsStr;

use core::os::process::windows_child::Child;
use launcher_protocol::message::launcher::Spawn;

use error::Result;

fn run(cmd: Spawn) -> Result<Service> {
    let ps_cmd = format!("iex $(gc {} | out-string)", path.as_ref().to_string_lossy());
    let args = vec!["-command", ps_cmd.as_str()];
    Child::spawn("powershell.exe", args, &pkg.env)?;
    // JW TODO: Get the PID
    Ok(0)
}
