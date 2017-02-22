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

use std::io::{self, BufRead, BufReader, Write};
use std::ops::Neg;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, ChildStderr, ChildStdout, ExitStatus};
use std::thread::{self, JoinHandle};

use ansi_term::Colour;
use core::os::process::{signal, Signal};
use libc::{self, c_int, pid_t};
use protocol;
use time::{Duration, SteadyTime};

pub use sys::exec::*;
use error::{Error, Result};

#[derive(Debug)]
pub struct Service {
    args: protocol::Spawn,
    pid: pid_t,
    status: Option<ExitStatus>,
    out_reader: Option<JoinHandle<()>>,
    err_reader: Option<JoinHandle<()>>,
}

impl Service {
    pub fn new(spawn: protocol::Spawn, child: Child) -> Self {
        let mut out_reader = None;
        let mut err_reader = None;
        let pid = child.id() as pid_t;
        if let Some(stdout) = child.stdout {
            let id = spawn.get_id().to_string();
            out_reader = thread::Builder::new()
                .name(format!("{}-out", spawn.get_id()))
                .spawn(move || pipe_stdout(stdout, id))
                .ok();
        }
        if let Some(stderr) = child.stderr {
            let id = spawn.get_id().to_string();
            err_reader = thread::Builder::new()
                .name(format!("{}-err", spawn.get_id()))
                .spawn(move || pipe_stderr(stderr, id))
                .ok();
        }
        Service {
            args: spawn,
            pid: pid,
            status: None,
            out_reader: out_reader,
            err_reader: err_reader,
        }
    }

    pub fn args(&self) -> &protocol::Spawn {
        &self.args
    }

    pub fn id(&self) -> u32 {
        self.pid as u32
    }

    /// Attempt to gracefully terminate a proccess and then forcefully kill it after
    /// 8 seconds if it has not terminated.
    pub fn kill(&mut self) -> protocol::ShutdownMethod {
        // check the group of the process being killed
        // if it is the root process of the process group
        // we send our signals to the entire process group
        // to prevent orphaned processes.
        let pgid = unsafe { libc::getpgid(self.pid as pid_t) };
        if self.pid == pgid {
            debug!(
                "pid to kill {} is the process group root. Sending signal to process group.",
                self.pid
            );
            // sending a signal to the negative pid sends it to the
            // entire process group instead just the single pid
            self.pid = self.pid.neg();
        }

        // JW TODO: Determine if the error represents a case where the process was already
        // exited before we return out and assume so.
        if signal(self.id(), Signal::TERM).is_err() {
            return protocol::ShutdownMethod::AlreadyExited;
        }
        let stop_time = SteadyTime::now() + Duration::seconds(8);
        loop {
            if let Ok(Some(_status)) = self.try_wait() {
                return protocol::ShutdownMethod::GracefulTermination;
            }
            if SteadyTime::now() < stop_time {
                continue;
            }
            // JW TODO: Determine if the error represents a case where the process was already
            // exited before we return out and assume so.
            if signal(self.id(), Signal::KILL).is_err() {
                return protocol::ShutdownMethod::GracefulTermination;
            }
            return protocol::ShutdownMethod::Killed;
        }
    }

    pub fn name(&self) -> &str {
        self.args.get_id()
    }

    pub fn take_args(self) -> protocol::Spawn {
        self.args
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        let mut status = 0 as c_int;
        match unsafe { libc::waitpid(self.pid, &mut status, libc::WNOHANG) } {
            0 => Ok(None),
            -1 => Err(Error::ExecWait(io::Error::last_os_error())),
            _ => {
                self.status = Some(ExitStatus::from_raw(status));
                Ok(Some(ExitStatus::from_raw(status)))
            }
        }
    }

    pub fn wait(&mut self) -> Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        let mut status = 0 as c_int;
        match unsafe { libc::waitpid(self.pid, &mut status, 0) } {
            -1 => Err(Error::ExecWait(io::Error::last_os_error())),
            _ => {
                self.status = Some(ExitStatus::from_raw(status));
                Ok(ExitStatus::from_raw(status))
            }
        }
    }
}

/// Consume output from a child process until EOF, then finish
fn pipe_stdout(out: ChildStdout, id: String) {
    let mut reader = BufReader::new(out);
    let mut buffer = String::new();
    while reader.read_line(&mut buffer).unwrap() > 0 {
        let mut line = output_format!(preamble &id, logkey "O");
        line.push_str(&buffer);
        write!(&mut io::stdout(), "{}", line).expect("unable to write to stdout");
        buffer.clear();
    }
}

/// Consume standard error from a child process until EOF, then finish
fn pipe_stderr(err: ChildStderr, id: String) {
    let mut reader = BufReader::new(err);
    let mut buffer = String::new();
    while reader.read_line(&mut buffer).unwrap() > 0 {
        let mut line = output_format!(preamble &id, logkey "E");
        let c = format!("{}", Colour::Red.bold().paint(buffer.clone()));
        line.push_str(c.as_str());
        write!(&mut io::stderr(), "{}", line).expect("unable to write to stderr");
        buffer.clear();
    }
}
