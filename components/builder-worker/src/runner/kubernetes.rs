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

use std::io;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};

use hab_core::env;
use hab_core::fs as hfs;

use error::{Error, Result};
use runner::log_pipe::LogPipe;
use runner::{NONINTERACTIVE_ENVVAR, RUNNER_DEBUG_ENVVAR};
use runner::workspace::Workspace;

lazy_static! {
    /// Absolute path to the Docker exporter program
    static ref KUBERNETES_EXPORTER_PROGRAM: PathBuf = hfs::resolve_cmd_in_pkg(
        "hab-pkg-export-kubernetes",
        include_str!(concat!(env!("OUT_DIR"), "/KUBERNETES_EXPORTER_PKG_IDENT")),
    );
}

const KUBECONFIG_ENVVAR: &'static str = "KUBECONFIG";

pub struct KubernetesExporterSpec {
    pub kubeconfig_path: String,
    pub replicas: i64,
}

pub struct KubernetesExporter<'a> {
    spec: KubernetesExporterSpec,
    workspace: &'a Workspace,
    bldr_url: &'a str,
}

impl<'a> KubernetesExporter<'a> {
    /// Creates a new Kubernetes exporter for a given `Workspace` and Builder URL.
    pub fn new(spec: KubernetesExporterSpec, workspace: &'a Workspace, bldr_url: &'a str) -> Self {
        KubernetesExporter {
            spec: spec,
            workspace: workspace,
            bldr_url: bldr_url,
        }
    }

    /// Spawns a Kubernetes export command, pipes output streams to the given `LogPipe`
    /// and returns the process' `ExitStatus`.
    ///
    /// # Errors
    ///
    /// * If the child process can't be spawned
    /// * If the calling thread can't wait on the child process
    /// * If the `LogPipe` fails to pipe output
    pub fn export(&self, log_pipe: &mut LogPipe) -> Result<ExitStatus> {
        let exporter = self.spawn_exporter().map_err(Error::Exporter)?;

        let exit_status = self.apply_to_cluster(exporter, log_pipe)?;
        debug!(
            "completed kubernetes export command, status={:?}",
            exit_status
        );
        Ok(exit_status)
    }

    fn spawn_exporter(&self) -> io::Result<Child> {

        let mut cmd = Command::new(&*KUBERNETES_EXPORTER_PROGRAM);
        cmd.current_dir(self.workspace.root());

        cmd.arg("--count");
        cmd.arg(format!("{}", self.spec.replicas));
        cmd.arg("--output");
        cmd.arg("-");
        cmd.arg(self.workspace.job.get_project().get_name()); // Locally built artifact

        debug!(
            "building kubernetes export command, cmd={}",
            format!("building kubernetes export command, cmd={:?}", &cmd)
        );
        cmd.env_clear();
        if let Some(_) = env::var_os(RUNNER_DEBUG_ENVVAR) {
            cmd.env("RUST_LOG", "debug");
        }
        cmd.env(NONINTERACTIVE_ENVVAR, "true"); // Disables progress bars
        cmd.env("TERM", "xterm-256color"); // Emits ANSI color codes

        cmd.stdout(Stdio::piped());

        debug!("spawning kubernetes export command");
        cmd.spawn()
    }

    fn apply_to_cluster(&self, exporter: Child, log_pipe: &mut LogPipe) -> Result<ExitStatus> {

        let mut cmd = Command::new("/usr/local/bin/kubectl");
        cmd.arg("apply");
        cmd.arg("-f");
        cmd.arg("-");

        debug!("building kubectl command, cmd={:?}", &cmd);
        cmd.env_clear();
        if let Some(_) = env::var_os(RUNNER_DEBUG_ENVVAR) {
            cmd.env("RUST_LOG", "debug");
        }
        cmd.env(NONINTERACTIVE_ENVVAR, "true"); // Disables progress bars
        cmd.env("TERM", "xterm-256color"); // Emits ANSI color codes
        debug!(
            "setting kubectl command env, {}={}",
            KUBECONFIG_ENVVAR,
            &self.spec.kubeconfig_path
        );
        cmd.env(KUBECONFIG_ENVVAR, &self.spec.kubeconfig_path); // Use the job-specific `dockerd`

        cmd.stdin(exporter.stdout.unwrap());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        debug!("spawning kubectl command");
        let mut child = cmd.spawn().map_err(Error::Exporter)?;
        log_pipe.pipe(&mut child)?;
        let exit_status = child.wait().map_err(Error::Exporter)?;
        debug!("deploying to cluster, status={:?}", exit_status);
        Ok(exit_status)
    }
}
