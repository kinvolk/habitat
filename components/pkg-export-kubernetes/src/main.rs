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

#[macro_use]
extern crate clap;
extern crate env_logger;
extern crate habitat_core as hcore;
extern crate habitat_common as common;
#[macro_use]
extern crate log;

use clap::App;
use std::fmt::Result;

use hcore::env as henv;
use hcore::PROGRAM_NAME;
use common::ui::{Coloring, UI, NOCOLORING_ENVVAR, NONINTERACTIVE_ENVVAR};

fn main() {
    env_logger::init().unwrap();
    let mut ui = ui();
    if let Err(e) = start(&mut ui) {
        ui.fatal(e).unwrap();
        std::process::exit(1)
    }
}

fn ui() -> UI {
    let isatty = if henv::var(NONINTERACTIVE_ENVVAR)
        .map(|val| val == "true")
        .unwrap_or(false)
    {
        Some(false)
    } else {
        None
    };
    let coloring = if henv::var(NOCOLORING_ENVVAR)
        .map(|val| val == "true")
        .unwrap_or(false)
    {
        Coloring::Never
    } else {
        Coloring::Auto
    };
    UI::default_with(coloring, isatty)
}

fn start(ui: &mut UI) -> Result {
    let m = cli().get_matches();
    debug!("clap cli args: {:?}", m);
    let count = m.value_of("COUNT").unwrap_or("1");
    let pkg_ident = m.value_of("PKG_IDENT").unwrap();
    println!(r###"
## Secret for initial configuration.
#apiVersion: v1
#kind: Secret
#metadata:
#  name: user-toml-secret
#type: Opaque
#data:
## Each configuration item needs to be encoded in base64.
## Plain text content of the secret: "port = 4444"
#  user.toml: cG9ydCA9IDQ0NDQ=
#---
apiVersion: habitat.sh/v1
kind: Habitat
metadata:
  ## name of the Habitat service.
  name: {metadata_name}
spec:
  ## image is the name of the Habitat service package exported as a Docker image.
  image: {image}
  ## count is the number of desired instances.
  count: {count}
  ## service is an object containing parameters that effect how the Habitat service is executed.
  service:
    ## topology refers to the Habitat topology of the service.
    topology: standalone
    ## group referes to a Habitat service group name, a logical grouping of services with the same package.
    group: default
    ## configSecretName is the name of the configuration secret. Edit the Kubernetes Secret at the top.
    #configSecretName: user-toml-secret
"###, metadata_name=pkg_ident.name, image=pkg_ident, count=count);
    Ok(())
}

fn cli<'a, 'b>() -> App<'a, 'b> {
    let name: &str = &*PROGRAM_NAME;
    clap_app!((name) =>
        (about: "Creates a Kubernetes manifest for a Habitat package. Habitat operator must be deployed within the Kubernetes cluster to intercept the created objects.")
        (version: "TODO: no idea where to get it")
        (author: "\nAuthors: The Habitat Maintainers <humans@habitat.sh>\n\n")
        (@arg COUNT: --("count") +takes_value
            "Count in manifest")
        (@arg PKG_IDENT: +required
            "Habitat package identifier (ex: acme/redis)")
    )
}
