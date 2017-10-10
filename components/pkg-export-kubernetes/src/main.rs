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

# Use this if you want to deploy the service in the Kubernetes namespace.
# apiVersion: v1
# kind: Namespace
# metadata:
#   name: example-namespace
# ---
apiVersion: habitat.sh/v1
kind: Habitat
metadata:
  name: example-standalone-habitat
  # Uncomment this to run this service in the Kubernetes namespace. Note that this
  # requires that the namespace is already defined. An example of the definition
  # is at the top of the file.
  # namespace: example-namespace
spec:
  # the core/nginx habitat service packaged as a Docker image
  image: {image}
  count: {count}
  service:
    topology: standalone
    # Uncomment this if you want to have the service in a different group (by
    # default it is "default").
    # group: Foobar
"###, image=pkg_ident, count=count);
    Ok(())
}

fn cli<'a, 'b>() -> App<'a, 'b> {
    let name: &str = &*PROGRAM_NAME;
    clap_app!((name) =>
        (about: "Creates a Kubernetes manifest for a Habitat package")
        (version: "TODO: no idea where to get it")
        (author: "\nAuthors: The Habitat Maintainers <humans@habitat.sh>\n\n")
        (@arg COUNT: --("count") +takes_value
            "Count in manifest")
        (@arg PKG_IDENT: +required
            "Habitat package identifier (ex: acme/redis)")
    )
}
