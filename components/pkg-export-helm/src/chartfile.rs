// Copyright (c) 2018 Chef Software Inc. and/or applicable contributors
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

use failure::SyncFailure;
use handlebars::Handlebars;
use serde_json::Value;

use export_docker::Result;

// Keep the default version in main::cli() in sync with this one
pub const DEFAULT_VERSION: &'static str = "0.0.1";

// Helm chart file template
const CHARTFILE: &'static str = include_str!("../defaults/HelmChartFile.hbs");

pub struct ChartFile {
    json: Value,
}

impl ChartFile {
    pub fn new(name: &str, version: Option<&str>, description: Option<&str>) -> Result<Self> {
        let json = json!({
            "name": name,
            "version": version.unwrap_or(DEFAULT_VERSION),
            "description": description,
        });

        Ok(ChartFile { json })
    }

    // TODO: Implement TryInto trait instead when it's in stable std crate
    pub fn into_string(&self) -> Result<String> {
        let r = Handlebars::new()
            .template_render(CHARTFILE, &self.json)
            .map_err(SyncFailure::new)?;
        let s = r.lines().filter(|l| *l != "").collect::<Vec<_>>().join(
            "\n",
        ) + "\n";

        Ok(s)
    }
}
