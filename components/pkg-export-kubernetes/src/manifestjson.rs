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

use failure::SyncFailure;
use handlebars::Handlebars;
use serde_json::Value;

use export_docker::Result;

use manifest::Manifest;

// Kubernetes manifest template
const MANIFESTFILE: &'static str = include_str!("../defaults/KubernetesManifest.hbs");
const BINDFILE: &'static str = include_str!("../defaults/KubernetesBind.hbs");

/// Represents the [`Manifest`] in JSON format. This is an intermediate type that can be converted
/// to the final manifest YAML file content, ready for consumption by a Kubernetes cluster.
///
/// The reason for the existence of this intermediate type is to allow users of this crate to be
/// able to modify the JSON before converting it to the final manifest string.
///
/// [`Manifest`]: ../manifest/struct.Manifest.html
pub struct ManifestJson {
    /// JSON object, holding values for the main body of the YAML content.
    pub main: Value,
    /// JSON representations of [`Bind`] instances.
    ///
    /// [`Bind`]: ../bind/struct.Bind.html
    pub binds: Vec<Value>,
}

impl ManifestJson {
    /// Create a `ManifestJson` from `manifest`.
    pub fn new(manifest: &Manifest) -> Self {
        let main = json!({
            "metadata_name": manifest.metadata_name,
            "habitat_name": manifest.habitat_name,
            "image": manifest.image,
            "count": manifest.count,
            "service_topology": manifest.service_topology.to_string(),
            "service_group": manifest.service_group,
            "config": manifest.config,
            "ring_secret_name": manifest.ring_secret_name,
            "bind": !manifest.binds.is_empty()
        });

        let mut binds = Vec::new();
        for bind in &manifest.binds {
            let json = json!({
                "name": bind.name.clone(),
                "service": bind.service.clone(),
                "group": bind.group.clone(),
            });

            binds.push(json);
        }

        ManifestJson {
            main: main,
            binds: binds,
        }
    }

    /// Convert into a string. The returned string is the final manifest YAML file content, ready
    /// for consumption by a Kubernetes cluster.
    ///
    /// # Errors
    ///
    /// * Rendering from the template fails. This can only happen if the template has a syntax
    /// error or the command-line arguments are incorrectly formatted. In both cases, it's a
    /// most likely a programmer error if it happens.
    // TODO: Implement TryInto trait instead when it's in stable std crate
    pub fn into_string(&self) -> Result<String> {
        let r = Handlebars::new()
            .template_render(MANIFESTFILE, &self.main)
            .map_err(SyncFailure::new)?;
        let mut s = r.lines().filter(|l| *l != "").collect::<Vec<_>>().join(
            "\n",
        ) + "\n";

        for bind in &self.binds {
            s += &Handlebars::new().template_render(BINDFILE, &bind).map_err(
                SyncFailure::new,
            )?;
        }

        Ok(s)
    }
}
