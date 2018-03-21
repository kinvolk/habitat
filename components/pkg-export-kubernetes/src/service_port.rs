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

use std::str::FromStr;
use std::result;
use clap::ArgMatches;
use serde_json;

use export_docker::Result;

use error::Error;

#[derive(Clone, Debug)]
pub struct ServicePort {
    pub port: u16,
    pub external_port: u16,
}

impl ServicePort {
    pub fn from_args(matches: &ArgMatches) -> Result<Vec<Self>> {
        let mut ports = Vec::new();

        if let Some(port_args) = matches.values_of("PORT") {
            for arg in port_args {
                let b = arg.parse::<Self>()?;

                ports.push(b);
            }
        };

        Ok(ports)
    }

    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "service_port": self.port,
            "external_port": self.external_port,
        })
    }
}

impl FromStr for ServicePort {
    type Err = Error;

    fn from_str(port_str: &str) -> result::Result<Self, Self::Err> {
        let values: Vec<&str> = port_str.split(':').collect();
        if values.len() < 1 || values.len() > 2 {
            return Err(invalid_port_err(port_str));
        }

        let port: u16 = values[0].parse().map_err(|_| invalid_port_err(values[0]))?;
        let p = values.get(1).unwrap_or(&values[0]);
        let external_port = p.parse().map_err(|_| invalid_port_err(p))?;

        Ok(ServicePort {
            port,
            external_port,
        })
    }
}

fn invalid_port_err(s: &str) -> Error {
    Error::InvalidPort(s.to_string())
}
