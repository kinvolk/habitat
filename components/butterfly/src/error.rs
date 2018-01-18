// Copyright (c) 2016-2017 Chef Software Inc. and/or applicable contributors
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

use std::error;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::result;
use std::str;

use habitat_core;
use protobuf;
use toml;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    BadDataPath(PathBuf, io::Error),
    CannotBind(io::Error),
    DatFileIO(PathBuf, io::Error),
    GossipChannelSetupError(String),
    GossipReceiveError(String),
    GossipReceiveIOError(io::Error),
    GossipSendError(String),
    GossipSendIOError(io::Error),
    HabitatCore(habitat_core::error::Error),
    NonExistentRumor(String, String),
    ProtobufError(protobuf::ProtobufError),
    ServiceConfigDecode(String, toml::de::Error),
    ServiceConfigNotUtf8(String, str::Utf8Error),
    SwimChannelSetupError(String),
    SwimReceiveError(String),
    SwimReceiveIOError(io::Error),
    SwimSendError(String),
    SwimSendIOError(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match *self {
            Error::BadDataPath(ref path, ref err) => {
                format!(
                    "Unable to read or write to data directory, {}, {}",
                    path.display(),
                    err
                )
            }
            Error::CannotBind(ref err) => format!("Cannot bind to port: {:?}", err),
            Error::DatFileIO(ref path, ref err) => {
                format!(
                    "Error reading or writing to DatFile, {}, {}",
                    path.display(),
                    err
                )
            }
            Error::GossipChannelSetupError(ref err) => {
                format!("Error setting up gossip channel: {}", err)
            }
            Error::GossipReceiveError(ref err) => {
                format!("Failed to receive data with gossip receiver: {}", err)
            }
            Error::GossipReceiveIOError(ref err) => {
                format!("Failed to receive data with gossip receiver: {}", err)
            }
            Error::GossipSendError(ref err) => {
                format!("Failed to send data with gossip sender: {}", err)
            }
            Error::GossipSendIOError(ref err) => {
                format!("Failed to send data with gossip sender: {}", err)
            }
            Error::HabitatCore(ref err) => format!("{}", err),
            Error::NonExistentRumor(ref member_id, ref rumor_id) => {
                format!(
                    "Non existent rumor asked to be written to bytes: {} {}",
                    member_id,
                    rumor_id
                )
            }
            Error::ProtobufError(ref err) => format!("ProtoBuf Error: {}", err),
            Error::ServiceConfigDecode(ref sg, ref err) => {
                format!("Cannot decode service config: group={}, {:?}", sg, err)
            }
            Error::ServiceConfigNotUtf8(ref sg, ref err) => {
                format!("Cannot read service configuration: group={}, {}", sg, err)
            }
            Error::SwimChannelSetupError(ref err) => {
                format!("Error setting up SWIM channel: {}", err)
            }
            Error::SwimReceiveError(ref err) => {
                format!("Failed to receive data from SWIM channel: {}", err)
            }
            Error::SwimReceiveIOError(ref err) => {
                format!("Failed to receive data from SWIM channel: {}", err)
            }
            Error::SwimSendError(ref err) => {
                format!("Failed to send data to SWIM channel: {}", err)
            }
            Error::SwimSendIOError(ref err) => {
                format!("Failed to send data to SWIM channel: {}", err)
            }
        };
        write!(f, "{}", msg)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::BadDataPath(_, _) => "Unable to read or write to data directory",
            Error::CannotBind(_) => "Cannot bind to port",
            Error::DatFileIO(_, _) => "Error reading or writing to DatFile",
            Error::GossipChannelSetupError(_) => "Error setting up gossip channel",
            Error::GossipReceiveError(_) => "Failed to receive data with gossip receiver",
            Error::GossipReceiveIOError(_) => "Failed to receive data with gossip receiver",
            Error::GossipSendError(_) => "Failed to send data with gossip sender",
            Error::GossipSendIOError(_) => "Failed to send data with gossip sender",
            Error::HabitatCore(_) => "Habitat core error",
            Error::NonExistentRumor(_, _) => {
                "Cannot write rumor to bytes because it does not exist"
            }
            Error::ProtobufError(ref err) => err.description(),
            Error::ServiceConfigDecode(_, _) => "Cannot decode service config into TOML",
            Error::ServiceConfigNotUtf8(_, _) => "Cannot read service config bytes to UTF-8",
            Error::SwimChannelSetupError(_) => "Error setting up SWIM channel",
            Error::SwimReceiveError(_) => "Failed to receive data from SWIM channel",
            Error::SwimReceiveIOError(_) => "Failed to receive data from SWIM channel",
            Error::SwimSendError(_) => "Failed to send data to SWIM channel",
            Error::SwimSendIOError(_) => "Failed to send data to SWIM channel",
        }
    }
}

impl From<protobuf::ProtobufError> for Error {
    fn from(err: protobuf::ProtobufError) -> Error {
        Error::ProtobufError(err)
    }
}

impl From<habitat_core::error::Error> for Error {
    fn from(err: habitat_core::error::Error) -> Error {
        Error::HabitatCore(err)
    }
}
