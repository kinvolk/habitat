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

use protocol;

use super::{Handler, HandleResult};
use exec;
use server::ServiceTable;

pub struct RestartHandler;
impl Handler for RestartHandler {
    type Message = protocol::Restart;
    type Reply = protocol::SpawnOk;

    fn handle(msg: Self::Message, services: &mut ServiceTable) -> HandleResult<Self::Reply> {
        let mut service = match services.remove(msg.get_pid()) {
            Some(service) => service,
            None => {
                let mut reply = protocol::NetErr::new();
                reply.set_code(protocol::ErrCode::NoPID);
                return Err(reply);
            }
        };
        service.kill();
        match service.wait() {
            Ok(_status) => {
                match exec::run(service.take_args()) {
                    Ok(new_service) => {
                        let mut reply = protocol::SpawnOk::new();
                        reply.set_pid(new_service.id());
                        services.insert(new_service);
                        Ok(reply)
                    }
                    Err(err) => Err(protocol::error(err)),
                }
            }
            Err(err) => {
                let mut reply = protocol::NetErr::new();
                reply.set_msg(err.to_string());
                reply.set_code(protocol::ErrCode::SigErr);
                Err(reply)
            }
        }
    }
}
