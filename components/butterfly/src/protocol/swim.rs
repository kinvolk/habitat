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

include!("../generated/butterfly.swim.rs");

pub use self::{membership::Health, swim::Payload as SwimPayload, swim::Type as SwimType};

#[cfg(test)]
mod tests {
    use super::*;

    // Theis test assures that we can properly compare Health values
    // along the spectrum of
    //
    //   Alive < Suspect < Confirmed < Departed
    //
    // since that is important in our decision whether or not to
    // propagate membership rumors.
    #[test]
    fn health_is_properly_ordered() {
        assert!(Health::Alive < Health::Suspect);
        assert!(Health::Suspect < Health::Confirmed);
        assert!(Health::Confirmed < Health::Departed);
    }
}