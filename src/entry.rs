// Copyright 2021, The Tremor Team
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

/// Represents a serializable entry in the write-ahead-log
pub trait Entry {
    type Output;
    fn serialize(self) -> Vec<u8>;
    fn deserialize(data: Vec<u8>) -> Self::Output;
}

impl Entry for Vec<u8> {
    type Output = Vec<u8>;
    fn serialize(self) -> Vec<u8> {
        self
    }

    fn deserialize(data: Vec<u8>) -> Self::Output {
        data
    }
}

impl Entry for &[u8] {
    type Output = Vec<u8>;
    fn serialize(self) -> Vec<u8> {
        self.to_vec()
    }

    fn deserialize(data: Vec<u8>) -> Self::Output {
        data
    }
}