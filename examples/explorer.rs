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

use qwal::WalFile;

#[cfg_attr(feature = "async-std", async_std::main)]
#[cfg_attr(feature = "tokio", tokio::main)]
async fn main() {
    let file = std::env::args()
        .nth(1)
        .expect("One argument needs to be supplie");
    print!("File: {file}");

    WalFile::inspect::<_, Vec<u8>>(&file)
        .await
        .expect("could not open file");
}
