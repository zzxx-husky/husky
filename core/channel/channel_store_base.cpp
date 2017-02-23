// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "channel_store_base.hpp"

#include <string>
#include <unordered_map>

#include "base/session_local.hpp"

namespace husky {

std::vector<int*> ChannelStoreBase::default_channel_id;
std::vector<std::unordered_map<std::string, ChannelBase*>*> ChannelStoreBase::channel_map;
const char* ChannelStoreBase::channel_name_prefix = "default_channel_";

}  // namespace husky
