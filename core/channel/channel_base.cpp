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

#include "core/channel/channel_base.hpp"

#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/worker_info.hpp"
#include "core/shard.hpp"

namespace husky {

thread_local int ChannelBase::max_channel_id_ = 0;

ChannelBase::ChannelBase() : channel_id_(max_channel_id_), progress_(0) { max_channel_id_ += 1; }

void ChannelBase::setup(LocalMailbox* mailbox) {
    set_mailbox(mailbox);
}

void ChannelBase::inc_progress() { progress_ += 1; }

}  // namespace husky
