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

#include "core/channel/aggregator_channel.hpp"

#include <functional>
#include <vector>

#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/mailbox.hpp"
#include "core/shard.hpp"
#include "core/utils.hpp"

namespace husky {

AggregatorChannel::AggregatorChannel() {}

AggregatorChannel::~AggregatorChannel() {}

void AggregatorChannel::default_setup(LocalMailbox* mailbox, std::function<void()> something = nullptr) {
    do_something = something;
    setup(mailbox);
}

// Mark these member function private to avoid being used by users
void AggregatorChannel::send(std::vector<BinStream>& bins) {
    this->inc_progress();
    ASSERT_MSG(bins.size() == this->destination_->get_num_shards(),
       ("Number of messages to send is expected to be number of workers, expected: " +
        std::to_string(this->destination_->get_num_local_shards()) +
        ", in fact: " + std::to_string(bins.size())).c_str());

    auto shard_info_iter = ShardInfoIter(*this->destination_);
    for (int i = 0; i < bins.size(); i++) {
        auto pid_and_sid = shard_info_iter.next();
        if (bins[i].size() > 0) {
            this->mailbox_->send(pid_and_sid.first, pid_and_sid.second,
                this->channel_id_, this->progress_, bins[i]);
            bins[i].clear();
        }
    }
    this->mailbox_->send_complete(this->channel_id_, this->progress_, this->source_->get_num_local_shards(),
                                  this->destination_->get_pids());
}

bool AggregatorChannel::poll() { return this->mailbox_->poll(this->channel_id_, this->progress_); }

BinStream AggregatorChannel::recv_() { return this->mailbox_->recv(this->channel_id_, this->progress_); }

void AggregatorChannel::prepare() {}

void AggregatorChannel::in(BinStream& bin) {}

void AggregatorChannel::out() {
    if (do_something != nullptr)
        do_something();
}

void AggregatorChannel::customized_setup() {}

void AggregatorChannel::set_source(Shard* source) { source_ = source; }

void AggregatorChannel::set_destination(Shard* destination) { destination_ = destination; }

}  // namespace husky
