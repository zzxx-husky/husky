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

#pragma once

#include <functional>
#include <vector>

#include "base/serialization.hpp"
#include "core/accessor_store.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/channel_source.hpp"
#include "core/combiner.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/shard.hpp"

namespace husky {

using base::BinStream;

template <typename KeyT, typename ValueT>
class BroadcastChannel : public ChannelBase, public Shard {
   public:
    BroadcastChannel() = default;

    ~BroadcastChannel() override {
        // Make sure to invoke inc_progress_ before destructor
        if (need_leave_accessor_)
            leave_accessor();
        AccessorStore::remove_accessor(channel_id_);
    }

    BroadcastChannel(const BroadcastChannel&) = delete;
    BroadcastChannel& operator=(const BroadcastChannel&) = delete;

    BroadcastChannel(BroadcastChannel&&) = default;
    BroadcastChannel& operator=(BroadcastChannel&&) = default;

    void buffer_accessor_setup() {
        broadcast_buffer_.resize(get_num_shards());
        accessor_ = AccessorStore::create_accessor<std::unordered_map<KeyT, ValueT>>(
            channel_id_, get_local_shard_id(), source_->get_num_local_shards());
    }

    void broadcast(const KeyT& key, const ValueT& value) {
        int total_num_shards = 0;
        for (auto& pid_and_num : get_shard_info()) {
            int recv_proc_num_shards = pid_and_num.second;
            int recver_local_id = std::hash<KeyT>()(key) % recv_proc_num_shards;
            int recver_id = total_num_shards + recver_local_id;
            broadcast_buffer_[recver_id] << key << value;
            total_num_shards += recv_proc_num_shards;
        }
    }

    ValueT& get(const KeyT& key) {
        auto& dict = (*accessor_)[std::hash<KeyT>()(key) % get_num_local_shards()].access();
        auto iter = dict.find(key);
        ASSERT_MSG(iter != dict.end(), "Key Not Found");
        return iter->second;
    }

    bool get(const KeyT& key, ValueT* value) {
        auto& dict = (*accessor_)[std::hash<KeyT>()(key) % get_num_local_shards()].access();
        auto iter = dict.find(key);
        if (iter == dict.end())
            return false;
        *value = iter->second;
        return true;
    }

    bool find(const KeyT& key) {
        auto& dict = (*accessor_)[std::hash<KeyT>()(key) % get_num_local_shards()].access();
        auto iter = dict.find(key);
        return iter != dict.end();
    }

    void set_clear_dict(bool clear) { clear_dict_each_progress_ = clear; }

    std::unordered_map<KeyT, ValueT>& get_local_dict() {
        return (*accessor_)[get_local_shard_id()].storage();
    }

    void send() override {
        this->inc_progress();
        int start = std::rand();
        auto shard_info_iter = ShardInfoIter(*this->destination_, start);
        for (int i = 0; i < broadcast_buffer_.size(); ++i) {
            int dst = (start + i) % broadcast_buffer_.size();
            auto pid_and_sid = shard_info_iter.next();
            if (broadcast_buffer_[dst].size() == 0)
                continue;
            mailbox_->send(pid_and_sid.first, pid_and_sid.second,
                channel_id_, progress_, broadcast_buffer_[dst]);
            broadcast_buffer_[dst].purge();
        }
        this->mailbox_->send_complete(this->channel_id_, this->progress_, this->get_num_local_shards(),
                                      this->get_pids());
    }

    void recv() override {
        // Check whether need to leave accessor_ (last round's accessor_)
        if (need_leave_accessor_)
            leave_accessor();
        need_leave_accessor_ = true;

        while (mailbox_->poll(channel_id_, progress_)) {
            auto bin = mailbox_->recv(channel_id_, progress_);
            if (bin_stream_processor_ != nullptr) {
                bin_stream_processor_(&bin);
            }
        }
        (*accessor_)[get_local_shard_id()].commit();
    }

    void set_source(Shard* source) { source_ = source; }

   protected:
    void leave_accessor() {
        for (int i = 0; i < accessor_->size(); ++i)
            (*accessor_)[i].leave();

        if (clear_dict_each_progress_) {
            (*accessor_)[get_local_shard_id()].storage().clear();
            (*accessor_)[get_local_shard_id()].commit();
            for (int i = 0; i < accessor_->size(); ++i)
                (*accessor_)[i].leave();
        }
    }

    Shard* source_ = nullptr;
    bool clear_dict_each_progress_ = false;
    bool need_leave_accessor_ = false;
    std::vector<BinStream> broadcast_buffer_;
    std::vector<Accessor<std::unordered_map<KeyT, ValueT>>>* accessor_;
};

}  // namespace husky
