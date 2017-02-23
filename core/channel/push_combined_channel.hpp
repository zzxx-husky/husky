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

#include <time.h>

#include <cstdlib>
#include <functional>
#include <utility>
#include <vector>

#include "base/serialization.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/channel_impl.hpp"
#include "core/combiner.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/shuffle_combiner_base.hpp"
#include "core/shard.hpp"
#include "core/zmq_helpers.hpp"

namespace husky {

template <typename MsgT, typename DstObjT, typename CombineT>
class PushCombinedChannel : public ChannelBase {
   public:
    PushCombinedChannel() = default;

    // The following are virtual methods

    void pre_send() override {
        // shuffle and combine
        this->shuffle_combiner_impl_->shuffle();
        this->shuffle_combiner_impl_->combine(&bin_stream_buffer_);
    }

    void send() override {
        int start = std::rand();
        auto shard_info_iter = ShardInfoIter(*this->destination_);
        for (int i = 0; i < bin_stream_buffer_.size(); ++i) {
            int dst = (start + i) % bin_stream_buffer_.size();
            auto pid_and_sid = shard_info_iter.next();
            if (bin_stream_buffer_[dst].size() > 0) {
                this->mailbox_->send(pid_and_sid.first, pid_and_sid.second,
                    this->channel_id_, this->progress_ + 1, bin_stream_buffer_[dst]);
                bin_stream_buffer_[dst].purge();
            }
        }
    }

    void post_send() override {
        this->inc_progress();
        this->mailbox_->send_complete(this->channel_id_, this->progress_, this->source_->get_num_local_shards(),
                                      this->destination_->get_pids());
        clear_recv_buffer_();
    }

    // The following are specific to this channel type

    void set_combiner(ShuffleCombinerBase<MsgT, typename DstObjT::KeyT>* combiner_base) {
        shuffle_combiner_impl_.reset(combiner_base);
        shuffle_combiner_impl_->set_channel_id(channel_id_);
        shuffle_combiner_impl_->set_source(source_);  // FIXME(fan) unsafe
        shuffle_combiner_impl_->set_destination(destination_);  // FIXME(zzxx) unsafe?
    }

    inline void push(const MsgT& msg, const typename DstObjT::KeyT& key) {
        int dst_shard_id = this->destination_->get_hash_ring().hash_lookup(key);
        this->shuffle_combiner_impl_->push(msg, key, dst_shard_id);
    }

    inline const MsgT& get(const DstObjT& obj) {
        auto idx = &obj - this->base_obj_addr_getter_();
        if (!has_msgs(idx)) {
            // TODO(???): avoid warning aobut returning reference of local variable
            return MsgT();
        }
        return get(idx);
    }

    inline const MsgT& get(int idx) { return recv_buffer_[idx]; }

    inline bool has_msgs(const DstObjT& obj) {
        if (this->base_obj_addr_getter_ == nullptr) {
            throw base::HuskyException(
                "Object Address Getter not set and thus cannot get message by providing an object. "
                "Please use `set_base_obj_addr_getter` first.");
        }
        auto idx = &obj - this->base_obj_addr_getter_();
        return has_msgs(idx);
    }

    inline bool has_msgs(int idx) {
        if (idx >= recv_buffer_.size())
            return false;
        return recv_flag_[idx];
    }

    void set_base_obj_addr_getter(std::function<DstObjT*()> base_obj_addr_getter) { base_obj_addr_getter_ = base_obj_addr_getter; }

    std::vector<MsgT>* get_recv_buffer() { return &recv_buffer_; }

    std::vector<bool>* get_recv_flags() { return &recv_flag_; }

    void set_source(Shard* source) { source_ = source; }

    void set_destination(Shard* destination) {
        destination_ = destination;
        if (bin_stream_buffer_.size() != destination->get_num_shards())
            bin_stream_buffer_.resize(destination->get_num_shards());
    }

   protected:
    void clear_recv_buffer_() { std::fill(recv_flag_.begin(), recv_flag_.end(), false); }

    Shard* source_ = nullptr;
    Shard* destination_ = nullptr;
    std::vector<base::BinStream> bin_stream_buffer_;
    std::vector<MsgT> recv_buffer_;
    std::vector<bool> recv_flag_;
    std::unique_ptr<ShuffleCombinerBase<MsgT, typename DstObjT::KeyT>> shuffle_combiner_impl_;
    std::function<DstObjT*()> base_obj_addr_getter_;    // TODO(fan) cache the address?
};

}  // namespace husky
