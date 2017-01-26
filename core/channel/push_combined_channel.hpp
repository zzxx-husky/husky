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
#include "core/worker_info.hpp"
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
        this->shuffle_combiner_impl_->combine(&send_buffer_);
    }

    void send() override {
        int start = this->global_id_;
        for (int i = 0; i < send_buffer_.size(); ++i) {
            int dst = (start + i) % send_buffer_.size();
            if (send_buffer_[dst].size() == 0)
                continue;
            this->mailbox_->send(dst, this->channel_id_, this->progress_ + 1, send_buffer_[dst]);
            send_buffer_[dst].purge();
        }
    }

    void post_send() override {
        this->inc_progress();
        this->mailbox_->send_complete(this->channel_id_, this->progress_, this->worker_info_->get_local_tids(),
                                      this->worker_info_->get_pids());
        clear_recv_buffer_();
    }

    void set_worker_info(const WorkerInfo& worker_info) override {
        worker_info_.reset(new WorkerInfo(worker_info));
        if (send_buffer_.size() != worker_info_->get_largest_tid() + 1)
            send_buffer_.resize(worker_info_->get_largest_tid() + 1);
    }

    // The following are specific to this channel type

    void set_combiner(ShuffleCombinerBase<MsgT, typename DstObjT::KeyT>* combiner_base) {
        shuffle_combiner_impl_.reset(combiner_base);
        shuffle_combiner_impl_->set_local_id(local_id_);
        shuffle_combiner_impl_->set_channel_id(channel_id_);
        shuffle_combiner_impl_->set_worker_info(*(worker_info_.get()));  // FIXME(fan) unsafe
    }

    inline void push(const MsgT& msg, const typename DstObjT::KeyT& key) {
        int dst_worker_id = this->worker_info_->get_hash_ring().hash_lookup(key);
        this->shuffle_combiner_impl_->push(msg, key, dst_worker_id);
    }

    inline const MsgT& get(const DstObjT& obj) {
        if (this->obj_list_ptr_ == nullptr) {
            throw base::HuskyException(
                "Object Address Getter not set and thus cannot get message by providing an object. "
                "Please use `set_base_obj_addr_getter` first.");
        }
        auto idx = &obj - this->base_obj_addr_getter_();
        if (!has_msgs(idx))
            return MsgT();
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

   protected:
    void clear_recv_buffer_() { std::fill(recv_flag_.begin(), recv_flag_.end(), false); }

    std::vector<base::BinStream> send_buffer_;
    std::vector<MsgT> recv_buffer_;
    std::vector<bool> recv_flag_;
    std::unique_ptr<ShuffleCombinerBase<MsgT, typename DstObjT::KeyT>> shuffle_combiner_impl_;
    std::function<DstObjT*()> base_obj_addr_getter_;    // TODO(fan) cache the address?
};

}  // namespace husky
