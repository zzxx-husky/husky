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
#include "core/channel/channel_impl.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/worker_info.hpp"

namespace husky {

template <typename MsgT, typename DstObjT>
class PushChannel : public ChannelBase {
   public:
    PushChannel() = default;

    // The following are virtual methods

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
    }

    void set_worker_info(const WorkerInfo& worker_info) override {
        worker_info_.reset(new WorkerInfo(worker_info));
        if (send_buffer_.size() != worker_info_->get_largest_tid() + 1)
            send_buffer_.resize(worker_info_->get_largest_tid() + 1);
    }

    // The following are specific to this channel type

    inline void push(const MsgT& msg, const typename DstObjT::KeyT& key) {
        int dst_worker_id = this->worker_info_->get_hash_ring().hash_lookup(key);
        send_buffer_[dst_worker_id] << key << msg;
    }

    inline const std::vector<MsgT>& get(const DstObjT& obj) {
        if (this->base_obj_addr_getter_ == nullptr) {
            throw base::HuskyException(
                "Object Address Getter not set and thus cannot get message by providing an object. "
                "Please use `set_base_obj_addr_getter` first.");
        }
        auto idx = &obj - this->base_obj_addr_getter_();
        if (idx >= recv_buffer_.size()) {                       // resize recv_buffer_ if it is not large enough
            recv_buffer_.resize(this->obj_list_ptr_->get_size());
        }
        return recv_buffer_[idx];
    }

    inline const std::vector<MsgT>& get(int idx) { return recv_buffer_[idx]; }

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
        return recv_buffer_[idx].size() != 0;
    }

    void set_base_obj_addr_getter(std::function<DstObjT*()> base_obj_addr_getter) { base_obj_addr_getter_ = base_obj_addr_getter; }

    std::vector<std::vector<MsgT>>* get_recv_buffer() { return &recv_buffer_; }

   protected:
    std::vector<base::BinStream> send_buffer_;
    std::vector<std::vector<MsgT>> recv_buffer_;
    std::function<DstObjT*()> base_obj_addr_getter_;    // TODO(fan) cache the address?
};

}  // namespace husky
