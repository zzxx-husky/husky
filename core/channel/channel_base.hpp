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

#include <cstdlib>
#include <vector>

#include "base/exception.hpp"
#include "base/serialization.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"

namespace husky {

class ChannelBase {
   public:
    virtual ~ChannelBase() = default;

    // Getters of basic information

    inline LocalMailbox* get_mailbox() const { return mailbox_; }
    inline size_t get_channel_id() const { return channel_id_; }
    inline size_t get_progress() const { return progress_; }

    // Setters of basic information

    void set_mailbox(LocalMailbox* mailbox) { mailbox_ = mailbox; }

    // Setup API for unit test
    void setup(LocalMailbox* mailbox);

    // Top-level APIs

    virtual void in() {
        this->recv();
        this->post_recv();
    }

    virtual void out() {
        this->pre_send();
        this->send();
        this->post_send();
    }

    // Second-level APIs

    virtual void recv() {
        // A simple default synchronous implementation
        if (mailbox_ == nullptr)
            throw base::HuskyException("Local mailbox not set, and thus cannot use the recv() method.");

        while (mailbox_->poll(channel_id_, progress_)) {
            base::BinStream bin_stream = mailbox_->recv(channel_id_, progress_);
            if (bin_stream_processor_ != nullptr)
                bin_stream_processor_(&bin_stream);
        }
    }

    virtual void post_recv(){}
    virtual void pre_send(){}
    virtual void send(){}
    virtual void post_send(){}

    // Third-level APIs (invoked by its upper level)

    void set_bin_stream_processor(std::function<void(base::BinStream*)> bin_stream_processor) {
        bin_stream_processor_ = bin_stream_processor;
    }

    std::function<void(base::BinStream*)> get_bin_stream_processor() { return bin_stream_processor_; }

    void inc_progress();

   protected:
    ChannelBase();

    ChannelBase(const ChannelBase&) = delete;
    ChannelBase& operator=(const ChannelBase&) = delete;

    ChannelBase(ChannelBase&&) = default;
    ChannelBase& operator=(ChannelBase&&) = default;

    size_t channel_id_;
    size_t progress_;

    LocalMailbox* mailbox_ = nullptr;

    std::function<void(base::BinStream*)> bin_stream_processor_ = nullptr;

    static thread_local int max_channel_id_;
};

}  // namespace husky
