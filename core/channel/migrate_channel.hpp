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
#include "core/objlist.hpp"
#include "core/shard.hpp"

namespace husky {

using base::BinStream;

template <typename ObjT>
class MigrateChannel : public ChannelBase {
   public:
    MigrateChannel() = default;

    MigrateChannel(const MigrateChannel&) = delete;
    MigrateChannel& operator=(const MigrateChannel&) = delete;

    MigrateChannel(MigrateChannel&&) = default;
    MigrateChannel& operator=(MigrateChannel&&) = default;

    void buffer_setup() { migrate_buffer_.resize(this->destination_->get_num_shards()); }

    void migrate(ObjT& obj, int dst_shard_id) {
        auto idx = this->source_obj_list_->delete_object(&obj);
        migrate_buffer_[dst_shard_id] << obj;
        this->source_obj_list_->migrate_attribute(migrate_buffer_[dst_shard_id], idx);
    }

    void set_source(ObjList<ObjT>* obj_list_ptr) { source_obj_list_ = obj_list_ptr; }
    void set_destination(ObjList<ObjT>* destination) { destination_ = destination; }

    void send() override {
        this->inc_progress();
        int start = std::rand();
        auto shard_info_iter = ShardInfoIter(*this->source_obj_list_);
        for (int i = 0; i < migrate_buffer_.size(); ++i) {
            int dst = (start + i) % migrate_buffer_.size();
            auto pid_and_sid = shard_info_iter.next();
            if (migrate_buffer_[dst].size() == 0)
                continue;
            this->mailbox_->send(pid_and_sid.first, pid_and_sid.second,
                this->channel_id_, this->progress_, migrate_buffer_[dst]);
            migrate_buffer_[dst].purge();
        }
        this->mailbox_->send_complete(this->channel_id_, this->progress_, this->source_obj_list_->get_num_local_shards(),
                                      this->destination_->get_pids());
    }

   protected:
    ObjList<ObjT>* source_obj_list_ = nullptr;
    Shard* destination_ = nullptr;
    std::vector<BinStream> migrate_buffer_;
};

}  // namespace husky
