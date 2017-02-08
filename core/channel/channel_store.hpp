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

#include <string>

#include "core/channel/channel_store_base.hpp"
#include "core/context.hpp"
#include "core/objlist.hpp"
#include "core/sync_shuffle_combiner.hpp"

namespace husky {

/// 3 types of APIs are provided
/// create_xxx_channel(), get_xxx_channel(), drop_channel()
/// ChannelStore serves as a decorator for ChannelStoreBase::create_xxx_channel.
class ChannelStore : public ChannelStoreBase {
   public:
    // Create PushChannel
    template <typename MsgT, typename DstObjT>
    static auto* create_push_channel(ObjList<DstObjT>* dst_list, const std::string& name = "") {
        auto* ch = ChannelStoreBase::create_push_channel<MsgT>(*dst_list);
        common_setup(ch);
        ch->set_base_obj_addr_getter([=](){
            // TODO(fan) should do &dst_list->get_data.get(0) in debug mode
            return &dst_list->get_data[0];
        });
        ch->set_bin_stream_processor([=](base::BinStream* bin_stream) {
            auto* recv_buffer = ch->get_recv_buffer();

            while (bin_stream->size() != 0) {
                typename DstObjT::KeyT key;
                *bin_stream >> key;

                MsgT msg;
                *bin_stream >> msg;

                DstObjT* recver_obj = dst_list->find(key);
                int idx;
                if (recver_obj == nullptr) {
                    DstObjT obj(key);  // Construct obj using key only
                    idx = dst_list->add_object(std::move(obj));
                } else {
                    idx = dst_list->index_of(recver_obj);
                }
                if (idx >= ch->get_recv_buffer()->size()) {
                    recv_buffer->resize(idx + 1);
                }

                (*recv_buffer)[idx].push_back(std::move(msg));
            }
        });
        return ch;
    }

    // Create PushCombinedChannel
    template <typename MsgT, typename CombineT, typename DstObjT>
    static auto* create_push_combined_channel(ObjList<DstObjT>* dst_list, const std::string& name = "") {
        auto* ch = ChannelStoreBase::create_push_combined_channel<MsgT, CombineT>(*dst_list);
        common_setup(ch);
        ch->set_base_obj_addr_getter([=](){
            // TODO(fan) should do &dst_list->get_data.get(0) in debug mode
            return &dst_list->get_data[0];
        });
        ch->set_combiner(new SyncShuffleCombiner<MsgT, typename DstObjT::KeyT, CombineT>(Context::get_zmq_context()));
        ch->set_bin_stream_processor([=](base::BinStream* bin_stream) {
            auto* recv_buffer = ch->get_recv_buffer();
            auto* recv_flags = ch->get_recv_flags();

            while (bin_stream->size() != 0) {
                typename DstObjT::KeyT key;
                *bin_stream >> key;
                MsgT msg;
                *bin_stream >> msg;

                DstObjT* recver_obj = dst_list->find(key);
                int idx;
                if (recver_obj == nullptr) {
                    DstObjT obj(key);  // Construct obj using key only
                    idx = dst_list->add_object(std::move(obj));
                } else {
                    idx = dst_list->index_of(recver_obj);
                }
                if (idx >= ch->get_recv_buffer()->size()) {
                    recv_buffer->resize(idx + 1);
                    recv_flags->resize(idx + 1);
                }
                if ((*recv_flags)[idx] == true) {
                    CombineT::combine((*recv_buffer)[idx], msg);
                } else {
                    (*recv_buffer)[idx] = std::move(msg);
                    (*recv_flags)[idx] = true;
                }
            }
        });
        return ch;
    }

    // Create MigrateChannel
    template <typename ObjT>
    static auto* create_migrate_channel(ObjList<ObjT>* src_list, ObjList<ObjT>* dst_list,
                                        const std::string& name = "") {
        auto* ch = ChannelStoreBase::create_migrate_channel<ObjT>(*src_list, *dst_list, name);
        common_setup(ch);
        ch->set_obj_list(src_list);
        ch->buffer_setup();
        ch->set_bin_stream_processor([=](base::BinStream* bin_stream) {
            while (bin_stream->size() != 0) {
                ObjT obj;
                *bin_stream >> obj;
                auto idx = dst_list->add_object(std::move(obj));
                dst_list->process_attribute(*bin_stream, idx);
            }
            if (dst_list->get_num_del() * 2 > dst_list->get_vector_size())
                dst_list->deletion_finalize();
        });
        return ch;
    }

    // Create BroadcastChannel
    template <typename KeyT, typename MsgT>
    static BroadcastChannel<KeyT, MsgT>& create_broadcast_channel(const std::string& name = "") {
        auto* ch = ChannelStoreBase::create_broadcast_channel<KeyT, MsgT>(name);
        common_setup(ch);
        ch->buffer_accessor_setup();
        auto& local_dict = ch->get_local_dict();
        ch->set_bin_stream_processor([=](base::BinStream* bin_stream) {
            auto& local_dict = ch->get_local_dict();
            while (bin_stream->size() != 0) {
                KeyT key;
                MsgT value;
                *bin_stream >> key >> value;
                local_dict[key] = value;
            }
        });
        return ch;
    }

    // Create PushAsyncChannel
    template <typename MsgT, typename ObjT>
    static AsyncPushChannel<MsgT, ObjT>& create_async_push_channel(ObjList<ObjT>& obj_list,
                                                                   const std::string& name = "") {
        auto& ch = ChannelStoreBase::create_async_push_channel<MsgT>(obj_list, name);
        setup(ch);
        return ch;
    }

    // Create MigrateAsyncChannel
    template <typename ObjT>
    static AsyncMigrateChannel<ObjT>& create_async_migrate_channel(ObjList<ObjT>& obj_list,
                                                                   const std::string& name = "") {
        auto& ch = ChannelStoreBase::create_async_migrate_channel<ObjT>(obj_list, name);
        setup(ch);
        return ch;
    }

    static void common_setup(ChannelBase* ch) {
        ch->set_mailbox(Context::get_mailbox(Context::get_local_tid()));
        ch->set_local_id(Context::get_local_tid());
        ch->set_global_id(Context::get_global_tid());
        ch->set_worker_info(Context::get_worker_info());
    }
};

}  // namespace husky
