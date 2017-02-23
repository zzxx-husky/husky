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
#include <unordered_map>

#include "base/session_local.hpp"
#include "core/channel/async_migrate_channel.hpp"
#include "core/channel/async_push_channel.hpp"
#include "core/channel/broadcast_channel.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/migrate_channel.hpp"
#include "core/channel/push_channel.hpp"
#include "core/channel/push_combined_channel.hpp"
#include "core/context.hpp"
#include "core/sync_shuffle_combiner.hpp"

namespace husky {

/// 3 types of APIs are provided
/// create_xxx_channel(), get_xxx_channel(), drop_channel()
class ChannelStoreBase {
   public:
    // Create PushChannel
    template <typename MsgT, typename DstObjT>
    static auto create_push_channel(int shard_id, ChannelSource* source, ObjList<DstObjT>* dst_list, const std::string& name = "") {
        // index_check(shard_id);
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(get_and_inc_channel_id(shard_id)) : name;
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(name) != shard_channel_map.end())
            throw base::HuskyException("ChannelStoreBase::create_channel: Channel name already exists");
        auto push_channel = new PushChannel<MsgT, DstObjT>();
        shard_channel_map.insert({channel_name, push_channel});
        return push_channel;
    }

    template <typename MsgT, typename DstObjT>
    static inline auto create_push_channel(ChannelSource* source, ObjList<DstObjT>* dst_list, const std::string& name = "") {
        return ChannelStoreBase::create_push_channel<MsgT, DstObjT>(Context::get_local_tid(), source, dst_list, name);
    }

    // Create PushChannel
    template <typename MsgT, typename DstObjT>
    static auto create_push_combined_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(get_and_inc_channel_id(shard_id)) : name;
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(name) != shard_channel_map.end())
            throw base::HuskyException("ChannelStoreBase::create_channel: Channel name already exists");
        auto push_channel = new PushChannel<MsgT, DstObjT>();
        shard_channel_map.insert({channel_name, push_channel});
        return push_channel;
    }

    template <typename MsgT, typename DstObjT>
    static inline auto create_push_combined_channel(const std::string& name = "") {
        return ChannelStoreBase::create_push_combined_channel<MsgT, DstObjT>(Context::get_local_tid(), name);
    }

    template <typename MsgT, typename DstObjT>
    static auto get_push_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(name) == shard_channel_map.end())
            throw base::HuskyException("ChannelStoreBase::get_channel: Channel name doesn't exist");
        auto channel = shard_channel_map[name];
        return static_cast<PushChannel<MsgT, DstObjT>*>(channel);
    }

    template <typename MsgT, typename DstObjT>
    static inline auto get_push_channel(const std::string& name = "") {
        return ChannelStoreBase::get_push_channel<MsgT, DstObjT>(Context::get_local_tid(), name);
    }

    // Create PushCombinedChannel
    template <typename MsgT, typename CombineT, typename DstObjT>
    static auto create_push_combined_channel(int shard_id, ChannelSource* source, ObjList<DstObjT>* dst_list, const std::string& name = "") {
        // index_check(shard_id);
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(get_and_inc_channel_id(shard_id)) : name;
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(name) != shard_channel_map.end())
            throw base::HuskyException("ChannelStoreBase::create_channel: Channel name already exists");
        auto push_combined_channel = new PushCombinedChannel<MsgT, DstObjT, CombineT>();
        shard_channel_map.insert({channel_name, push_combined_channel});
        return push_combined_channel;
    }

    template <typename MsgT, typename CombineT, typename DstObjT>
    static inline auto create_push_combined_channel(ChannelSource* source, ObjList<DstObjT>* dst_list, const std::string& name = "") {
        return ChannelStoreBase::create_push_combined_channel<MsgT, CombineT, DstObjT>(Context::get_local_tid(), source, dst_list, name);
    }

    // Create PushCombinedChannel
    template <typename MsgT, typename CombineT, typename DstObjT>
    static auto create_push_combined_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(get_and_inc_channel_id(shard_id)) : name;
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(name) != shard_channel_map.end())
            throw base::HuskyException("ChannelStoreBase::create_channel: Channel name already exists");
        auto push_combined_channel = new PushCombinedChannel<MsgT, DstObjT, CombineT>();
        shard_channel_map.insert({channel_name, push_combined_channel});
        return push_combined_channel;
    }

    template <typename MsgT, typename CombineT, typename DstObjT>
    static inline auto create_push_combined_channel(const std::string& name = "") {
        return ChannelStoreBase::create_push_combined_channel<MsgT, CombineT, DstObjT>(Context::get_local_tid(), name);
    }

    template <typename MsgT, typename CombineT, typename DstObjT>
    static auto get_push_combined_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(name) == shard_channel_map.end())
            throw base::HuskyException("ChannelStoreBase::get_channel: Channel name doesn't exist");
        auto channel = shard_channel_map[name];
        return static_cast<PushCombinedChannel<MsgT, DstObjT, CombineT>*>(channel);
    }

    template <typename MsgT, typename CombineT, typename DstObjT>
    static inline auto get_push_combined_channel(const std::string& name = "") {
        return ChannelStoreBase::get_push_channel<MsgT, CombineT, DstObjT>(Context::get_local_tid(), name);
    }

    // Create MigrateChannel
    template <typename ObjT>
    static auto create_migrate_channel(int shard_id, ObjList<ObjT>* src_list, ObjList<ObjT>* dst_list,
                                        const std::string& name = "") {
        // index_check(shard_id);
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(get_and_inc_channel_id(shard_id)) : name;
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(channel_name) != shard_channel_map.end())
            throw base::HuskyException("ChannelStoreBase::create_channel: Channel name already exists");
        auto migrate_channel = new MigrateChannel<ObjT>();
        shard_channel_map.insert({channel_name, migrate_channel});
        return migrate_channel;
    }

    template <typename ObjT>
    static inline auto create_migrate_channel(ObjList<ObjT>* src_list, ObjList<ObjT>* dst_list, const std::string& name = "") {
        return ChannelStoreBase::create_migrate_channel<ObjT>(Context::get_local_tid(), src_list, dst_list, name);
    }

    template <typename ObjT>
    static auto get_migrate_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(name) != shard_channel_map.end())
            throw base::HuskyException("ChannelStoreBase::get_channel: Channel name doesn't exist");
        auto channel = shard_channel_map[name];
        return static_cast<MigrateChannel<ObjT>*>(channel);
    }

    template <typename ObjT>
    static inline auto get_migrate_channel(const std::string& name = "") {
        return ChannelStoreBase::get_migrate_channel<ObjT>(Context::get_local_tid(), name);
    }

    // Create BroadcastChannel
    template <typename KeyT, typename MsgT>
    static auto create_broadcast_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(get_and_inc_channel_id(shard_id)) : name;
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(channel_name) != shard_channel_map.end()) {
            throw base::HuskyException("ChannelStoreBase::create_channel: Channel name already exists");
        }
        auto broadcast_channel = new BroadcastChannel<KeyT, MsgT>();
        shard_channel_map.insert({channel_name, broadcast_channel});
        return broadcast_channel;
    }

    template <typename KeyT, typename MsgT>
    static inline auto create_broadcast_channel(const std::string& name = "") {
        return ChannelStoreBase::create_broadcast_channel<KeyT, MsgT>(Context::get_local_tid(), name);
    }

    template <typename KeyT, typename MsgT>
    static auto get_broadcast_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        if (shard_channel_map.find(name) == shard_channel_map.end()) {
            throw base::HuskyException("ChannelStoreBase::get_channel: Channel name doesn't exist");
        }
        auto channel = shard_channel_map[name];
        return static_cast<BroadcastChannel<KeyT, MsgT>*>(channel);
    }

    template <typename KeyT, typename MsgT>
    static inline auto get_broadcast_channel(const std::string& name = "") {
        return ChannelStoreBase::get_broadcast_channel<KeyT, MsgT>(Context::get_local_tid(), name);
    }

    // Create AsyncPushChannel
    template <typename MsgT, typename ObjT>
    static AsyncPushChannel<MsgT, ObjT>* create_async_push_channel(int shard_id, ObjList<ObjT>* obj_list,
                                                                   const std::string& name = "") {
        // index_check(shard_id);
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(get_and_inc_channel_id(shard_id)) : name;
        auto& shard_channel_map = get_channel_map(shard_id);
        ASSERT_MSG(shard_channel_map.find(channel_name) == shard_channel_map.end(),
                   "ChannelStoreBase::create_channel: Channel name already exists");
        auto async_push_channel = new AsyncPushChannel<MsgT, ObjT>(&obj_list);
        shard_channel_map.insert({channel_name, async_push_channel});
        return async_push_channel;
    }

    template <typename MsgT, typename ObjT>
    static inline AsyncPushChannel<MsgT, ObjT>* create_async_push_channel(ObjList<ObjT>* obj_list,
                                                                   const std::string& name = "") {
        return ChannelStoreBase::create_async_push_channel<MsgT, ObjT>(Context::get_local_tid(), obj_list, name);
    }

    template <typename MsgT, typename ObjT>
    static AsyncPushChannel<MsgT, ObjT>* get_async_push_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        ASSERT_MSG(shard_channel_map.find(name) != shard_channel_map.end(),
                   "ChannelStoreBase::get_channel: Channel name doesn't exist");
        auto* channel = shard_channel_map[name];
        return *dynamic_cast<AsyncPushChannel<MsgT, ObjT>*>(channel);
    }

    template <typename MsgT, typename ObjT>
    static inline AsyncPushChannel<MsgT, ObjT>* get_async_push_channel(const std::string& name = "") {
        return ChannelStoreBase::get_async_push_channel<MsgT, ObjT>(Context::get_local_tid(), name);
    }

    // Create AsyncMigrateChannel
    template <typename ObjT>
    static AsyncMigrateChannel<ObjT>* create_async_migrate_channel(int shard_id, ObjList<ObjT>* obj_list,
                                                                   const std::string& name = "") {
        // index_check(shard_id);
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(get_and_inc_channel_id(shard_id)) : name;
        auto& shard_channel_map = get_channel_map(shard_id);
        ASSERT_MSG(shard_channel_map.find(channel_name) == shard_channel_map.end(),
                   "ChannelStoreBase::create_channel: Channel name already exists");
        auto async_migrate_channel = new AsyncMigrateChannel<ObjT>(&obj_list);
        shard_channel_map.insert({channel_name, async_migrate_channel});
        return async_migrate_channel;
    }

    template <typename ObjT>
    static inline AsyncMigrateChannel<ObjT>* create_async_migrate_channel(ObjList<ObjT>* obj_list,
                                                                   const std::string& name = "") {
        return ChannelStoreBase::create_async_migrate_channel<ObjT>(Context::get_local_tid(), obj_list, name);
    }

    template <typename ObjT>
    static AsyncMigrateChannel<ObjT>* get_async_migrate_channel(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        ASSERT_MSG(shard_channel_map.find(name) != shard_channel_map.end(),
                   "ChannelStoreBase::get_channel: Channel name doesn't exist");
        auto channel = shard_channel_map[name];
        return dynamic_cast<AsyncMigrateChannel<ObjT>*>(channel);
    }

    template <typename ObjT>
    static inline AsyncMigrateChannel<ObjT>* get_async_migrate_channel(const std::string& name = "") {
        return ChannelStoreBase::get_async_push_channel<ObjT>(Context::get_local_tid(), name);
    }

    static void drop_channel(int shard_id, const std::string& name) {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        ASSERT_MSG(shard_channel_map.find(name) != shard_channel_map.end(),
                   "ChannelStoreBase::drop_channel: Channel name doesn't exist");
        delete shard_channel_map[name];
        shard_channel_map.erase(name);
    }

    static inline void drop_channel(const std::string& name) {
        ChannelStoreBase::drop_channel(Context::get_local_tid(), name);
    }

    static bool has_channel(int shard_id, const std::string& name) {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        return shard_channel_map.find(name) != shard_channel_map.end();
    }

    static inline bool has_channel(const std::string& name) {
        return ChannelStoreBase::has_channel(Context::get_local_tid(), name);
    }

    static size_t size(int shard_id) {
        // index_check(shard_id);
        return get_channel_map(shard_id).size();
    }

    static inline size_t size() {
        return ChannelStoreBase::size(Context::get_local_tid());
    }

    static void drop_all_channels(int shard_id) {
        // index_check(shard_id);
        auto& shard_channel_map = get_channel_map(shard_id);
        for (auto& channel_pair : shard_channel_map) {
            delete channel_pair.second;
        }
        shard_channel_map.clear();
    }

    static int get_and_inc_channel_id(int shard_id) {
        static std::mutex channel_id_mutex;
        if (default_channel_id.size() <= shard_id) {
            channel_id_mutex.lock();
            default_channel_id.reserve(shard_id + 1);
            for (int i = default_channel_id.size(); i < shard_id + 1; i++) {
                default_channel_id.push_back(new int(0));
            }
            channel_id_mutex.unlock();
        }
        return (*default_channel_id[shard_id])++;
    }

    static std::unordered_map<std::string, ChannelBase*>& get_channel_map(int shard_id) {
        static std::mutex channel_map_mutex;
        if (channel_map.size() <= shard_id) {
            channel_map_mutex.lock();
            if (channel_map.empty()) {
                // set finalize_all_channels priority to Level2, the higher the level, the higher the priority
                base::SessionLocal::register_finalizer(base::SessionLocalPriority::Level2, [](){
                    for (int i = 0; i < channel_map.size(); i++) {
                        drop_all_channels(i);
                        delete channel_map[i];
                        delete default_channel_id[i];
                    } 
                    channel_map.clear();
                    default_channel_id.clear();
                });
            }
            channel_map.reserve(shard_id + 1);
            for (int i = channel_map.size(); i < shard_id + 1; i++) {
                channel_map.push_back(new std::unordered_map<std::string, ChannelBase*>());
            }
            channel_map_mutex.unlock();
        }
        return *channel_map[shard_id];
    }

   protected:
    static std::vector<std::unordered_map<std::string, ChannelBase*>*> channel_map;
    static std::vector<int*> default_channel_id;
    static const char* channel_name_prefix;

    static void index_check(int shard_id) {
        if (channel_map.size() <= shard_id) {
            throw base::HuskyException("Shard index error. Expected shard id: " + 
                std::to_string(shard_id) + ", actual objlist store shard range: [0, " +
                std::to_string(channel_map.size()) + ").");
        }
    }
};

}  // namespace husky
