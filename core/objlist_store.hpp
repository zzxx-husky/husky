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
#include "core/context.hpp"
#include "core/objlist.hpp"
#include "core/utils.hpp"

namespace husky {

/// 3 APIs are provided
/// create_objlist(), get_objlist(), drop_objlist()
/// See the unittest for the usages
class ObjListStore {
   public:
    template <typename ObjT>
    static ObjList<ObjT>& create_objlist(int shard_id, const std::string& name = "") {
        // index_check(shard_id);
        std::string list_name = name.empty() ? objlist_name_prefix + std::to_string(get_and_inc_objlist_id(shard_id)) : name;
        auto& shard_map = get_objlist_map(shard_id);
        if(shard_map.find(name) != shard_map.end())
            throw base::HuskyException("ObjListStore::create_objlist: ObjList name already exists");
        auto* objlist = new ObjList<ObjT>();
        objlist->initialize_shard(shard_id, Context::get_worker_info());
        shard_map.insert({list_name, objlist});
        return *objlist;
    }

    template <typename ObjT>
    static inline ObjList<ObjT>& create_objlist(const std::string& name = "") {
        return ObjListStore::create_objlist<ObjT>(Context::get_local_tid(), name);
    }

    template <typename ObjT>
    static ObjList<ObjT>& get_objlist(int shard_id, const std::string& name) {
        // index_check(shard_id);
        auto& shard_map = get_objlist_map(shard_id);
        if(shard_map.find(name) == shard_map.end())
            throw base::HuskyException("ObjListStore::get_objlist: ObjList name doesn't exist");
        auto* objlist = shard_map[name];
        return *static_cast<ObjList<ObjT>*>(objlist);
    }

    template <typename ObjT>
    static inline ObjList<ObjT>& get_objlist(const std::string& name) {
        return ObjListStore::get_objlist<ObjT>(Context::get_local_tid(), name);
    }

    static void drop_objlist(int shard_id, const std::string& name) {
        // index_check(shard_id);
        auto& shard_map = get_objlist_map(shard_id);
        if(shard_map.find(name) == shard_map.end())
            throw base::HuskyException("ObjListStore::drop_objlist: ObjList name doesn't exist");
        delete shard_map[name];
        shard_map.erase(name);
    }

    static inline void drop_objlist(const std::string& name) {
        ObjListStore::drop_objlist(Context::get_local_tid(), name);
    }

    static bool has_objlist(int shard_id, const std::string& name) {
        // index_check(shard_id);
        auto& shard_map = get_objlist_map(shard_id);
        return shard_map.find(name) != shard_map.end();
    }

    static inline bool has_objlist(const std::string& name) {
        return ObjListStore::has_objlist(Context::get_local_tid(), name);
    }

    static size_t size(int shard_id) {
        // index_check(shard_id);
        return get_objlist_map(shard_id).size();
    }

    static inline size_t size() {
        return ObjListStore::size(Context::get_local_tid());
    }

    static void drop_all_objlists(int shard_id) {
        // index_check(shard_id);
        auto& shard_map = get_objlist_map(shard_id);
        for (auto& objlist_pair : shard_map) {
            delete objlist_pair.second;
        }
        shard_map.clear();
    }

    static int get_and_inc_objlist_id(int shard_id) {
        static std::mutex objlist_id_mutex;
        if (default_objlist_id.size() <= shard_id) {
            objlist_id_mutex.lock();
            default_objlist_id.reserve(shard_id + 1);
            for (int i = default_objlist_id.size(); i < shard_id + 1; i++) {
                default_objlist_id.push_back(new int(0));
            }
            objlist_id_mutex.unlock();
        }
        return (*default_objlist_id[shard_id])++;
    }

    static std::unordered_map<std::string, ObjListBase*>& get_objlist_map(int shard_id) {
        static std::mutex objlist_map_mutex;
        if (objlist_map.size() <= shard_id) {
            objlist_map_mutex.lock();
            if (objlist_map.empty()) {
                // set finalize_all_channels priority to Level2, the higher the level, the higher the priority
                base::SessionLocal::register_finalizer(base::SessionLocalPriority::Level1, [](){
                    for (int i = 0; i < objlist_map.size(); ++i) {
                        drop_all_objlists(i);
                        delete objlist_map[i];
                        delete default_objlist_id[i];
                    }
                    objlist_map.clear();
                    default_objlist_id.clear();
                });
            }
            objlist_map.reserve(shard_id + 1);
            for (int i = objlist_map.size(); i < shard_id + 1; i++) {
                objlist_map.push_back(new std::unordered_map<std::string, ObjListBase*>());
            }
            objlist_map_mutex.unlock();
        }
        return *objlist_map[shard_id];
    }

   protected:
    static std::vector<std::unordered_map<std::string, ObjListBase*>*> objlist_map;
    static std::vector<int*> default_objlist_id;
    static const char* objlist_name_prefix;

    static void index_check(int shard_id) {
        if (objlist_map.size() <= shard_id) {
            throw base::HuskyException("Shard index error. Expected shard id: " + 
                std::to_string(shard_id) + ", actual objlist store shard range: [0, " +
                std::to_string(objlist_map.size()) + ").");
        }
    }
};

}  // namespace husky
