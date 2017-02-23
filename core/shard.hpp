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

#include <memory>
#include <string>
#include <vector>

#include "core/hash_ring.hpp"
#include "core/worker_info.hpp"

namespace husky {

/**
 * Notes:
 * 1. Receiver is `shard`, instead of worker any more.
 * 2. Local shard id is consecutive, while the local worker ids who holds the channel is inconsecutive.
 * 3. We should not try to know which worker works on which shard, that's what shard doesn't support.
 * 4. No global shard id.
 *
 * Make it work in husky first.
 * HashRing is copied for each Shard.
 */
class Shard {
   private:
    void init(const WorkerInfo& worker_info);

   public:
    Shard();

    // initialize with [(process id, num of local shards)]
    void initialize_shard(int local_shard_id, const WorkerInfo& worker_info,
        const std::vector<std::pair<int, int>>& pid_num_info = {});

    // initialize with [(hostname, num of local shards)]
    // Note: Extra info can be added in hostname for different processes in the same machine
    void initialize_shard(int local_shard_id, const WorkerInfo& worker_info,
        const std::vector<std::pair<std::string, int>>& hostname_num_info);

    inline int get_num_processes() const { return shard_info_.size(); }

    void set_local_shard_id(int id);

    inline int get_local_shard_id() const { return local_shard_id_; }

    inline int get_global_shard_id() const { return global_shard_id_; }

    // Get the number of shards in local machine
    inline int get_num_local_shards() const { return num_local_shards_; }

    inline int get_num_shards() const { return num_shards_; }

    // Get the process ids of the machines where shards locate
    std::vector<int> get_pids();

    inline const std::vector<std::pair<int, int>>& get_shard_info() const { return shard_info_; }

    inline const HashRing& get_hash_ring() const { return hash_ring_; }

   private:
    int local_shard_id_ = -1;
    int global_shard_id_ = -1;
    int self_pid_ = -1;  // id of this process
    int num_shards_ = 0;
    int num_local_shards_ = 0;
    HashRing hash_ring_;
    std::vector<std::pair<int, int>> shard_info_;  // ids of processes that hold at least one shard
};

class ShardInfoIter {
   public:
    ShardInfoIter(Shard& shard);

    inline int size() { return shard_.get_num_shards(); }

    std::pair<int, int> next();

   private:
    const Shard& shard_;
    int cur_local_shard_id_ = -1;
    std::vector<std::pair<int, int>>::const_iterator shard_info_iter_;
};

}  // namespace husky
