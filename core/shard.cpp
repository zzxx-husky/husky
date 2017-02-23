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

#include <memory>
#include <string>
#include <vector>

#include "core/hash_ring.hpp"
#include "core/shard.hpp"
#include "core/worker_info.hpp"

namespace husky {

Shard::Shard() {}

void Shard::init(const WorkerInfo& worker_info) {
    self_pid_ = worker_info.get_process_id();
    // local_shard_id_ by default is set to worker local id,
    // global_shard_id_ by default is set to worker global id,
    // this is important for back compatibility
    if (shard_info_.empty()) {
        // the default configuration of this ShardInfo comes from worker info.
        for (int id : worker_info.get_pids()) {
            shard_info_.push_back({id, worker_info.get_num_local_workers(id)});
        }
        num_local_shards_ = worker_info.get_num_local_workers();
        num_shards_ = worker_info.get_num_workers();
        hash_ring_ = worker_info.get_hash_ring();
    } else {
        num_shards_ = 0;
        num_local_shards_ = 0;
        hash_ring_ = HashRing();
        for (auto& info : shard_info_) {
            if (info.second <= 0) {
                throw base::HuskyException(
                    "Invalid number of shard given: " + std::to_string(info.second));
            }
            for (int i = 0; i < info.second; i++) {
                hash_ring_.insert(i + num_shards_);
            }
            if (info.first == self_pid_) {
                // global_shard_id_ is inferenced here
                global_shard_id_ = num_shards_ + local_shard_id_;
                num_local_shards_ = info.second;
            }
            num_shards_ += info.second;
        }
    }
}

void Shard::initialize_shard(int local_shard_id, const WorkerInfo& worker_info,
    const std::vector<std::pair<int, int>>& pid_num_info) {
    shard_info_ = pid_num_info;
    local_shard_id_ = local_shard_id;
    init(worker_info);
}

void Shard::initialize_shard(int local_shard_id, const WorkerInfo& worker_info,
    const std::vector<std::pair<std::string, int>>& hostname_num_info) {
    shard_info_.reserve(hostname_num_info.size());
    for (auto& info: hostname_num_info) {
        shard_info_.push_back(std::make_pair(worker_info.get_pid_by_hostname(info.first), info.second));
    }
    local_shard_id_ = local_shard_id;
    init(worker_info);
}

void Shard::set_local_shard_id(int id) { local_shard_id_ = id; }

// Get the process ids of the machines where shards locate
std::vector<int> Shard::get_pids() {
    std::vector<int> pids(shard_info_.size());
    for (int i = 0; i < shard_info_.size(); ++i) {
        pids[i] = shard_info_[i].first;
    }
    return pids;
}

ShardInfoIter::ShardInfoIter(Shard& shard)
  : shard_(shard),
    shard_info_iter_(shard.get_shard_info().begin()) {
}

std::pair<int, int> ShardInfoIter::next() {
    while (++cur_local_shard_id_ == shard_info_iter_->second) {
        cur_local_shard_id_ = -1;
        // assert(shard_info_iter_ == shard_.get_shard_info().end());
        ++shard_info_iter_;
    }
    return {shard_info_iter_->first, cur_local_shard_id_};
}

}  // namespace husky
