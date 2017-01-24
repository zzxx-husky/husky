#pragma once

#include "base/serialization.hpp"
#include "core/worker_info.hpp"

namespace husky {

template<typename MsgT, typename KeyT>
class ShuffleCombinerBase {
   public:
    virtual void set_local_id(int local_id) { local_id_ = local_id; }
    virtual void set_channel_id(int channel_id) { channel_id_ = channel_id; }
    virtual void set_worker_info(const WorkerInfo& worker_info) { worker_info_ = worker_info; }

    virtual void push(const MsgT& msg, const KeyT& key, int dst_worker_id) = 0;
    virtual void shuffle() = 0;
    virtual void combine(std::vector<base::BinStream>* send_buffers) = 0;

    virtual ~ShuffleCombinerBase() {}
   protected:
    int local_id_;
    int channel_id_;
    WorkerInfo worker_info_;
};

}  // namespace husky
