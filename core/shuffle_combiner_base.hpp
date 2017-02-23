#pragma once

#include "base/serialization.hpp"
#include "core/shard.hpp"

namespace husky {

template<typename MsgT, typename KeyT>
class ShuffleCombinerBase {
   public:
    // original local id follows the shard id of source_
    virtual void set_channel_id(int channel_id) { channel_id_ = channel_id; }
    virtual void set_source(Shard* source) { source_ = source; }
    virtual void set_destination(Shard* destination) { destination_ = destination; }

    virtual void push(const MsgT& msg, const KeyT& key, int dst_shard_id) = 0;
    virtual void shuffle() = 0;
    virtual void combine(std::vector<base::BinStream>* send_buffers) = 0;

    virtual ~ShuffleCombinerBase() {}
   protected:
    int channel_id_;
    Shard* source_ = nullptr;
    Shard* destination_ = nullptr;
};

}  // namespace husky
