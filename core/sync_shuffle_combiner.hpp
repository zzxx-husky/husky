#pragma once

#include <utility>
#include <vector>

#include "zmq.hpp"

#include "core/shard.hpp"
#include "core/shuffle_combiner_base.hpp"


namespace husky {

template <typename MsgT, typename KeyT>
class SyncShuffleCombinerSharedStore {
   public:
    typedef std::vector<std::vector<std::pair<KeyT, MsgT>>> BufferT;

    static SyncShuffleCombinerSharedStore* get_instance() {
        static SyncShuffleCombinerSharedStore shared_store;
        return &shared_store;
    }

    BufferT* get(int tid) {
        std::lock_guard<std::mutex> lock(mutex_);
        return &shared_buffers[tid];
    }

   protected:
    SyncShuffleCombinerSharedStore() = default;
    std::mutex mutex_;
    std::unordered_map<int, BufferT> shared_buffers;
};

template <typename MsgT, typename KeyT, typename CombineT>
class SyncShuffleCombiner : public ShuffleCombinerBase<MsgT, KeyT> {
   public:
    typedef std::vector<std::vector<std::pair<KeyT, MsgT>>> BufferT;

    explicit SyncShuffleCombiner(zmq::context_t* zmq_context) : zmq_context_(zmq_context) {}

    void set_destination(Shard* destination) override {
        ShuffleCombinerBase<MsgT, KeyT>::set_destination(destination);
        send_buffers_.resize(destination->get_num_shards());
    }

    void push(const MsgT& msg, const KeyT& key, int dst_shard_id) override {
        send_buffers_[dst_shard_id].push_back({key, msg});
    }

    void shuffle() override {
        // shuffle the buffers
        // 1. declare that my buffer is ready
        if (!is_shuffle_itc_ready_) {
            init_shuffle_itc();
            is_shuffle_itc_ready_ = true;
        }
        SyncShuffleCombinerSharedStore<MsgT, KeyT>::get_instance()->get(this->source_->get_local_shard_id())->clear();
        SyncShuffleCombinerSharedStore<MsgT, KeyT>::get_instance()->get(this->source_->get_local_shard_id())->swap(send_buffers_);
        send_buffers_.resize(this->destination_->get_num_shards());
        for (int i = 0; i < this->source_->get_num_local_shards(); i++)
            zmq_send_int32(push_sock_list_[i].get(), this->source_->get_local_shard_id());

        // 2. stream in others' buffers
        for (int i = 0; i < this->source_->get_num_local_shards(); i++) {
            int tid = zmq_recv_int32(sub_sock_.get());
            auto* peer_buffer = SyncShuffleCombinerSharedStore<MsgT, KeyT>::get_instance()->get(tid);
            for (int j = 0; j < this->destination_->get_num_shards(); j++) {
                if (j % this->source_->get_num_local_shards() == this->source_->get_local_shard_id())
                    send_buffers_[j].insert(send_buffers_[j].end(), (*peer_buffer)[j].begin(), (*peer_buffer)[j].end());
            }
        }
    }

    void combine(std::vector<base::BinStream>* bin_stream_buffers) override {
        for (int i = 0; i < this->destination_->get_num_shards(); i++) {
            if (send_buffers_[i].size() != 0) {
                combine_single<CombineT>(send_buffers_[i]);
                for (auto& pair : send_buffers_[i])
                    (*bin_stream_buffers)[i] << pair;
            }
        }
    }

    ~SyncShuffleCombiner() {
        sub_sock_.reset(nullptr);
        for (auto& push_sock : push_sock_list_)
            push_sock.reset(nullptr);
    }

   protected:
    void init_shuffle_itc() {
        sub_sock_.reset(new zmq::socket_t(*zmq_context_, ZMQ_PULL));
        sub_sock_->bind("inproc://sync-shuffle-combine-" +
            std::to_string(this->channel_id_) + "-" +
            std::to_string(this->source_->get_local_shard_id()));
        for (int tid = 0; tid < this->source_->get_num_local_shards(); tid++) {
            auto push_sock = std::make_unique<zmq::socket_t>(*zmq_context_, ZMQ_PUSH);
            push_sock->connect("inproc://sync-shuffle-combine-" +
                std::to_string(this->channel_id_) + "-" +
                std::to_string(tid));
            if (push_sock_list_.size() < tid + 1)
                push_sock_list_.resize(tid + 1);
            push_sock_list_[tid] = std::move(push_sock);
        }
    }

    bool is_shuffle_itc_ready_ = false;
    zmq::context_t* zmq_context_ = nullptr;
    std::vector<std::unique_ptr<zmq::socket_t>> push_sock_list_;
    std::unique_ptr<zmq::socket_t> sub_sock_ = nullptr;
    BufferT send_buffers_;
};

}  // namespace husky
