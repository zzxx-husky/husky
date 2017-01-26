#pragma once

#include <utility>
#include <vector>

#include "zmq.hpp"

#include "core/shuffle_combiner_base.hpp"
#include "core/worker_info.hpp"


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

    void set_worker_info(const WorkerInfo& worker_info) override {
        worker_info_ = worker_info;
        send_buffers_.resize(worker_info_.get_num_workers());
    }

    void push(const MsgT& msg, const KeyT& key, int dst_worker_id) override {
        send_buffers_[dst_worker_id].push_back({key, msg});
    }

    void shuffle() override {
        // shuffle the buffers
        // 1. declare that my buffer is ready
        if (!is_shuffle_itc_ready_)
            init_shuffle_itc();
        SyncShuffleCombinerSharedStore<MsgT, KeyT>::get_instance()->get(this->local_id_)->clear();
        SyncShuffleCombinerSharedStore<MsgT, KeyT>::get_instance()->get(this->local_id_)->swap(send_buffers_);
        send_buffers_.resize(worker_info_.get_num_workers());
        for (int tid : worker_info_.get_local_tids())
            zmq_send_int32(push_sock_list_[tid].get(), this->local_id_);

        // 2. stream in others' buffers
        for (int i = 0; i < worker_info_.get_num_local_workers(); i++) {
            int tid = zmq_recv_int32(sub_sock_.get());
            auto* peer_buffer = SyncShuffleCombinerSharedStore<MsgT, KeyT>::get_instance()->get(tid);
            for (int j = 0; j < worker_info_.get_num_workers(); j++) {
                if (j % worker_info_.get_num_local_workers() == this->local_id_)
                    send_buffers_[j].insert(send_buffers_[j].end(), (*peer_buffer)[j].begin(), (*peer_buffer)[j].end());
            }
        }
    }

    void combine(std::vector<base::BinStream>* bin_stream_buffers) override {
        for (int i : worker_info_.get_global_tids()) {
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
        sub_sock_->bind("inproc://sync-shuffle-combine-" + std::to_string(this->channel_id_) + "-" +
                        std::to_string(this->local_id_));
        for (int tid : worker_info_.get_local_tids()) {
            auto push_sock = std::make_unique<zmq::socket_t>(*zmq_context_, ZMQ_PUSH);
            push_sock->connect("inproc://sync-shuffle-combine-" + std::to_string(this->channel_id_) + "-" +
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
    WorkerInfo worker_info_;
    BufferT send_buffers_;
};

}  // namespace husky
