#include "core/channel/push_channel.hpp"

#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/channel/migrate_channel.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/worker_info.hpp"

namespace husky {
namespace {

class TestPushChannel : public testing::Test {
   public:
    TestPushChannel() {}
    ~TestPushChannel() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class Obj {
   public:
    using KeyT = int;
    KeyT key;
    const KeyT& id() const { return key; }
    Obj() {}
    explicit Obj(const KeyT& k) : key(k) {}
};

// Create PushChannel without setting
template <typename MsgT, typename DstObjT>
PushChannel<MsgT, DstObjT> create_push_channel(ChannelSource* src_list, ObjList<DstObjT>* dst_list) {
    PushChannel<MsgT, DstObjT> push_channel;
    push_channel.set_source(src_list);
    push_channel.set_destination(dst_list);
    push_channel.set_base_obj_addr_getter([=](){
        // TODO(fan) should do &dst_list->get_data.get(0) in debug mode
        // return &dst_list->get_data[0];
        return &dst_list->get_data()[0];
    });
    push_channel.set_bin_stream_processor([=, ch=&push_channel](base::BinStream* bin_stream) {
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

    return push_channel;
}
// Create MigrateChannel without setting
template <typename ObjT>
MigrateChannel<ObjT> create_migrate_channel(ObjList<ObjT>* src_list, ObjList<ObjT>* dst_list) {
    MigrateChannel<ObjT> migrate_channel;
    migrate_channel.set_source(src_list);
    migrate_channel.set_destination(dst_list);
    migrate_channel.buffer_setup();
    migrate_channel.set_bin_stream_processor([=](base::BinStream* bin_stream) {
        while (bin_stream->size() != 0) {
            ObjT obj;
            *bin_stream >> obj;
            auto idx = dst_list->add_object(std::move(obj));
            dst_list->process_attribute(*bin_stream, idx);
        }
        if (dst_list->get_num_del() * 2 > dst_list->get_vector_size())
            dst_list->deletion_finalize();
    });

    return migrate_channel;
}

TEST_F(TestPushChannel, Create) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_local_id(0);
    el.register_mailbox(mailbox);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // PushChannel
    auto push_channel = create_push_channel<int>(&src_list, &dst_list);
    push_channel.setup(&mailbox);
}

TEST_F(TestPushChannel, PushSingle) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_local_id(0);
    el.register_mailbox(mailbox);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // PushChannel
    auto push_channel = create_push_channel<int>(&src_list, &dst_list);
    push_channel.setup(&mailbox);
    // push
    push_channel.push(123, 10);  // send 123 to 10
    push_channel.send();
    // get
    // push_channel.prepare_messages();
    Obj& obj = dst_list.get_data()[0];

    EXPECT_EQ(obj.id(), 10);
    auto msgs = push_channel.get(obj);
    EXPECT_EQ(msgs.size(), 1);
    EXPECT_EQ(msgs[0], 123);
}

TEST_F(TestPushChannel, PushMultipleTime) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_local_id(0);
    el.register_mailbox(mailbox);

    // ObjList Setup
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // PushChannel
    auto push_channel = create_push_channel<int>(&src_list, &dst_list);
    push_channel.setup(&mailbox);
    // push to two dst
    push_channel.push(123, 10);  // send 123 to 10
    push_channel.push(32, 3);
    push_channel.push(4, 10);
    push_channel.push(532, 3);
    push_channel.push(56, 10);
    push_channel.send();
    // get
    // push_channel.prepare_messages();
    EXPECT_EQ(dst_list.get_size(), 2);
    int count = 0;
    for (auto& obj : dst_list.get_data()) {
        for (auto& msg : push_channel.get(obj))
            count += 1;
    }
    EXPECT_EQ(count, 5);  // Totally 5 msgs
}

TEST_F(TestPushChannel, IncProgress) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_local_id(0);
    el.register_mailbox(mailbox);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // PushChannel
    // Round 1
    auto push_channel = create_push_channel<int>(&src_list, &dst_list);
    push_channel.setup(&mailbox);
    // push
    push_channel.push(123, 10);  // send 123 to 10
    push_channel.send();
    // get
    // push_channel.prepare_messages();
    Obj& obj = dst_list.get_data()[0];

    EXPECT_EQ(obj.id(), 10);
    auto msgs = push_channel.get(obj);
    EXPECT_EQ(msgs.size(), 1);
    EXPECT_EQ(msgs[0], 123);

    // Round 2
    push_channel.push(456, 10);  // send 123 to 10
    push_channel.send();

    // push_channel.prepare_messages();

    EXPECT_EQ(obj.id(), 10);
    msgs = push_channel.get(obj);
    EXPECT_EQ(msgs.size(), 1);
    EXPECT_EQ(msgs[0], 456);
}

TEST_F(TestPushChannel, MultiThread) {
    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    // Mailbox_0
    LocalMailbox mailbox_0(&zmq_context);
    mailbox_0.set_local_id(0);
    el.register_mailbox(mailbox_0);
    // Mailbox_1
    LocalMailbox mailbox_1(&zmq_context);
    mailbox_1.set_local_id(1);
    el.register_mailbox(mailbox_1);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.add_worker(0, 1, 1);
    workerinfo.set_process_id(0);

    std::thread th1 = std::thread([&]() {
        ObjList<Obj> src_list;

        src_list.add_object(Obj(100));
        src_list.add_object(Obj(18));
        src_list.add_object(Obj(57));

        // Globalize
        auto migrate_channel = create_migrate_channel(&src_list, &src_list);
        migrate_channel.setup(&mailbox_0);
        for (auto& obj : src_list.get_data()) {
            int dst_thread_id = workerinfo.get_hash_ring().hash_lookup(obj.id());
            if (dst_thread_id != 0) {
                migrate_channel.migrate(obj, dst_thread_id);
            }
        }
        src_list.deletion_finalize();
        migrate_channel.send();
        // migrate_channel.prepare_immigrants();
        src_list.sort();

        // Push
        auto push_channel = create_push_channel<int>(&src_list, &src_list);
        push_channel.setup(&mailbox_0);
        push_channel.push(123, 1);
        push_channel.push(123, 1342148);
        push_channel.push(123, 5);
        push_channel.push(123, 100);
        push_channel.push(123, 18);
        push_channel.push(123, 57);

        push_channel.send();
        // push_channel.prepare_messages();

        for (auto& obj : src_list.get_data()) {
            auto& msgs = push_channel.get(obj);
            EXPECT_EQ(msgs.size(), 2);
        }
    });
    std::thread th2 = std::thread([&]() {
        ObjList<Obj> src_list;

        src_list.add_object(Obj(1));
        src_list.add_object(Obj(1342148));
        src_list.add_object(Obj(5));

        // GLobalize
        auto migrate_channel = create_migrate_channel(&src_list, &src_list);
        migrate_channel.setup(&mailbox_1);
        for (auto& obj : src_list.get_data()) {
            int dst_thread_id = workerinfo.get_hash_ring().hash_lookup(obj.id());
            if (dst_thread_id != 1) {
                migrate_channel.migrate(obj, dst_thread_id);
            }
        }
        src_list.deletion_finalize();
        migrate_channel.send();
        // migrate_channel.prepare_immigrants();
        src_list.sort();

        // Push
        auto push_channel = create_push_channel<int>(&src_list, &src_list);
        push_channel.setup(&mailbox_1);
        push_channel.push(123, 1);
        push_channel.push(123, 1342148);
        push_channel.push(123, 5);
        push_channel.push(123, 100);
        push_channel.push(123, 18);
        push_channel.push(123, 57);

        push_channel.send();
        // push_channel.prepare_messages();

        for (auto& obj : src_list.get_data()) {
            auto& msgs = push_channel.get(obj);
            EXPECT_EQ(msgs.size(), 2);
        }
    });

    th1.join();
    th2.join();
}

}  // namespace
}  // namespace husky
