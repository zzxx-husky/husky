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

#include "core/context.hpp"

#include <string>
#include <vector>

#include "core/constants.hpp"

namespace husky {

ContextGlobal Context::global_;
thread_local ContextLocal Context::local_;

void Context::create_mailbox_env(int max_num_local_mailbox) {
    global_.mailbox_event_loop.reset(new MailboxEventLoop(&global_.zmq_context_));
    global_.mailbox_event_loop->set_process_id(get_process_id());

    max_num_local_mailbox = max_num_local_mailbox <= 0 ? get_num_local_workers() : max_num_local_mailbox;
    global_.local_mailboxes_.resize(max_num_local_mailbox);
    for (int local_tid = 0; local_tid < max_num_local_mailbox; ++local_tid) {
        global_.local_mailboxes_.at(local_tid).reset(new LocalMailbox(&global_.zmq_context_));
        global_.local_mailboxes_.at(local_tid)->set_local_id(local_tid);
        global_.mailbox_event_loop->register_mailbox(*(global_.local_mailboxes_.at(local_tid).get()));
    }

    global_.central_recver.reset(new CentralRecver(get_zmq_context()));
    // send the bind port of central recver to master
    base::BinStream port_info;
    port_info << global_.central_recver->get_bind_port() << get_process_id() << get_num_processes();
    // this will block until master receives all the ports
    base::BinStream peer_ports = get_coordinator()->ask_master(port_info, TYPE_PROC_PORT);
    for (int proc_id = 0, peer_comm_port; proc_id < get_num_processes(); proc_id++) {
        peer_ports >> peer_comm_port;
        global_.mailbox_event_loop->register_peer_recver(proc_id, "tcp://" + global_.worker_info.get_hostname(proc_id) +
                                                                      ":" + std::to_string(peer_comm_port));
    }
}

}  // namespace husky
