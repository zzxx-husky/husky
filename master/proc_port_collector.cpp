// Copyright 2017 Husky Team
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

#include "master/proc_port_collector.hpp"

#include <string>
#include <vector>

#include "core/constants.hpp"
#include "master/master.hpp"

namespace husky {

static ProcPortCollector proc_port_collector;

ProcPortCollector::ProcPortCollector() {
    Master::get_instance().register_main_handler(TYPE_PROC_PORT, std::bind(&ProcPortCollector::response, this));
    Master::get_instance().register_setup_handler(std::bind(&ProcPortCollector::setup, this));
}

void ProcPortCollector::setup() {}

void ProcPortCollector::response() {
    auto& master = Master::get_instance();
    auto socket = master.get_socket();
    base::BinStream request = zmq_recv_binstream(socket.get());
    int port, proc_id, num_proc;
    request >> port >> proc_id >> num_proc;
    if (proc_ports.size() < num_proc) {
        proc_ports.resize(num_proc, -1);
        proc_clients.reserve(num_proc);
    }
    ASSERT_MSG(proc_ports[proc_id] == -1,
        "Multiple processes set the port in the same localtion: " + std::to_string(port));
    proc_ports[proc_id] = port;
    proc_clients.push_back(master.get_cur_client());
    if (proc_clients.size() == num_proc) {
        for (const std::string& s : proc_clients) {
            BinStream proc_ports_bin;
            for (int p : proc_ports) {
                proc_ports_bin << p;
            }
            zmq_sendmore_string(socket.get(), s);
            zmq_sendmore_dummy(socket.get());
            zmq_send_binstream(socket.get(), proc_ports_bin);
        }
    }
}

}  // namespace husky
