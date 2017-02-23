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

#include <functional>
#include <vector>

#include "base/serialization.hpp"
#include "core/channel/channel_base.hpp"
#include "core/shard.hpp"

namespace husky {

using base::BinStream;

class AggregatorChannel : public ChannelBase {
   public:
    AggregatorChannel();
    virtual ~AggregatorChannel();

    virtual void prepare();
    virtual void in(BinStream& bin);
    virtual void out();
    virtual void customized_setup();

    void default_setup(LocalMailbox* mailbox, std::function<void()> something);
    void send(std::vector<BinStream>& bins);
    bool poll();
    BinStream recv_();

    void set_source(Shard* source);
    void set_destination(Shard* destination);

   private:
    Shard* source_ = nullptr;
    Shard* destination_ = nullptr;
    std::function<void()> do_something = nullptr;
};

}  // namespace husky
