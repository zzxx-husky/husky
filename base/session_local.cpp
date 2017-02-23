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

#include "base/session_local.hpp"

#include <algorithm>
#include <functional>
#include <mutex>
#include <utility>
#include <vector>

namespace husky {
namespace base {

std::vector<std::pair<SessionLocalPriority, std::function<void()>>>& SessionLocal::get_finalizers() {
    static std::vector<std::pair<SessionLocalPriority, std::function<void()>>> finalizers;
    return finalizers;
}

void SessionLocal::register_finalizer(SessionLocalPriority prior, std::function<void()> fina) {
    static std::mutex register_mutex;
    auto& finalizers = get_finalizers();
    auto pair = std::make_pair(prior, fina);
    register_mutex.lock();
    finalizers.push_back(pair);
    register_mutex.unlock();
}

void SessionLocal::register_finalizer(std::function<void()> fina) {
    static std::mutex register_mutex;
    auto& finalizers = get_finalizers();
    auto pair = std::make_pair(SessionLocalPriority::Level2, fina);
    register_mutex.lock();
    finalizers.push_back(pair);
    register_mutex.unlock();
}


void SessionLocal::finalize() {
    auto& lambda_pair = get_finalizers();
    std::sort(lambda_pair.begin(), lambda_pair.end(), [](auto& a, auto& b) { return a.first > b.first; });
    for (auto func : get_finalizers())
        (func.second)();
}

RegSessionFinalizer::RegSessionFinalizer(SessionLocalPriority prior, std::function<void()> fina) {
    SessionLocal::register_finalizer(prior, fina);
}

}  // namespace base
}  // namespace husky
