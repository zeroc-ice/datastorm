// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormInternal;

CallbackExecutor::CallbackExecutor()
{
}

void
CallbackExecutor::queue(const std::shared_ptr<DataElementI>& element, std::function<void()> cb)
{
    unique_lock<mutex> lock(_mutex);
    _queue.emplace_back(element, move(cb));
}

void
CallbackExecutor::flush()
{
    std::vector<std::pair<std::shared_ptr<DataElementI>, std::function<void()>>> queue;
    {
        unique_lock<mutex> lock(_mutex);
        _queue.swap(queue);
    }
    for(const auto& p : queue)
    {
        p.second();
    }
}
