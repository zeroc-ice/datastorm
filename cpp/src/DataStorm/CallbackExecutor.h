// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <thread>
#include <condition_variable>

namespace DataStormI
{

class DataElementI;

class CallbackExecutor
{
public:

    CallbackExecutor();

    void queue(const std::shared_ptr<DataElementI>&, std::function<void()>, bool = false);
    void flush();
    void destroy();

private:

    std::mutex _mutex;
    std::thread _thread;
    std::condition_variable _cond;
    bool _flush;
    bool _destroyed;
    std::vector<std::pair<std::shared_ptr<DataElementI>, std::function<void()>>> _queue;
};

}
