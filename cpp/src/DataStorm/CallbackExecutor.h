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

namespace DataStormI
{

class DataElementI;

class CallbackExecutor
{
public:

    CallbackExecutor();

    void queue(const std::shared_ptr<DataElementI>&, std::function<void()>);
    void flush();

private:

    std::mutex _mutex;
    std::vector<std::pair<std::shared_ptr<DataElementI>, std::function<void()>>> _queue;
};

}
