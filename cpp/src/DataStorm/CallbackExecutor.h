// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <vector>
#include <memory>
#include <mutex>

namespace DataStormInternal
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
