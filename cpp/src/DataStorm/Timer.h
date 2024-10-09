//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#pragma once

#include <DataStorm/Config.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

namespace DataStormI
{

    class Timer : public std::enable_shared_from_this<Timer>
    {
    public:
        Timer();

        std::function<void()> schedule(std::chrono::milliseconds, std::function<void()>);
        void destroy();

    private:
        void runTimer();

        std::thread _thread;
        mutable std::mutex _mutex;
        std::condition_variable _cond;
        bool _destroyed;
        std::multimap<std::chrono::steady_clock::time_point, std::function<void()>> _timers;
    };

}
