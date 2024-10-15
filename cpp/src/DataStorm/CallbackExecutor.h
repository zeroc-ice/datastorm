//
// Copyright (c) ZeroC, Inc. All rights reserved.
//

#ifndef DATASTORM_CALLBACK_EXECUTOR_H
#define DATASTORM_CALLBACK_EXECUTOR_H

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

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

#endif
