//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#pragma once

#include <DataStorm/Config.h>

#include <Ice/Ice.h>

namespace DataStormI
{

    class CallbackExecutor;
    class ForwarderManager;

    class ConnectionManager : public std::enable_shared_from_this<ConnectionManager>
    {
    public:
        ConnectionManager(const std::shared_ptr<CallbackExecutor>&);

        void
        add(const std::shared_ptr<void>&,
            const std::shared_ptr<Ice::Connection>&,
            std::function<void(const std::shared_ptr<Ice::Connection>&, std::exception_ptr)>);

        void remove(const std::shared_ptr<void>&, const std::shared_ptr<Ice::Connection>&);
        void remove(const std::shared_ptr<Ice::Connection>&);

        void destroy();

    private:
        using Callback = std::function<void(const std::shared_ptr<Ice::Connection>&, std::exception_ptr)>;

        std::mutex _mutex;
        std::map<std::shared_ptr<Ice::Connection>, std::map<std::shared_ptr<void>, Callback>> _connections;
        std::shared_ptr<CallbackExecutor> _executor;
    };

}
