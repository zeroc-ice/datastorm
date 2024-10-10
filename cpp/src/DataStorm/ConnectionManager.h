//
// Copyright (c) ZeroC, Inc. All rights reserved.
//

#ifndef DATASTORM_CONNECTIONMANAGER_H
#define DATASTORM_CONNECTIONMANAGER_H

#include "DataStorm/Config.h"
#include "Ice/Connection.h"

namespace DataStormI
{

    class CallbackExecutor;

    class ConnectionManager : public std::enable_shared_from_this<ConnectionManager>
    {
    public:
        ConnectionManager(const std::shared_ptr<CallbackExecutor>&);

        void
        add(const std::shared_ptr<void>&,
            const Ice::ConnectionPtr&,
            std::function<void(const Ice::ConnectionPtr&, std::exception_ptr)>);

        void remove(const std::shared_ptr<void>&, const Ice::ConnectionPtr&);
        void remove(const Ice::ConnectionPtr&);

        void destroy();

    private:
        using Callback = std::function<void(const Ice::ConnectionPtr&, std::exception_ptr)>;

        std::mutex _mutex;
        std::map<Ice::ConnectionPtr, std::map<std::shared_ptr<void>, Callback>> _connections;
        std::shared_ptr<CallbackExecutor> _executor;
    };

} // namespace DataStormI

#endif
