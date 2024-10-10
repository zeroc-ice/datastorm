//
// Copyright (c) ZeroC, Inc. All rights reserved.
//

#ifndef DATASTORM_NODESESSIONI_H
#define DATASTORM_NODESESSIONI_H

#include "DataStorm/Contract.h"
#include "Ice/Ice.h"

namespace DataStormI
{

    class Instance;
    class TraceLevels;

    class NodeSessionI : public std::enable_shared_from_this<NodeSessionI>
    {
    public:
        NodeSessionI(
            std::shared_ptr<Instance>,
            std::optional<DataStormContract::NodePrx>,
            std::shared_ptr<Ice::Connection>,
            bool);

        void init();
        void destroy();
        void addSession(std::optional<DataStormContract::SessionPrx>);

        std::optional<DataStormContract::NodePrx> getPublicNode() const { return _publicNode; }
        std::optional<DataStormContract::LookupPrx> getLookup() const { return _lookup; }
        const std::shared_ptr<Ice::Connection>& getConnection() const { return _connection; }
        template<typename T> std::optional<T> getSessionForwarder(std::optional<T> session) const
        {
            return Ice::uncheckedCast<T>(forwarder(session));
        }

    private:
        std::optional<DataStormContract::SessionPrx> forwarder(std::optional<DataStormContract::SessionPrx>) const;

        const std::shared_ptr<Instance> _instance;
        const std::shared_ptr<TraceLevels> _traceLevels;
        std::optional<DataStormContract::NodePrx> _node;
        const std::shared_ptr<Ice::Connection> _connection;

        std::mutex _mutex;
        bool _destroyed;
        std::optional<DataStormContract::NodePrx> _publicNode;
        std::optional<DataStormContract::LookupPrx> _lookup;
        std::map<Ice::Identity, std::optional<DataStormContract::SessionPrx>> _sessions;
    };

} // namespace DataStormI
#endif
