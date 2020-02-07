//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#pragma once

#include <DataStorm/Contract.h>

#include <Ice/Ice.h>

namespace DataStormI
{

class Instance;
class TraceLevels;

class NodeSessionI : public std::enable_shared_from_this<NodeSessionI>
{
public:

    NodeSessionI(std::shared_ptr<Instance>,
                   std::shared_ptr<DataStormContract::NodePrx>,
                   std::shared_ptr<Ice::Connection>,
                   bool);

    void init();
    void destroy();
    void addSession(std::shared_ptr<DataStormContract::SessionPrx>);

    const std::shared_ptr<DataStormContract::NodePrx>& getPublicNode() const { return _publicNode; }
    const std::shared_ptr<DataStormContract::LookupPrx>& getLookup() const { return _lookup; }
    const std::shared_ptr<Ice::Connection>& getConnection() const { return _connection; }
    template<typename T> std::shared_ptr<T> getSessionForwarder(const std::shared_ptr<T>& session) const
    {
        return Ice::uncheckedCast<T>(forwarder(session));
    }

private:

    std::shared_ptr<DataStormContract::SessionPrx>
    forwarder(const std::shared_ptr<DataStormContract::SessionPrx>&) const;

    const std::shared_ptr<Instance> _instance;
    const std::shared_ptr<TraceLevels> _traceLevels;
    const std::shared_ptr<DataStormContract::NodePrx> _node;
    const std::shared_ptr<Ice::Connection> _connection;

    std::mutex _mutex;
    bool _destroyed;
    std::shared_ptr<DataStormContract::NodePrx> _publicNode;
    std::shared_ptr<DataStormContract::LookupPrx> _lookup;
    std::map<Ice::Identity, std::shared_ptr<DataStormContract::SessionPrx>> _sessions;
};

}
