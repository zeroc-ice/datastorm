// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>
#include <DataStorm/Contract.h>

#include <Ice/Ice.h>

namespace DataStormI
{

class NodeSessionI;
class CallbackExecutor;
class Instance;
class TraceLevels;

class NodeSessionManager : public std::enable_shared_from_this<NodeSessionManager>
{
public:

    NodeSessionManager(const std::shared_ptr<Instance>&);

    void init();

    std::shared_ptr<NodeSessionI>
    createOrGet(const std::shared_ptr<DataStormContract::NodePrx>&, const std::shared_ptr<Ice::Connection>&, bool);

    void announceTopicReader(const Ice::Identity&,
                             const std::string&,
                             const std::shared_ptr<DataStormContract::NodePrx>&,
                             const std::shared_ptr<Ice::Connection>& = nullptr) const;

    void announceTopicWriter(const Ice::Identity&,
                             const std::string&,
                             const std::shared_ptr<DataStormContract::NodePrx>&,
                             const std::shared_ptr<Ice::Connection>& = nullptr) const;

    void announceTopics(const Ice::Identity&,
                        const DataStormContract::StringSeq&,
                        const DataStormContract::StringSeq&,
                        const std::shared_ptr<DataStormContract::NodePrx>&,
                        const std::shared_ptr<Ice::Connection>& = nullptr) const;

    std::shared_ptr<NodeSessionI> getSession(const Ice::Identity&) const;
    std::shared_ptr<NodeSessionI> getSession(const std::string& name) const
    {
        return getSession(Ice::Identity { name, ""});
    }

    void forward(const Ice::ByteSeq&, const Ice::Current&) const;

private:

    void connect(const std::shared_ptr<DataStormContract::LookupPrx>&,
                 const std::shared_ptr<DataStormContract::NodePrx>&);

    void connected(const std::shared_ptr<DataStormContract::NodePrx>&,
                   const std::shared_ptr<DataStormContract::LookupPrx>&);

    void disconnected(const std::shared_ptr<DataStormContract::NodePrx>&,
                      const std::shared_ptr<DataStormContract::LookupPrx>&);

    void destroySession(const std::shared_ptr<DataStormContract::NodePrx>&);

    std::shared_ptr<Instance> getInstance() const
    {
        auto instance = _instance.lock();
        assert(instance);
        return instance;
    }

    const std::weak_ptr<Instance> _instance;
    const std::shared_ptr<TraceLevels> _traceLevels;
    const Ice::Identity _nodeId;
    const bool _forwardToMulticast;

    mutable std::mutex _mutex;

    int _retryCount;

    std::map<Ice::Identity, std::shared_ptr<NodeSessionI>> _sessions;
    std::map<Ice::Identity, std::pair<std::shared_ptr<DataStormContract::NodePrx>,
                                      std::shared_ptr<DataStormContract::LookupPrx>>> _connectedTo;

    mutable std::shared_ptr<Ice::Connection> _exclude;
    std::shared_ptr<DataStormContract::LookupPrx> _forwarder;
};

}
