// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/InternalI.h>
#include <DataStorm/ForwarderManager.h>
#include <DataStorm/Contract.h>

#include <Ice/Ice.h>

#include <set>

namespace DataStormI
{

class TraceLevels;
class Instance;
class SessionI;
class PublisherSessionI;
class SubscriberSessionI;

class NodeI : virtual public DataStormContract::Node, public Forwarder, public std::enable_shared_from_this<NodeI>
{

public:

    NodeI(const std::shared_ptr<Instance>&);
    virtual ~NodeI();

    void init();
    void destroy();

    bool createSubscriberSession(const std::shared_ptr<DataStormContract::NodePrx>&);

    void createSubscriberSessionAsync(std::shared_ptr<DataStormContract::NodePrx>,
                                      std::shared_ptr<DataStormContract::PublisherSessionPrx>,
                                      std::function<void (const std::shared_ptr<DataStormContract::SubscriberSessionPrx>&)>,
                                      std::function<void (std::exception_ptr)>,
                                      const Ice::Current&);

    void subscriberSessionConnected(const std::shared_ptr<PublisherSessionI>&,
                                    const std::shared_ptr<DataStormContract::SubscriberSessionPrx>&,
                                    const std::shared_ptr<DataStormContract::NodePrx>&);

    void removeSubscriberSession(const std::shared_ptr<SubscriberSessionI>&, const std::exception_ptr&);

    bool createPublisherSession(const std::shared_ptr<DataStormContract::NodePrx>&);

    void createPublisherSessionAsync(std::shared_ptr<DataStormContract::NodePrx>,
                                     std::shared_ptr<DataStormContract::SubscriberSessionPrx>,
                                     std::function<void (const std::shared_ptr<DataStormContract::PublisherSessionPrx>&)>,
                                     std::function<void (std::exception_ptr)>,
                                     const Ice::Current&);

    void publisherSessionConnected(const std::shared_ptr<SubscriberSessionI>&,
                                   const std::shared_ptr<DataStormContract::PublisherSessionPrx>&,
                                   const std::shared_ptr<DataStormContract::NodePrx>&);

    void removePublisherSession(const std::shared_ptr<PublisherSessionI>&, const std::exception_ptr&);

    std::shared_ptr<Ice::Connection> getSessionConnection(const std::string&) const;

    std::shared_ptr<SessionI> getSession(const Ice::Identity&) const;

    std::shared_ptr<DataStormContract::NodePrx> getProxy() const
    {
        return Ice::uncheckedCast<DataStormContract::NodePrx>(_proxy);
    }

    std::shared_ptr<Instance> getInstance() const
    {
        return _instance;
    }

    std::shared_ptr<DataStormContract::PublisherSessionPrx> getPublisherForwarder() const
    {
        return _publisherForwarder;
    }

    std::shared_ptr<DataStormContract::SubscriberSessionPrx> getSubscriberForwarder() const
    {
        return _subscriberForwarder;
    }

private:

    std::shared_ptr<SubscriberSessionI> createSubscriberSessionServant(const std::shared_ptr<DataStormContract::NodePrx>&);
    std::shared_ptr<PublisherSessionI> createPublisherSessionServant(const std::shared_ptr<DataStormContract::NodePrx>&);

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const;

    mutable std::mutex _mutex;
    mutable std::condition_variable _cond;
    std::shared_ptr<Instance> _instance;
    std::shared_ptr<DataStormContract::NodePrx> _proxy;
    std::shared_ptr<DataStormContract::SubscriberSessionPrx> _subscriberForwarder;
    std::shared_ptr<DataStormContract::PublisherSessionPrx> _publisherForwarder;
    std::map<Ice::Identity, std::shared_ptr<SubscriberSessionI>> _subscribers;
    std::map<Ice::Identity, std::shared_ptr<PublisherSessionI>> _publishers;
    std::map<Ice::Identity, std::shared_ptr<SubscriberSessionI>> _subscriberSessions;
    std::map<Ice::Identity, std::shared_ptr<PublisherSessionI>> _publisherSessions;
    long long int _nextSubscriberSessionId;
    long long int _nextPublisherSessionId;
};

}
