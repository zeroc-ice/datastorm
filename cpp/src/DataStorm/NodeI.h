//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#pragma once

#include <DataStorm/Contract.h>
#include <DataStorm/ForwarderManager.h>
#include <DataStorm/InternalI.h>

#include <Ice/Ice.h>

#include <set>

namespace DataStormI
{

    class TraceLevels;
    class Instance;
    class SessionI;
    class PublisherSessionI;
    class SubscriberSessionI;

    class NodeI : virtual public DataStormContract::Node, public std::enable_shared_from_this<NodeI>
    {
    public:
        NodeI(const std::shared_ptr<Instance>&);
        virtual ~NodeI();

        void init();
        void destroy(bool);

        virtual void initiateCreateSession(std::shared_ptr<DataStormContract::NodePrx>, const Ice::Current&) override;

        virtual void createSession(
            std::shared_ptr<DataStormContract::NodePrx>,
            std::shared_ptr<DataStormContract::SubscriberSessionPrx>,
            bool,
            const Ice::Current&) override;

        virtual void confirmCreateSession(
            std::shared_ptr<DataStormContract::NodePrx>,
            std::shared_ptr<DataStormContract::PublisherSessionPrx>,
            const Ice::Current&) override;

        void createSubscriberSession(
            std::shared_ptr<DataStormContract::NodePrx>,
            const std::shared_ptr<Ice::Connection>&,
            const std::shared_ptr<PublisherSessionI>&);

        void createPublisherSession(
            const std::shared_ptr<DataStormContract::NodePrx>&,
            const std::shared_ptr<Ice::Connection>&,
            std::shared_ptr<SubscriberSessionI>);

        void removeSubscriberSession(
            const std::shared_ptr<DataStormContract::NodePrx>&,
            const std::shared_ptr<SubscriberSessionI>&,
            const std::exception_ptr&);

        void removePublisherSession(
            const std::shared_ptr<DataStormContract::NodePrx>&,
            const std::shared_ptr<PublisherSessionI>&,
            const std::exception_ptr&);

        std::shared_ptr<Ice::Connection> getSessionConnection(const std::string&) const;

        std::shared_ptr<SessionI> getSession(const Ice::Identity&) const;

        std::shared_ptr<DataStormContract::NodePrx> getNodeWithExistingConnection(
            const std::shared_ptr<DataStormContract::NodePrx>&,
            const std::shared_ptr<Ice::Connection>&);

        std::shared_ptr<DataStormContract::NodePrx> getProxy() const { return _proxy; }

        std::shared_ptr<Instance> getInstance() const
        {
            auto instance = _instance.lock();
            assert(instance);
            return instance;
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
        std::shared_ptr<SubscriberSessionI>
        createSubscriberSessionServant(const std::shared_ptr<DataStormContract::NodePrx>&);

        std::shared_ptr<PublisherSessionI>
        createPublisherSessionServant(const std::shared_ptr<DataStormContract::NodePrx>&);

        void forward(const Ice::ByteSeq&, const Ice::Current&) const;

        mutable std::mutex _mutex;
        mutable std::condition_variable _cond;
        std::weak_ptr<Instance> _instance;
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
