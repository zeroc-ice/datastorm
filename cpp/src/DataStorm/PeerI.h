// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Ice.h>
#include <DataStorm/DataStorm.h>
#include <DataStorm/ForwarderManager.h>

#include <Internal.h>

#include <set>

namespace DataStormInternal
{

class TraceLevels;
class Instance;
class SessionI;
class PublisherI;
class SubscriberI;

class PeerI : virtual public DataStormContract::Peer, private Forwarder, public std::enable_shared_from_this<PeerI>
{

public:

    PeerI(const std::shared_ptr<Instance>&);
    virtual ~PeerI();

    void init();

    virtual bool createSession(const std::shared_ptr<DataStormContract::PeerPrx>&) = 0;
    void removeSession(SessionI*);

    std::shared_ptr<Instance> getInstance() const
    {
        return _instance;
    }

    std::shared_ptr<DataStormContract::SessionPrx> getForwarder() const
    {
        return _forwarder;
    }

protected:

    std::shared_ptr<SessionI> createSessionServant(const std::shared_ptr<DataStormContract::PeerPrx>&);
    virtual std::shared_ptr<SessionI> makeSessionServant(const std::shared_ptr<DataStormContract::PeerPrx>&) = 0;

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const;

    mutable std::mutex _mutex;
    mutable std::condition_variable _cond;
    std::shared_ptr<Instance> _instance;
    std::shared_ptr<DataStormContract::PeerPrx> _proxy;
    std::shared_ptr<DataStormContract::SessionPrx> _forwarder;
    std::map<Ice::Identity, std::shared_ptr<SessionI>> _sessions;
};

class PublisherI : public PeerI, public DataStormContract::Publisher
{
public:

    PublisherI(const std::shared_ptr<Instance>&);

    virtual bool createSession(const std::shared_ptr<DataStormContract::PeerPrx>&) override;

    virtual void createSessionAsync(std::shared_ptr<DataStormContract::SubscriberPrx>,
                                    std::shared_ptr<DataStormContract::SubscriberSessionPrx>,
                                    std::function<void (const std::shared_ptr<DataStormContract::PublisherSessionPrx>&)>,
                                    std::function<void (std::exception_ptr)>,
                                    const Ice::Current&) override;

    void sessionConnected(const std::shared_ptr<SessionI>&,
                          const std::shared_ptr<DataStormContract::SubscriberSessionPrx>&,
                          const std::shared_ptr<DataStormContract::PeerPrx>&);

    std::shared_ptr<DataStormContract::PublisherPrx> getProxy() const
    {
        return Ice::uncheckedCast<DataStormContract::PublisherPrx>(_proxy);
    }

private:

    virtual std::shared_ptr<SessionI> makeSessionServant(const std::shared_ptr<DataStormContract::PeerPrx>&) override;
};

class SubscriberI : public PeerI, public DataStormContract::Subscriber
{
public:

    SubscriberI(const std::shared_ptr<Instance>&);

    virtual bool createSession(const std::shared_ptr<DataStormContract::PeerPrx>&) override;

    virtual void createSessionAsync(std::shared_ptr<DataStormContract::PublisherPrx>,
                                    std::shared_ptr<DataStormContract::PublisherSessionPrx>,
                                    std::function<void (const std::shared_ptr<DataStormContract::SubscriberSessionPrx>&)>,
                                    std::function<void (std::exception_ptr)>,
                                    const Ice::Current&) override;

    void sessionConnected(const std::shared_ptr<SessionI>&,
                          const std::shared_ptr<DataStormContract::PublisherSessionPrx>&,
                          const std::shared_ptr<DataStormContract::PeerPrx>&);

    std::shared_ptr<DataStormContract::SubscriberPrx> getProxy() const
    {
        return Ice::uncheckedCast<DataStormContract::SubscriberPrx>(_proxy);
    }

private:

    virtual std::shared_ptr<SessionI> makeSessionServant(const std::shared_ptr<DataStormContract::PeerPrx>&) override;
};

}