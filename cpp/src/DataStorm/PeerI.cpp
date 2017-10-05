// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/PeerI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/PeerI.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;

PeerI::PeerI(const shared_ptr<Instance>& instance) :
    _instance(instance),
    _forwarder(Ice::uncheckedCast<DataStormContract::SessionPrx>(_instance->getForwarderManager()->add(this)))
{
}

PeerI::~PeerI()
{
    _instance->getForwarderManager()->remove(_forwarder->ice_getIdentity());
}

void
PeerI::init()
{
    try
    {
        auto adapter = _instance->getObjectAdapter();
        _proxy = Ice::uncheckedCast<DataStormContract::PeerPrx>(adapter->addWithUUID(shared_from_this()));
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

void
PeerI::removeSession(SessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    _sessions.erase(session->getPeer()->ice_getIdentity());
}

shared_ptr<SessionI>
PeerI::createSessionServant(const shared_ptr<DataStormContract::PeerPrx>& peer)
{
    auto p = _sessions.find(peer->ice_getIdentity());
    if(p != _sessions.end())
    {
        return p->second;
    }
    else
    {
        try
        {
            auto session = makeSessionServant(peer);
            session->init();
            _sessions.insert(make_pair(peer->ice_getIdentity(), session));
            _cond.notify_all();
            return session;
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
            return nullptr;
        }
    }
}

void
PeerI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    lock_guard<mutex> lock(_mutex);
    for(const auto s : _sessions)
    {
        shared_ptr<DataStormContract::SessionPrx> session = s.second->getSession();
        if(session)
        {
            session->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
        }
    }
}

PublisherI::PublisherI(const shared_ptr<Instance>& instance) : PeerI(instance)
{
}

bool
PublisherI::createSession(const shared_ptr<DataStormContract::PeerPrx>& peer)
{
    unique_lock<mutex> lock(_mutex);
    auto session = createSessionServant(peer);
    if(!session)
    {
        return false; // Shutting down.
    }

    auto proxy = session->getSession();
    if(proxy)
    {
        return false; // Already connected.
    }

    auto self = dynamic_pointer_cast<PublisherI>(shared_from_this());
    auto subscriber = Ice::uncheckedCast<DataStormContract::SubscriberPrx>(peer);
    subscriber->createSessionAsync(Ice::uncheckedCast<DataStormContract::PublisherPrx>(_proxy),
                                   Ice::uncheckedCast<DataStormContract::PublisherSessionPrx>(session->getProxy()),
                                   [self, session, peer](shared_ptr<DataStormContract::SubscriberSessionPrx> s)
                                   {
                                        self->sessionConnected(session, s, peer);
                                   },
                                   [self, session, peer](exception_ptr e)
                                   {
                                        self->sessionConnected(session, nullptr, peer);
                                   });
    return true;
}

void
PublisherI::createSessionAsync(shared_ptr<DataStormContract::SubscriberPrx> t,
                               shared_ptr<DataStormContract::SubscriberSessionPrx> s,
                               function<void (const shared_ptr<DataStormContract::PublisherSessionPrx>&)> response,
                               function<void (exception_ptr)>,
                               const Ice::Current& c)
{
    unique_lock<mutex> lock(_mutex);
    auto session = createSessionServant(t);
    if(!session)
    {
        response(nullptr);
        return; // Shutting down.
    }
    if(!c.con->getAdapter())
    {
        // Setup the bi-dir connection before sending the reply, the peer calls initTopics as
        // it got the reply.
        c.con->setAdapter(_instance->getObjectAdapter());
    }
    response(Ice::uncheckedCast<DataStormContract::PublisherSessionPrx>(session->getProxy())); // Must be called before connected

    // TODO: Check if the session is already connected?
    // if(session->getSession())
    // {
    //      return;
    // }
    session->connected(s, c.con, _instance->getTopicFactoryI()->getTopicWriters());

    //
    // Attach to available topics from peer.
    //
}

void
PublisherI::sessionConnected(const shared_ptr<SessionI>& session,
                             const shared_ptr<DataStormContract::SubscriberSessionPrx>& prx,
                             const shared_ptr<DataStormContract::PeerPrx>& peer)
{
    if(!prx) // This might occurs if the peer is shutting down.
    {
        try
        {
            session->getProxy()->destroyAsync();
        }
        catch(const Ice::LocalException&)
        {
            // Ignore
        }
    }
    else
    {
        unique_lock<mutex> lock(_mutex);
        session->connected(prx, peer->ice_getCachedConnection(), _instance->getTopicFactoryI()->getTopicWriters());
    }
}

shared_ptr<SessionI>
PublisherI::makeSessionServant(const shared_ptr<DataStormContract::PeerPrx>& peer)
{
    return make_shared<PublisherSessionI>(this, peer);
}

SubscriberI::SubscriberI(const shared_ptr<Instance>& instance) : PeerI(instance)
{
}

bool
SubscriberI::createSession(const shared_ptr<DataStormContract::PeerPrx>& peer)
{
    unique_lock<mutex> lock(_mutex);
    auto session = createSessionServant(peer);
    if(!session)
    {
        return false; // Shutting down.
    }

    auto proxy = session->getSession();
    if(proxy)
    {
        return false; // Already connected.
    }

    auto self = dynamic_pointer_cast<SubscriberI>(shared_from_this());
    auto publisher = Ice::uncheckedCast<DataStormContract::PublisherPrx>(peer);
    publisher->createSessionAsync(Ice::uncheckedCast<DataStormContract::SubscriberPrx>(_proxy),
                                  Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(session->getProxy()),
                                  [self, session, peer](shared_ptr<DataStormContract::PublisherSessionPrx> s)
                                  {
                                      self->sessionConnected(session, s, peer);
                                  },
                                  [self, session, peer](exception_ptr e)
                                  {
                                      self->sessionConnected(session, nullptr, peer);
                                  });
    return true;
}

void
SubscriberI::createSessionAsync(shared_ptr<DataStormContract::PublisherPrx> t,
                                shared_ptr<DataStormContract::PublisherSessionPrx> s,
                                function<void (const shared_ptr<DataStormContract::SubscriberSessionPrx>&)> response,
                                function<void (exception_ptr)>,
                                const Ice::Current& c)
{
    unique_lock<mutex> lock(_mutex);
    auto session = createSessionServant(t);
    if(!session)
    {
        response(nullptr);
        return; // Shutting down.
    }
    if(!c.con->getAdapter())
    {
        // Setup the bi-dir connection before sending the reply, the peer calls initTopics as
        // it got the reply.
        c.con->setAdapter(_instance->getObjectAdapter());
    }
    response(Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(session->getProxy())); // Must be called before connected

    // TODO: Check if the session is already connected?
    // if(session->getSession())
    // {
    //      return;
    // }
    session->connected(s, c.con, _instance->getTopicFactoryI()->getTopicReaders());
}

void
SubscriberI::sessionConnected(const shared_ptr<SessionI>& session,
                              const shared_ptr<DataStormContract::PublisherSessionPrx>& prx,
                              const shared_ptr<DataStormContract::PeerPrx>& peer)
{
    if(!prx) // This might occurs if the peer is shutting down.
    {
        try
        {
            session->getProxy()->destroyAsync();
        }
        catch(const Ice::LocalException&)
        {
            // Ignore
        }
    }
    else
    {
        unique_lock<mutex> lock(_mutex);
        session->connected(prx, peer->ice_getCachedConnection(), _instance->getTopicFactoryI()->getTopicReaders());
    }
}

shared_ptr<SessionI>
SubscriberI::makeSessionServant(const shared_ptr<DataStormContract::PeerPrx>& peer)
{
    return make_shared<SubscriberSessionI>(this, peer);
}
