// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;
using namespace DataStormContract;

NodeI::NodeI(const shared_ptr<Instance>& instance) :
    _instance(instance),
    _subscriberForwarder(Ice::uncheckedCast<SubscriberSessionPrx>(_instance->getForwarderManager()->add(this))),
    _publisherForwarder(Ice::uncheckedCast<PublisherSessionPrx>(_instance->getForwarderManager()->add(this)))
{
}

NodeI::~NodeI()
{
    _instance->getForwarderManager()->remove(_subscriberForwarder->ice_getIdentity());
    _instance->getForwarderManager()->remove(_publisherForwarder->ice_getIdentity());
}

void
NodeI::init()
{
    try
    {
        auto adapter = _instance->getObjectAdapter();
        _proxy = Ice::uncheckedCast<NodePrx>(adapter->addWithUUID(shared_from_this()));
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

bool
NodeI::createSubscriberSession(const shared_ptr<NodePrx>& subscriber)
{
    unique_lock<mutex> lock(_mutex);
    auto session = createPublisherSessionServant(subscriber);
    if(!session)
    {
        return false; // Shutting down.
    }

    auto proxy = session->getSession();
    if(proxy)
    {
        return false; // Already connected.
    }

    auto self = dynamic_pointer_cast<NodeI>(shared_from_this());
    subscriber->createSubscriberSessionAsync(Ice::uncheckedCast<NodePrx>(_proxy),
                                             Ice::uncheckedCast<PublisherSessionPrx>(session->getProxy()),
                                             [self, session, subscriber](shared_ptr<SubscriberSessionPrx> s)
                                             {
                                                 self->subscriberSessionConnected(session, s, subscriber);
                                             },
                                             [self, session, subscriber](exception_ptr e)
                                             {
                                                 self->subscriberSessionConnected(session, nullptr, subscriber);
                                             });
    return true;
}

void
NodeI::createSubscriberSessionAsync(shared_ptr<NodePrx> t,
                                    shared_ptr<PublisherSessionPrx> s,
                                    function<void (const shared_ptr<SubscriberSessionPrx>&)> response,
                                    function<void (exception_ptr)>,
                                    const Ice::Current& c)
{
    unique_lock<mutex> lock(_mutex);
    auto session = createSubscriberSessionServant(t);
    if(!session)
    {
        response(nullptr);
        return; // Shutting down.
    }
    if(!c.con->getAdapter())
    {
        // Setup the bi-dir connection before sending the reply, the node calls initTopics as
        // it got the reply.
        c.con->setAdapter(_instance->getObjectAdapter());
    }
    response(Ice::uncheckedCast<SubscriberSessionPrx>(session->getProxy())); // Must be called before connected

    if(!session->getSession())
    {
        session->connected(s, c.con, _instance->getTopicFactory()->getTopicWriters());
    }

    //
    // Attach to available topics from node.
    //
}

void
NodeI::subscriberSessionConnected(const shared_ptr<PublisherSessionI>& session,
                                  const shared_ptr<SubscriberSessionPrx>& prx,
                                  const shared_ptr<NodePrx>& node)
{
    if(!prx) // This might occurs if the node is shutting down.
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
        session->connected(prx, node->ice_getCachedConnection(), _instance->getTopicFactory()->getTopicWriters());
    }
}

void
NodeI::removeSubscriberSession(SubscriberSessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    _subscribers.erase(session->getNode()->ice_getIdentity());
}

bool
NodeI::createPublisherSession(const shared_ptr<NodePrx>& publisher)
{
    unique_lock<mutex> lock(_mutex);
    auto session = createSubscriberSessionServant(publisher);
    if(!session)
    {
        return false; // Shutting down.
    }

    auto proxy = session->getSession();
    if(proxy)
    {
        return false; // Already connected.
    }

    auto self = dynamic_pointer_cast<NodeI>(shared_from_this());
    publisher->createPublisherSessionAsync(Ice::uncheckedCast<NodePrx>(_proxy),
                                           Ice::uncheckedCast<SubscriberSessionPrx>(session->getProxy()),
                                           [self, session, publisher](shared_ptr<PublisherSessionPrx> s)
                                           {
                                               self->publisherSessionConnected(session, s, publisher);
                                           },
                                           [self, session, publisher](exception_ptr e)
                                           {
                                               self->publisherSessionConnected(session, nullptr, publisher);
                                           });
    return true;
}

void
NodeI::createPublisherSessionAsync(shared_ptr<NodePrx> t,
                                   shared_ptr<SubscriberSessionPrx> s,
                                   function<void (const shared_ptr<PublisherSessionPrx>&)> response,
                                   function<void (exception_ptr)>,
                                   const Ice::Current& c)
{
    unique_lock<mutex> lock(_mutex);
    auto session = createPublisherSessionServant(t);
    if(!session)
    {
        response(nullptr);
        return; // Shutting down.
    }
    if(!c.con->getAdapter())
    {
        // Setup the bi-dir connection before sending the reply, the node calls initTopics as
        // it got the reply.
        c.con->setAdapter(_instance->getObjectAdapter());
    }
    response(Ice::uncheckedCast<PublisherSessionPrx>(session->getProxy())); // Must be called before connected

    // TODO: Check if the session is already connected?
    // if(session->getSession())
    // {
    //      return;
    // }
    session->connected(s, c.con, _instance->getTopicFactory()->getTopicReaders());
}

void
NodeI::publisherSessionConnected(const shared_ptr<SubscriberSessionI>& session,
                                 const shared_ptr<PublisherSessionPrx>& prx,
                                 const shared_ptr<NodePrx>& node)
{
    if(!prx) // This might occurs if the node is shutting down.
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
        session->connected(prx, node->ice_getCachedConnection(), _instance->getTopicFactory()->getTopicReaders());
    }
}

void
NodeI::removePublisherSession(PublisherSessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    _publishers.erase(session->getNode()->ice_getIdentity());
}

shared_ptr<SubscriberSessionI>
NodeI::createSubscriberSessionServant(const shared_ptr<NodePrx>& node)
{
    auto p = _subscribers.find(node->ice_getIdentity());
    if(p != _subscribers.end())
    {
        return p->second;
    }
    else
    {
        try
        {
            auto session = make_shared<SubscriberSessionI>(this, node);
            session->init();
            _subscribers.emplace(node->ice_getIdentity(), session);
            return session;
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
            return nullptr;
        }
    }
}

shared_ptr<PublisherSessionI>
NodeI::createPublisherSessionServant(const shared_ptr<NodePrx>& node)
{
    auto p = _publishers.find(node->ice_getIdentity());
    if(p != _publishers.end())
    {
        return p->second;
    }
    else
    {
        try
        {
            auto session = make_shared<PublisherSessionI>(this, node);
            session->init();
            _publishers.emplace(node->ice_getIdentity(), session);
            return session;
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
            return nullptr;
        }
    }
}

void
NodeI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    lock_guard<mutex> lock(_mutex);
    if(current.id == _subscriberForwarder->ice_getIdentity())
    {
        for(const auto s : _subscribers)
        {
            shared_ptr<SessionPrx> session = s.second->getSession();
            if(session)
            {
                session->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
            }
        }
    }
    else
    {
        for(const auto s : _publishers)
        {
            shared_ptr<SessionPrx> session = s.second->getSession();
            if(session)
            {
                session->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
            }
        }
    }
}
