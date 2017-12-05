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

namespace
{

class ServantLocatorI : public Ice::ServantLocator
{
public:

    ServantLocatorI(const shared_ptr<NodeI>& node) : _node(node)
    {
    }

    virtual shared_ptr<Ice::Object> locate(const Ice::Current& current, std::shared_ptr<void>&) override
    {
        return _node->getServant(current.id);
    }

    virtual void finished(const Ice::Current&, const Ice::ObjectPtr&, const std::shared_ptr<void>&) override
    {
    }

    virtual void deactivate(const string&) override
    {
    }

private:

    const shared_ptr<NodeI> _node;
};

}

NodeI::NodeI(const shared_ptr<Instance>& instance) :
    _instance(instance),
    _subscriberForwarder(Ice::uncheckedCast<SubscriberSessionPrx>(_instance->getForwarderManager()->add(this))),
    _publisherForwarder(Ice::uncheckedCast<PublisherSessionPrx>(_instance->getForwarderManager()->add(this))),
    _nextSubscriberSessionId(0),
    _nextPublisherSessionId(0)
{
}

NodeI::~NodeI()
{
    _instance->getForwarderManager()->remove(_subscriberForwarder->ice_getIdentity());
    _instance->getForwarderManager()->remove(_publisherForwarder->ice_getIdentity());
    assert(_subscribers.empty());
    assert(_publishers.empty());
}

void
NodeI::init()
{
    try
    {
        auto adapter = _instance->getObjectAdapter();
        _proxy = Ice::uncheckedCast<NodePrx>(adapter->addWithUUID(shared_from_this()));

        auto servantLocator = make_shared<ServantLocatorI>(shared_from_this());
        adapter->addServantLocator(servantLocator, "s");
        adapter->addServantLocator(servantLocator, "p");
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

bool
NodeI::createSubscriberSession(const shared_ptr<NodePrx>& subscriber)
{
    std::shared_ptr<PublisherSessionI> session;
    try
    {
        unique_lock<mutex> lock(_mutex);
        session = createPublisherSessionServant(subscriber);
        if(!session)
        {
            return false; // Shutting down.
        }

        auto proxy = session->getSession();
        if(proxy)
        {
            return true; // Already connected.
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
    }
    catch(const Ice::LocalException&)
    {
        removePublisherSession(session.get());
        return false;
    }
    return true;
}

void
NodeI::createSubscriberSessionAsync(shared_ptr<NodePrx> publisher,
                                    shared_ptr<PublisherSessionPrx> s,
                                    function<void (const shared_ptr<SubscriberSessionPrx>&)> response,
                                    function<void (exception_ptr)>,
                                    const Ice::Current& c)
{
    shared_ptr<SubscriberSessionI> session;
    try
    {
        unique_lock<mutex> lock(_mutex);
        auto p = _subscribers.find(publisher->ice_getIdentity());
        if(p != _subscribers.end())
        {
            p->second->addConnectedCallback([response](std::shared_ptr<SessionPrx> prx)
                                            {
                                                response(Ice::uncheckedCast<SubscriberSessionPrx>(prx));
                                            });
            return;
        }

        session = createSubscriberSessionServant(publisher);
        if(!session)
        {
            response(nullptr);
            return; // Shutting down.
        }

        if(c.con && !c.con->getAdapter())
        {
            // Setup the bi-dir connection before sending the reply, the node calls initTopics as
            // it got the reply.
            c.con->setAdapter(_instance->getObjectAdapter());
        }
        response(Ice::uncheckedCast<SubscriberSessionPrx>(session->getProxy())); // Must be called before connected
        session->connected(s, c.con, _instance->getTopicFactory()->getTopicReaders());
    }
    catch(const Ice::LocalException& ex)
    {
        removeSubscriberSession(session.get());
    }
}

void
NodeI::subscriberSessionConnected(const shared_ptr<PublisherSessionI>& session,
                                  const shared_ptr<SubscriberSessionPrx>& prx,
                                  const shared_ptr<NodePrx>& node)
{
    if(!prx) // This might occurs if the node is shutting down.
    {
        removePublisherSession(session.get());
    }
    else
    {
        try
        {
            unique_lock<mutex> lock(_mutex);
            session->connected(prx, node->ice_getCachedConnection(), _instance->getTopicFactory()->getTopicWriters());
        }
        catch(const Ice::LocalException& ex)
        {
            removePublisherSession(session.get());
        }
    }
}

void
NodeI::removeSubscriberSession(SubscriberSessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    if(_subscribers.erase(session->getNode()->ice_getIdentity()))
    {
        _sessions.erase(session->getProxy()->ice_getIdentity());
        session->destroyImpl();
    }
}

bool
NodeI::createPublisherSession(const shared_ptr<NodePrx>& publisher)
{
    std::shared_ptr<SubscriberSessionI> session;
    try
    {
        unique_lock<mutex> lock(_mutex);
        session = createSubscriberSessionServant(publisher);
        if(!session)
        {
            return false; // Shutting down.
        }

        auto proxy = session->getSession();
        if(proxy)
        {
            return true; // Already connected.
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
    }
    catch(const Ice::LocalException&)
    {
        removeSubscriberSession(session.get());
        return false;
    }
    return true;
}

void
NodeI::createPublisherSessionAsync(shared_ptr<NodePrx> t,
                                   shared_ptr<SubscriberSessionPrx> s,
                                   function<void (const shared_ptr<PublisherSessionPrx>&)> response,
                                   function<void (exception_ptr)>,
                                   const Ice::Current& c)
{
    shared_ptr<PublisherSessionI> session;
    try
    {
        unique_lock<mutex> lock(_mutex);
        session = createPublisherSessionServant(t);
        if(!session)
        {
            response(nullptr);
            return; // Shutting down.
        }

        if(c.con && !c.con->getAdapter())
        {
            // Setup the bi-dir connection before sending the reply, the node calls initTopics as
            // it got the reply.
            c.con->setAdapter(_instance->getObjectAdapter());
        }
        response(Ice::uncheckedCast<PublisherSessionPrx>(session->getProxy())); // Must be called before connected
        session->connected(s, c.con, _instance->getTopicFactory()->getTopicWriters());
    }
    catch(const Ice::LocalException& ex)
    {
        removePublisherSession(session.get());
    }
}

void
NodeI::publisherSessionConnected(const shared_ptr<SubscriberSessionI>& session,
                                 const shared_ptr<PublisherSessionPrx>& prx,
                                 const shared_ptr<NodePrx>& node)
{
    if(!prx) // This might occurs if the node is shutting down.
    {
        removeSubscriberSession(session.get());
    }
    else
    {
        try
        {
            unique_lock<mutex> lock(_mutex);
            session->connected(prx, node->ice_getCachedConnection(), _instance->getTopicFactory()->getTopicReaders());
        }
        catch(const Ice::LocalException& ex)
        {
            removeSubscriberSession(session.get());
        }
    }
}

void
NodeI::removePublisherSession(PublisherSessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    if(_publishers.erase(session->getNode()->ice_getIdentity()))
    {
        _sessions.erase(session->getProxy()->ice_getIdentity());
        session->destroyImpl();
    }
}

shared_ptr<Ice::Connection>
NodeI::getSessionConnection(const string& id) const
{
    unique_lock<mutex> lock(_mutex);
    auto p = _sessions.find(Ice::stringToIdentity(id));
    if(p != _sessions.end())
    {
        return p->second->getConnection();
    }
    return nullptr;
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
            ostringstream os;
            os << ++_nextSubscriberSessionId;
            session->init(Ice::uncheckedCast<SessionPrx>(_instance->getObjectAdapter()->createProxy({ os.str(), "s" })));
            _subscribers.emplace(node->ice_getIdentity(), session);
            _sessions.emplace(session->getProxy()->ice_getIdentity(), session);
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
            ostringstream os;
            os << ++_nextPublisherSessionId;
            session->init(Ice::uncheckedCast<SessionPrx>(_instance->getObjectAdapter()->createProxy({ os.str(), "p" })));
            _publishers.emplace(node->ice_getIdentity(), session);
            _sessions.emplace(session->getProxy()->ice_getIdentity(), session);
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

shared_ptr<Ice::Object>
NodeI::getServant(const Ice::Identity& ident) const
{
    auto p = _sessions.find(ident);
    if(p != _sessions.end())
    {
        return p->second;
    }
    return nullptr;
}
