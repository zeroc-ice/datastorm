// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/TraceUtil.h>
#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormI;
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
        return _node->getSession(current.id);
    }

    virtual void finished(const Ice::Current&, const Ice::ObjectPtr&, const std::shared_ptr<void>&) override
    {
        _node->getInstance()->getCallbackExecutor()->flush();
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
    _nextSubscriberSessionId(0),
    _nextPublisherSessionId(0)
{
}

NodeI::~NodeI()
{
    assert(_subscribers.empty());
    assert(_publishers.empty());
}

void
NodeI::init()
{
    auto self = shared_from_this();
    _subscriberForwarder = Ice::uncheckedCast<SubscriberSessionPrx>(_instance->getForwarderManager()->add(self));
    _publisherForwarder = Ice::uncheckedCast<PublisherSessionPrx>(_instance->getForwarderManager()->add(self));
    try
    {
        auto adapter = _instance->getObjectAdapter();
        _proxy = Ice::uncheckedCast<NodePrx>(adapter->addWithUUID(self));

        auto servantLocator = make_shared<ServantLocatorI>(self);
        adapter->addServantLocator(servantLocator, "s");
        adapter->addServantLocator(servantLocator, "p");
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

void
NodeI::destroy()
{
    unique_lock<mutex> lock(_mutex);
    //
    // TODO: destroy explicitly the sessions? The communicator might not be destroyed
    // at this point if it's not owned by the node.
    //
    _subscribers.clear();
    _publishers.clear();
    _subscriberSessions.clear();
    _publisherSessions.clear();
    _instance->getForwarderManager()->remove(_subscriberForwarder->ice_getIdentity());
    _instance->getForwarderManager()->remove(_publisherForwarder->ice_getIdentity());
    _instance->getCallbackExecutor()->destroy();
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
            throw CannotCreateSessionException("node is shutting down");
        }
        else if(session->getSession())
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
                                                     self->removePublisherSession(session, e);
                                                 });
    }
    catch(const Ice::LocalException&)
    {
        removePublisherSession(session, current_exception());
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
        if(publisher->ice_getEndpoints().empty() && publisher->ice_getAdapterId().empty())
        {
            publisher = Ice::uncheckedCast<NodePrx>(c.con->createProxy(publisher->ice_getIdentity()));
        }

        session = createSubscriberSessionServant(publisher);
        if(!session)
        {
            throw CannotCreateSessionException("node is shutting down");
        }

        if(c.con && !c.con->getAdapter())
        {
            // Setup the bi-dir connection before sending the reply, the node calls initTopics as soon as
            // it got the reply.
            c.con->setAdapter(_instance->getObjectAdapter());
        }
        response(Ice::uncheckedCast<SubscriberSessionPrx>(session->getProxy())); // Must be called before connected
        if(!session->getSession())
        {
            session->connected(s, c.con, _instance->getTopicFactory()->getTopicReaders());
        }
    }
    catch(const Ice::LocalException&)
    {
        removeSubscriberSession(session, current_exception());
    }
}

void
NodeI::subscriberSessionConnected(const shared_ptr<PublisherSessionI>& session,
                                  const shared_ptr<SubscriberSessionPrx>& prx,
                                  const shared_ptr<NodePrx>& node)
{
    if(prx)
    {
        try
        {
            unique_lock<mutex> lock(_mutex);
            if(!session->getSession())
            {
                session->connected(prx, node->ice_getCachedConnection(), _instance->getTopicFactory()->getTopicWriters());
            }
        }
        catch(const Ice::LocalException&)
        {
            removePublisherSession(session, current_exception());
        }
    }
}

void
NodeI::removeSubscriberSession(const shared_ptr<SubscriberSessionI>& session, const exception_ptr& ex)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _subscribers.find(session->getNode()->ice_getIdentity());
    if(p != _subscribers.end() && p->second == session)
    {
        _subscribers.erase(p);
        _subscriberSessions.erase(session->getProxy()->ice_getIdentity());
        session->destroyImpl(ex);
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

        if(session->getSession())
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
                                                   self->removeSubscriberSession(session, e);
                                               });
    }
    catch(const Ice::LocalException&)
    {
        removeSubscriberSession(session, current_exception());
        return false;
    }
    return true;
}

void
NodeI::createPublisherSessionAsync(shared_ptr<NodePrx> subscriber,
                                   shared_ptr<SubscriberSessionPrx> s,
                                   function<void (const shared_ptr<PublisherSessionPrx>&)> response,
                                   function<void (exception_ptr)>,
                                   const Ice::Current& c)
{
    shared_ptr<PublisherSessionI> session;
    try
    {
        unique_lock<mutex> lock(_mutex);
        if(subscriber->ice_getEndpoints().empty() && subscriber->ice_getAdapterId().empty())
        {
            subscriber = Ice::uncheckedCast<NodePrx>(c.con->createProxy(subscriber->ice_getIdentity()));
        }

        session = createPublisherSessionServant(subscriber);
        if(!session)
        {
            throw CannotCreateSessionException("node is shutting down");
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
                                                     self->removePublisherSession(session, e);
                                                 });
        response(nullptr);
    }
    catch(const Ice::LocalException&)
    {
        removePublisherSession(session, current_exception());
    }
}

void
NodeI::publisherSessionConnected(const shared_ptr<SubscriberSessionI>& session,
                                 const shared_ptr<PublisherSessionPrx>& prx,
                                 const shared_ptr<NodePrx>& node)
{
    if(prx)
    {
        try
        {
            unique_lock<mutex> lock(_mutex);
            if(!session->getSession())
            {
                session->connected(prx, node->ice_getCachedConnection(), _instance->getTopicFactory()->getTopicReaders());
            }
        }
        catch(const Ice::LocalException&)
        {
            removeSubscriberSession(session, current_exception());
        }
    }
}

void
NodeI::removePublisherSession(const std::shared_ptr<PublisherSessionI>& session, const exception_ptr& ex)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _publishers.find(session->getNode()->ice_getIdentity());
    if(p != _publishers.end() && p->second == session)
    {
        _publishers.erase(p);
        _publisherSessions.erase(session->getProxy()->ice_getIdentity());
        session->destroyImpl(ex);
    }
}

shared_ptr<Ice::Connection>
NodeI::getSessionConnection(const string& id) const
{
    unique_lock<mutex> lock(_mutex);
    auto session = getSession(Ice::stringToIdentity(id));
    if(session)
    {
        return session->getConnection();
    }
    else
    {
        return nullptr;
    }
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
            auto session = make_shared<SubscriberSessionI>(shared_from_this(), node);
            ostringstream os;
            os << ++_nextSubscriberSessionId;
            session->init(Ice::uncheckedCast<SessionPrx>(_instance->getObjectAdapter()->createProxy({ os.str(), "s" })));
            _subscribers.emplace(node->ice_getIdentity(), session);
            _subscriberSessions.emplace(session->getProxy()->ice_getIdentity(), session);
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
            auto session = make_shared<PublisherSessionI>(shared_from_this(), node);
            ostringstream os;
            os << ++_nextPublisherSessionId;
            session->init(Ice::uncheckedCast<SessionPrx>(_instance->getObjectAdapter()->createProxy({ os.str(), "p" })));
            _publishers.emplace(node->ice_getIdentity(), session);
            _publisherSessions.emplace(session->getProxy()->ice_getIdentity(), session);
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

shared_ptr<SessionI>
NodeI::getSession(const Ice::Identity& ident) const
{
    if(ident.category == "s")
    {
        auto p = _subscriberSessions.find(ident);
        if(p != _subscriberSessions.end())
        {
            return p->second;
        }
    }
    else if(ident.category == "p")
    {
        auto p = _publisherSessions.find(ident);
        if(p != _publisherSessions.end())
        {
            return p->second;
        }
    }
    return nullptr;
}
