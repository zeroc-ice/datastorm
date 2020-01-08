// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/NodeSessionManager.h>
#include <DataStorm/NodeSessionI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/TraceUtil.h>
#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormI;
using namespace DataStormContract;

namespace
{

class DispatchInterceptorI : public Ice::DispatchInterceptor
{
public:

    DispatchInterceptorI(shared_ptr<NodeI> node, shared_ptr<CallbackExecutor> executor) :
        _node(move(node)), _executor(move(executor))
    {
    }

    virtual bool dispatch(Ice::Request& req) override
    {
        auto session = _node->getSession(req.getCurrent().id);
        if(!session)
        {
            throw Ice::ObjectNotExistException(__FILE__, __LINE__);
        }
        auto sync = session->ice_dispatch(req);
        _executor->flush();
        return sync;
    }

private:

    shared_ptr<NodeI> _node;
    shared_ptr<CallbackExecutor> _executor;
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
    assert(_subscriberSessions.empty());
    assert(_publisherSessions.empty());
}

void
NodeI::init()
{
    auto self = shared_from_this();
    auto instance = getInstance();
    auto forwarder = [self=shared_from_this()](Ice::ByteSeq e, const Ice::Current& c) { self->forward(e, c); };
    _subscriberForwarder = Ice::uncheckedCast<SubscriberSessionPrx>(instance->getCollocatedForwarder()->add(forwarder));
    _publisherForwarder = Ice::uncheckedCast<PublisherSessionPrx>(instance->getCollocatedForwarder()->add(forwarder));
    try
    {
        auto adapter = instance->getObjectAdapter();
        _proxy = Ice::uncheckedCast<NodePrx>(adapter->addWithUUID(self));

        auto interceptor = make_shared<DispatchInterceptorI>(self, instance->getCallbackExecutor());
        adapter->addDefaultServant(interceptor, "s");
        adapter->addDefaultServant(interceptor, "p");
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

void
NodeI::destroy(bool ownsCommunicator)
{
    unique_lock<mutex> lock(_mutex);
    if(!ownsCommunicator)
    {
        //
        // Notifies peer sessions of the disconnection.
        //
        auto disconnect = [](auto p)
        {
            auto s = p.second->getSession();
            if(s)
            {
                try
                {
                    s->disconnectedAsync();
                }
                catch(const Ice::LocalException&)
                {
                }
            }
        };
        for(const auto& p : _subscribers)
        {
            disconnect(p);
        }
        for(const auto& p : _publishers)
        {
            disconnect(p);
        }
    }
    _subscribers.clear();
    _publishers.clear();
    _subscriberSessions.clear();
    _publisherSessions.clear();
}

void
NodeI::initiateCreateSession(shared_ptr<NodePrx> publisher, const Ice::Current& current)
{
    //
    // Create a session with the given publisher.
    //
    createPublisherSession(move(publisher), current.con, nullptr);
}

void
NodeI::createSession(shared_ptr<NodePrx> subscriber,
                     shared_ptr<SubscriberSessionPrx> subscriberSession,
                     bool fromRelay,
                     const Ice::Current& current)
{
    auto s = subscriber;
    if(fromRelay)
    {
        //
        // If the call is from a relay, we check if we already have a connection to this node
        // and eventually re-use it. Otherwise, we'll try to establish a connection to the node
        // if it has endpoints. If it doesn't, we'll re-use the current connection to send the
        // confirmation.
        //
        s = getNodeWithExistingConnection(subscriber, current.con);
    }
    else if(current.con)
    {
        s = subscriber->ice_fixed(current.con);
    }

    shared_ptr<PublisherSessionI> session;
    try
    {
        unique_lock<mutex> lock(_mutex);
        session = createPublisherSessionServant(subscriber);
        if(!session || session->getSession())
        {
            return; // Shutting down or already connected
        }

        auto self = shared_from_this();
        s->ice_getConnectionAsync([=](auto connection) mutable
        {
            if(session->getSession())
            {
                return;
            }

            if(connection && !connection->getAdapter())
            {
                connection->setAdapter(getInstance()->getObjectAdapter());
            }

            if(connection)
            {
                subscriberSession = subscriberSession->ice_fixed(connection);
            }

            try
            {
                // Must be called before connected
                s->confirmCreateSessionAsync(_proxy,
                                             session->getProxy<PublisherSessionPrx>(),
                                             nullptr,
                                             [=](auto ex)
                                             {
                                                self->removePublisherSession(subscriber, session, ex);
                                             });
                assert(!s->ice_getCachedConnection() || s->ice_getCachedConnection() == connection);
                session->connected(subscriberSession, connection, getInstance()->getTopicFactory()->getTopicWriters());
            }
            catch(const Ice::LocalException&)
            {
                removePublisherSession(subscriber, session, current_exception());
            }
        },
        [=](auto ex)
        {
            self->removePublisherSession(subscriber, session, ex);
        });
    }
    catch(const Ice::LocalException&)
    {
        removePublisherSession(subscriber, session, current_exception());
    }
}

void
NodeI::confirmCreateSession(shared_ptr<NodePrx> publisher,
                            shared_ptr<PublisherSessionPrx> publisherSession,
                            const Ice::Current& current)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _subscribers.find(publisher->ice_getIdentity());
    if(p == _subscribers.end())
    {
        return;
    }

    auto session = p->second;
    if(session->getSession())
    {
        return;
    }

    if(current.con && publisherSession->ice_getEndpoints().empty() && publisherSession->ice_getAdapterId().empty())
    {
        publisherSession = publisherSession->ice_fixed(current.con);
    }

    session->connected(publisherSession, current.con, getInstance()->getTopicFactory()->getTopicReaders());
}

bool
NodeI::createSubscriberSession(shared_ptr<NodePrx> subscriber,
                               const shared_ptr<Ice::Connection>& connection,
                               const shared_ptr<PublisherSessionI>& session)
{
    subscriber = getNodeWithExistingConnection(subscriber, connection);
    try
    {
        auto self = shared_from_this();
        subscriber->ice_getConnectionAsync([=](auto connection)
        {
            if(connection && !connection->getAdapter())
            {
                connection->setAdapter(getInstance()->getObjectAdapter());
            }
            subscriber->initiateCreateSessionAsync(_proxy,
                                                   nullptr,
                                                   [=](auto ex)
                                                   {
                                                      self->removePublisherSession(subscriber, session, ex);
                                                   });
        },
        [=](auto ex)
        {
            self->removePublisherSession(subscriber, session, ex);
        });
        return true;
    }
    catch(const Ice::LocalException&)
    {
        removePublisherSession(subscriber, session, current_exception());
    }
    return false;
}

bool
NodeI::createPublisherSession(const shared_ptr<NodePrx>& publisher,
                              const shared_ptr<Ice::Connection>& con,
                              shared_ptr<SubscriberSessionI> session)
{
    auto p = getNodeWithExistingConnection(publisher, con);
    try
    {
        unique_lock<mutex> lock(_mutex);
        if(!session)
        {
            session = createSubscriberSessionServant(publisher);
        }
        if(!session || session->getSession())
        {
            return true;
        }

        auto self = shared_from_this();
        p->ice_getConnectionAsync([=](auto connection)
        {
            if(session->getSession())
            {
                return;
            }

            if(connection && !connection->getAdapter())
            {
                connection->setAdapter(getInstance()->getObjectAdapter());
            }

            try
            {
                p->createSessionAsync(_proxy,
                                      session->getProxy<SubscriberSessionPrx>(),
                                      false,
                                      nullptr,
                                      [=](auto ex)
                                      {
                                        self->removeSubscriberSession(publisher, session, ex);
                                      });
            }
            catch(const Ice::LocalException&)
            {
                removeSubscriberSession(publisher, session, current_exception());
            }
        },
        [=](auto ex)
        {
            self->removeSubscriberSession(publisher, session, ex);
        });
        return true;
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
        removeSubscriberSession(publisher, session, current_exception());
    }
    catch(const Ice::CommunicatorDestroyedException&)
    {
        removeSubscriberSession(publisher, session, current_exception());
    }
    return false;
}

void
NodeI::removeSubscriberSession(const shared_ptr<NodePrx>& node,
                               const shared_ptr<SubscriberSessionI>& session,
                               const exception_ptr& ex)
{
    if(session && !session->retry(node, ex))
    {
        unique_lock<mutex> lock(_mutex);
        if(!session->getSession() && session->getNode() == node)
        {
            auto p = _subscribers.find(session->getNode()->ice_getIdentity());
            if(p != _subscribers.end() && p->second == session)
            {
                _subscribers.erase(p);
                _subscriberSessions.erase(session->getProxy()->ice_getIdentity());
                session->destroyImpl(ex);
            }
        }
    }
}

void
NodeI::removePublisherSession(const shared_ptr<NodePrx>& node,
                              const shared_ptr<PublisherSessionI>& session,
                              const exception_ptr& ex)
{
    if(session && !session->retry(node, ex))
    {
        unique_lock<mutex> lock(_mutex);
        if(!session->getSession() && session->getNode() == node)
        {
            auto p = _publishers.find(session->getNode()->ice_getIdentity());
            if(p != _publishers.end() && p->second == session)
            {
                _publishers.erase(p);
                _publisherSessions.erase(session->getProxy()->ice_getIdentity());
                session->destroyImpl(ex);
            }
        }
    }
}

shared_ptr<Ice::Connection>
NodeI::getSessionConnection(const string& id) const
{
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

shared_ptr<SessionI>
NodeI::getSession(const Ice::Identity& ident) const
{
    unique_lock<mutex> lock(_mutex);
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

shared_ptr<SubscriberSessionI>
NodeI::createSubscriberSessionServant(const shared_ptr<NodePrx>& node)
{
    auto p = _subscribers.find(node->ice_getIdentity());
    if(p != _subscribers.end())
    {
        p->second->setNode(node);
        return p->second;
    }
    else
    {
        try
        {
            auto session = make_shared<SubscriberSessionI>(shared_from_this(), node);
            ostringstream os;
            os << ++_nextSubscriberSessionId;
            auto prx = getInstance()->getObjectAdapter()->createProxy({ os.str(), "s" })->ice_oneway();
            session->init(Ice::uncheckedCast<SessionPrx>(prx));
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
        p->second->setNode(node);
        return p->second;
    }
    else
    {
        try
        {
            auto session = make_shared<PublisherSessionI>(shared_from_this(), node);
            ostringstream os;
            os << ++_nextPublisherSessionId;
            auto prx = getInstance()->getObjectAdapter()->createProxy({ os.str(), "p" })->ice_oneway();
            session->init(Ice::uncheckedCast<SessionPrx>(prx));
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

shared_ptr<NodePrx>
NodeI::getNodeWithExistingConnection(const shared_ptr<NodePrx>& node, const shared_ptr<Ice::Connection>& con)
{
    shared_ptr<Ice::Connection> connection;

    //
    // If the node has a session with this node, use a bi-dir proxy associated with
    // node session's connection.
    //
    auto instance = _instance.lock();
    if(instance)
    {
        auto nodeSession = instance->getNodeSessionManager()->getSession(node->ice_getIdentity());
        if(nodeSession)
        {
            connection = nodeSession->getConnection();
        }
    }

    //
    // Otherwise, check if the node already has a session established and use the connection
    // from the session.
    //
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _subscribers.find(node->ice_getIdentity());
        if(p != _subscribers.end())
        {
            connection = p->second->getConnection();
        }

        auto q = _publishers.find(node->ice_getIdentity());
        if(q != _publishers.end())
        {
            connection = q->second->getConnection();
        }
    }

    //
    // Make sure the connection is still valid.
    //
    if(connection)
    {
        try
        {
            connection->throwException();
        }
        catch(...)
        {
            connection = nullptr;
        }
    }

    if(!connection && node->ice_getEndpoints().empty() && node->ice_getAdapterId().empty())
    {
        connection = con;
    }

    if(connection)
    {
        if(!connection->getAdapter())
        {
            connection->setAdapter(instance->getObjectAdapter());
        }
        return node->ice_fixed(connection);
    }
    else
    {
        //
        // Ensure that the returned proxy doesn't have a cached connection.
        //
        return node->ice_connectionCached(false)->ice_connectionCached(true);
    }
}
