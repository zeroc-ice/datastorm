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

TopicPeer::DataElementForwarderI::DataElementForwarderI(shared_ptr<TopicPeer> topic, const shared_ptr<Key>& key) :
    _topic(topic), _key(key)
{
}

bool
TopicPeer::DataElementForwarderI::ice_invoke(Ice::ByteSeq inEncaps, Ice::ByteSeq&, const Ice::Current& current)
{
    _topic->forward(_key, inEncaps, current);
    return true;
}

TopicPeer::FilteredDataElementForwarderI::FilteredDataElementForwarderI(shared_ptr<TopicPeer> topic, const string& filter) :
    _topic(topic), _filter(filter)
{
}

bool
TopicPeer::FilteredDataElementForwarderI::ice_invoke(Ice::ByteSeq inEncaps, Ice::ByteSeq&, const Ice::Current& current)
{
    _topic->forward(_filter, inEncaps, current);
    return true;
}

TopicPeer::ForwarderI::ForwarderI(shared_ptr<TopicPeer> topic) : _topic(topic)
{
}

bool
TopicPeer::ForwarderI::ice_invoke(Ice::ByteSeq inEncaps, Ice::ByteSeq&, const Ice::Current& current)
{
    _topic->forward(inEncaps, current);
    return true;
}

TopicPeer::TopicPeer(TopicI* topic) :
    _topic(topic), _traceLevels(_topic->getInstance()->getTraceLevels())
{
}

void
TopicPeer::init()
{
    try
    {
        auto prx = _topic->getInstance()->getObjectAdapter()->addWithUUID(make_shared<ForwarderI>(shared_from_this()));
        _forwarder = Ice::uncheckedCast<DataStormContract::SessionPrx>(prx);
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

void
TopicPeer::waitForKeyListeners(const shared_ptr<Key>& key, int count) const
{
    unique_lock<mutex> lock(_mutex);
    while(true)
    {
        size_t total = 0;
        auto p = _listeners.find(key);
        if(p != _listeners.end())
        {
            total += p->second.getSessionCount();
        }
        for(const auto& filter : _filters)
        {
            if(key->match(filter.first))
            {
                total += filter.second.getSessionCount();
            }
        }
        if(count < 0 ? total == 0 : total >= static_cast<size_t>(count))
        {
            return;
        }
        _cond.wait(lock);
    }
}

bool
TopicPeer::hasKeyListeners(const shared_ptr<Key>& key) const
{
    unique_lock<mutex> lock(_mutex);
    if(_listeners.find(key) != _listeners.end())
    {
        return true;
    }
    for(const auto& filter : _filters)
    {
        if(key->match(filter.first))
        {
            return true;
        }
    }
    return false;
}

void
TopicPeer::waitForFilteredListeners(const string& filter, int count) const
{
    unique_lock<mutex> lock(_mutex);
    while(true)
    {
        size_t total = 0;
        for(const auto& listener : _listeners)
        {
            if(listener.first->match(filter))
            {
                total += listener.second.getSessionCount();
            }
        }
        if(count < 0 ? total == 0 : total >= static_cast<size_t>(count))
        {
            return;
        }
        _cond.wait(lock);
    }
}

bool
TopicPeer::hasFilteredListeners(const string& filter) const
{
    unique_lock<mutex> lock(_mutex);
    for(const auto& listener : _listeners)
    {
        if(listener.first->match(filter))
        {
            return true;
        }
    }
    return false;
}

void
TopicPeer::addSession(SessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "added session `" << session << "' to topic `" << _topic << "'";
    }
    _sessions.insert(session);
    _cond.notify_all();
}

void
TopicPeer::removeSession(SessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "removed session `" << session << "' from topic `" << _topic << "'";
    }

    for(auto listener : session->_listeners[this])
    {
        if(listener->removeSession(session))
        {
            _listeners.erase(listener->getKey());
        }
    }
    session->_listeners.erase(this);

    for(auto filter : session->_filters[this])
    {
        if(filter->removeSession(session))
        {
            _filters.erase(filter->getFilter());
        }
    }
    session->_filters.erase(this);

    _sessions.erase(session);
    _cond.notify_all();
}

bool
TopicPeer::addKeyListener(const shared_ptr<Key>& key, SessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _listeners.find(key);
    if(p == _listeners.end())
    {
        p = _listeners.emplace(key, Listener { this, key }).first;
    }

    auto& listeners = session->_listeners[this];
    if(listeners.find(&p->second) == listeners.end())
    {
        if(_traceLevels->session > 0)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << "added key `" << key << "' for session `" << session << "' to topic `" << _topic << "'";
        }
        p->second.addSession(session);
        listeners.insert(&p->second);
        _cond.notify_all();
    }
    return p->second.getSessionCount() == 1;
}

void
TopicPeer::removeKeyListener(const shared_ptr<Key>& key, SessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "removed key `" << key << "' for session `" << session << "' from topic `" << _topic << "'";
    }

    auto p = _listeners.find(key);
    assert(p != _listeners.end());
    session->_listeners[this].erase(&p->second);
    if(p->second.removeSession(session))
    {
        _listeners.erase(p);
    }
    _cond.notify_all();
}

void
TopicPeer::addFilteredListener(const string& filter, SessionI* session)
{
    unique_lock<mutex> lock(_mutex);

    auto p = _filters.find(filter);
    if(p == _filters.end())
    {
        p = _filters.emplace(filter, Filter { this, filter }).first;
    }

    auto& filters = session->_filters[this];
    if(filters.find(&p->second) == filters.end())
    {
        if(_traceLevels->session > 0)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << "added filter `" << filter << "' for session `" << session << "' to topic `" << _topic << "'";
        }
        p->second.addSession(session);
        filters.insert(&p->second);
        _cond.notify_all();
    }
}

void
TopicPeer::removeFilteredListener(const string& filter, SessionI* session)
{
    unique_lock<mutex> lock(_mutex);
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "removed filter `" << filter << "' for session `" << session << "' from topic `" << _topic << "'";
    }

    auto p = _filters.find(filter);
    session->_filters[this].erase(&p->second);
    if(p->second.removeSession(session))
    {
        _filters.erase(p);
    }
    _cond.notify_all();
}

void
TopicPeer::forEachPeerKey(const function<void(const shared_ptr<Key>&)>& fn, const string& filter) const
{
    unique_lock<mutex> lock(_mutex);
    for(const auto& listener : _listeners)
    {
        if(listener.first->match(filter))
        {
            fn(listener.first);
        }
    }
}

void
TopicPeer::subscribe(const string& filter)
{
    unique_lock<mutex> lock(_mutex);
    for(const auto& s : _sessions)
    {
        shared_ptr<DataStormContract::SessionPrx> prx = s->getSession();
        if(prx)
        {
            prx->attachFilterAsync(_topic->getName(), s->getLastId(_topic->getName()), filter);
        }
    }
}

void
TopicPeer::unsubscribe(const string& filter)
{
    unique_lock<mutex> lock(_mutex);
    for(const auto& s : _sessions)
    {
        shared_ptr<DataStormContract::SessionPrx> prx = s->getSession();
        if(prx)
        {
            prx->detachFilterAsync(_topic->getName(), filter);
        }
    }
}

void
TopicPeer::subscribe(const shared_ptr<Key>& key)
{
    unique_lock<mutex> lock(_mutex);
    for(const auto& s : _sessions)
    {
        shared_ptr<DataStormContract::SessionPrx> prx = s->getSession();
        if(prx)
        {
            // TODO: key priority
            prx->attachKeyAsync(_topic->getName(), s->getLastId(_topic->getName()), { key->marshal(), 0 });
        }
    }
}

void
TopicPeer::unsubscribe(const shared_ptr<Key>& key)
{
    unique_lock<mutex> lock(_mutex);
    for(const auto& s : _sessions)
    {
        shared_ptr<DataStormContract::SessionPrx> prx = s->getSession();
        if(prx)
        {
            prx->detachKeyAsync(_topic->getName(), key->marshal());
        }
    }
}

shared_ptr<DataStormContract::SessionPrx>
TopicPeer::addForwarder(const shared_ptr<Key>& key)
{
    unique_lock<mutex> lock(_mutex);

    for(auto& filter : _filters)
    {
        filter.second.addKey(key);
    }
    _cond.notify_all();

    try
    {
        auto servant = make_shared<DataElementForwarderI>(shared_from_this(), key);
        auto prx = _topic->getInstance()->getObjectAdapter()->addWithUUID(servant);
        return _forwarders.insert(make_pair(key, Ice::uncheckedCast<DataStormContract::SessionPrx>(prx))).first->second;
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    return 0;
}

void
TopicPeer::removeForwarder(const shared_ptr<Key>& key)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _forwarders.find(key);
    try
    {
        _topic->getInstance()->getObjectAdapter()->remove(p->second->ice_getIdentity());
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    _forwarders.erase(p);

    for(auto& filter : _filters)
    {
        filter.second.removeKey(key);
    }
    _cond.notify_all();
}

shared_ptr<DataStormContract::SessionPrx>
TopicPeer::addForwarder(const string& filter)
{
    unique_lock<mutex> lock(_mutex);
    try
    {
        auto servant = make_shared<FilteredDataElementForwarderI>(shared_from_this(), filter);
        auto prx = _topic->getInstance()->getObjectAdapter()->addWithUUID(servant);
        return _filterForwarders.insert(make_pair(filter, Ice::uncheckedCast<DataStormContract::SessionPrx>(prx))).first->second;
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    return 0;
}

void
TopicPeer::removeForwarder(const string& filter)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _filterForwarders.find(filter);
    try
    {
        _topic->getInstance()->getObjectAdapter()->remove(p->second->ice_getIdentity());
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    _filterForwarders.erase(p);
    _cond.notify_all();
}

void
TopicPeer::destroy()
{
    try
    {
        _topic->getInstance()->getObjectAdapter()->remove(_forwarder->ice_getIdentity());
        for(const auto& forwarder : _forwarders)
        {
            _topic->getInstance()->getObjectAdapter()->remove(forwarder.second->ice_getIdentity());
        }
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

void
TopicPeer::Listener::forward(const Ice::ByteSeq& encaps, const Ice::Current& current)
{
    for(const auto& session : _sessions)
    {
        std::shared_ptr<DataStormContract::SessionPrx> prx = session.first->getSession();
        if(prx)
        {
            prx->ice_invokeAsync(current.operation, current.mode, encaps, current.ctx);
        }
    }
}

void
TopicPeer::Filter::forward(const Ice::ByteSeq& encaps, const Ice::Current& current)
{
    for(const auto& session : _sessions)
    {
        std::shared_ptr<DataStormContract::SessionPrx> prx = session.first->getSession();
        if(prx)
        {
            prx->ice_invokeAsync(current.operation, current.mode, encaps, current.ctx);
        }
    }
}

void
TopicPeer::forward(const Ice::ByteSeq& encaps, const Ice::Current& current)
{
    unique_lock<mutex> lock(_mutex);
    for(const auto& s : _sessions)
    {
        shared_ptr<DataStormContract::SessionPrx> prx = s->getSession();
        if(prx)
        {
            prx->ice_invokeAsync(current.operation, current.mode, encaps, current.ctx);
        }
    }
}

void
TopicPeer::forward(const shared_ptr<Key>& key, const Ice::ByteSeq& encaps, const Ice::Current& current)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _listeners.find(key);
    if(p != _listeners.end())
    {
        p->second.forward(encaps, current);
    }
}

void
TopicPeer::forward(const string& filter, const Ice::ByteSeq& encaps, const Ice::Current& current)
{
    unique_lock<mutex> lock(_mutex);
    auto p = _filters.find(filter);
    if(p != _filters.end())
    {
        p->second.forward(encaps, current);
    }
}

TopicPublisher::TopicPublisher(PublisherI* publisher, TopicWriterI* writer) :
    TopicPeer(writer), _writer(writer)
{
    writer->getInstance()->getTopicLookup()->announceTopicPublisherAsync(writer->getName(), publisher->getProxy());
}

TopicSubscriber::TopicSubscriber(SubscriberI* subscriber, TopicReaderI* reader) :
    TopicPeer(reader), _reader(reader)
{
    reader->getInstance()->getTopicLookup()->announceTopicSubscriberAsync(reader->getName(), subscriber->getProxy());
}

PeerI::ForwarderI::ForwarderI(shared_ptr<PeerI> peer) : _peer(peer)
{
}

bool
PeerI::ForwarderI::ice_invoke(Ice::ByteSeq inEncaps, Ice::ByteSeq&, const Ice::Current& current)
{
    _peer->forward(inEncaps, current);
    return true;
}

PeerI::PeerI(shared_ptr<Instance> instance) : _instance(instance)
{
}

void
PeerI::init()
{
    try
    {
        auto adapter = _instance->getObjectAdapter();
        _proxy = Ice::uncheckedCast<DataStormContract::PeerPrx>(adapter->addWithUUID(shared_from_this()));
        auto prx = adapter->addWithUUID(make_shared<ForwarderI>(shared_from_this()));
        _forwarder = Ice::uncheckedCast<DataStormContract::SessionPrx>(prx);
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

void
PeerI::checkSessions(shared_ptr<TopicI> topic)
{
    lock_guard<mutex> lock(_mutex);
    for(const auto s : _sessions)
    {
        s.second->checkTopic(topic);
    }
}

shared_ptr<SessionI>
PeerI::createSessionServant(shared_ptr<DataStormContract::PeerPrx> peer)
{
    auto id = peer->ice_getIdentity();
    auto p = _sessions.find(id);
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
            _sessions.insert(make_pair(id, session));
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
PeerI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current)
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

PublisherI::PublisherI(shared_ptr<Instance> instance) : PeerI(instance)
{
}

bool
PublisherI::createSession(shared_ptr<DataStormContract::PeerPrx> peer)
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
    session->connected(s, c.con, _instance->getTopicFactoryI()->getTopicWriters());
}

void
PublisherI::sessionConnected(shared_ptr<SessionI> session,
                             shared_ptr<DataStormContract::SubscriberSessionPrx> prx,
                             shared_ptr<DataStormContract::PeerPrx> peer)
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
        return;
    }
    unique_lock<mutex> lock(_mutex);
    session->connected(prx, peer->ice_getCachedConnection(), _instance->getTopicFactoryI()->getTopicWriters());
}

shared_ptr<SessionI>
PublisherI::makeSessionServant(shared_ptr<DataStormContract::PeerPrx> peer)
{
    return make_shared<PublisherSessionI>(this, peer);
}

shared_ptr<TopicPublisher>
PublisherI::createTopicPublisher(TopicWriterI* writer)
{
    auto impl = make_shared<TopicPublisher>(this, writer);
    impl->init();
    return impl;
}

SubscriberI::SubscriberI(shared_ptr<Instance> instance) : PeerI(instance)
{
}

bool
SubscriberI::createSession(shared_ptr<DataStormContract::PeerPrx> peer)
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
    session->connected(s, c.con, _instance->getTopicFactoryI()->getTopicReaders());
}

void
SubscriberI::sessionConnected(shared_ptr<SessionI> session,
                              shared_ptr<DataStormContract::PublisherSessionPrx> prx,
                              shared_ptr<DataStormContract::PeerPrx> peer)
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
        return;
    }

    unique_lock<mutex> lock(_mutex);
    session->connected(prx, peer->ice_getCachedConnection(), _instance->getTopicFactoryI()->getTopicReaders());
}

shared_ptr<SessionI>
SubscriberI::makeSessionServant(shared_ptr<DataStormContract::PeerPrx> peer)
{
    return make_shared<SubscriberSessionI>(this, peer);
}

shared_ptr<TopicSubscriber>
SubscriberI::createTopicSubscriber(TopicReaderI* reader)
{
    auto impl = make_shared<TopicSubscriber>(this, reader);
    impl->init();
    return impl;
}