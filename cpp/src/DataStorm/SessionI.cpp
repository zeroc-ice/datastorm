// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/SessionI.h>
#include <DataStorm/SessionManager.h>
#include <DataStorm/PeerI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;

SessionI::SessionI(PeerI* parent, shared_ptr<DataStormContract::PeerPrx> peer) :
    _instance(parent->getInstance()), _traceLevels(_instance->getTraceLevels()), _parent(parent), _peer(peer)
{
}

void
SessionI::init()
{
    auto prx = _instance->getObjectAdapter()->addWithUUID(shared_from_this());
    _proxy = Ice::uncheckedCast<DataStormContract::SessionPrx>(prx);
}

void
SessionI::initTopics(DataStormContract::StringSeq topics, const Ice::Current&)
{
    for(const auto& name : topics)
    {
        {
            lock_guard<mutex> lock(_mutex);
            if(_traceLevels->session > 1)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << "initializing topic `" << name << "' for session `" << _peer << "'";
            }
            _topics.insert(name);
        }

        auto topic = getTopic(name);
        if(topic)
        {
            topic->attach(this);
        }
    }
}

void
SessionI::addTopic(string name, const Ice::Current&)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "adding topic `" << name << "' for session `" << _peer << "'";
        }
        _topics.insert(name);
    }

    auto topic = getTopic(name);
    if(topic)
    {
        topic->attach(this);
    }
}

void
SessionI::removeTopic(string name, const Ice::Current&)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "removing topic `" << name << "' for session `" << _peer << "'";
        }
        _topics.erase(name);
    }

    auto topic = getTopic(name);
    if(topic)
    {
        topic->detach(this);
    }
}

void
SessionI::initKeysAndFilters(string topic,
                             long long int lastId,
                             DataStormContract::KeyInfoSeq keys,
                             DataStormContract::StringSeq filters,
                             const Ice::Current&)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "initializing topic `" << topic << "' keys and filters for session `" << _peer << "'";
    }

    auto t = getTopic(topic);
    if(t)
    {
        t->initKeysAndFilters(keys, filters, lastId, this);
    }
}

void
SessionI::attachKey(string topic, long long int lastId, DataStormContract::KeyInfo key, const Ice::Current&)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "attaching topic `" << topic << "' key for session `" << _peer << "'";
    }

    auto t = getTopic(topic);
    if(t)
    {
        t->attachKey(key, lastId, this);
    }
}

void
SessionI::detachKey(string topic, DataStormContract::Key key, const Ice::Current&)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "detaching topic `" << topic << "' key for session `" << _peer << "'";
    }

    auto t = getTopic(topic);
    if(t)
    {
        t->detachKey(key, this);
    }
}

void
SessionI::attachFilter(string topic, long long int lastId, string filter, const Ice::Current&)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "attaching topic `" << topic << "' filter `" << filter << "' for session `" << _peer << "'";
    }

    auto t = getTopic(topic);
    if(t)
    {
        t->attachFilter(filter, lastId, this);
    }
}

void
SessionI::detachFilter(string topic, string filter, const Ice::Current&)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "detaching topic `" << topic << "' filter `" << filter << "' for session `" << _peer << "'";
    }

    auto t = getTopic(topic);
    if(t)
    {
        t->detachFilter(filter, this);
    }
}

void
SessionI::destroy(const Ice::Current&)
{
    set<string> topics;
    {
        lock_guard<mutex> lock(_mutex);
        if(_traceLevels->session > 0)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "destroyed session for peer `" << _peer << "'";
        }
        _topics.swap(topics);

        _instance->getSessionManager()->remove(this, _connection);

        try
        {
            _instance->getObjectAdapter()->remove(_proxy->ice_getIdentity());
        }
        catch(const Ice::LocalException&)
        {
            // Ignore, OA is deactivated or servant is already destroyed.
        }

        _session = nullptr;
        _connection = nullptr;
    }

    for(const auto& name : topics)
    {
        auto topic = getTopic(name);
        if(topic)
        {
            topic->detach(this);
        }
    }
    _parent->removeSession(this);
}

void
SessionI::connected(shared_ptr<DataStormContract::SessionPrx> session,
                    shared_ptr<Ice::Connection> connection,
                    const DataStormContract::StringSeq& topics)
{
    lock_guard<mutex> lock(_mutex);
    if(_connection)
    {
        return;
    }

    _connection = connection;
    if(!_connection->getAdapter())
    {
        _connection->setAdapter(_instance->getObjectAdapter());
    }
    _instance->getSessionManager()->add(this, connection);

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "session `" << session->ice_getIdentity() << "' for peer `" << _peer << "' connected";
    }

    auto prx = connection->createProxy(session->ice_getIdentity())->ice_oneway();
    _session = Ice::uncheckedCast<DataStormContract::SessionPrx>(prx);
    if(!topics.empty())
    {
        _session->initTopicsAsync(topics);
    }
}

void
SessionI::disconnected(exception_ptr ex)
{
    std::shared_ptr<DataStormContract::SessionPrx> prx;
    set<string> topics;
    {
        lock_guard<mutex> lock(_mutex);
        topics.swap(_topics);
        swap(_session, prx);
        _connection = nullptr;
    }

    for(const auto& name : topics)
    {
        auto topic = getTopic(name);
        if(topic)
        {
            topic->detach(this);
        }
    }

    if(_traceLevels->session > 0)
    {
        try
        {
            rethrow_exception(ex);
        }
        catch(const std::exception& e)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "session `" << prx->ice_getIdentity() << "' for peer `" << _peer << "' disconnected:\n" << e.what();
        }
    }

    //
    // Try re-connecting if we got disconnected.
    //
    // TODO: Improve retry logic.
    //
    if(!_parent->createSession(_peer))
    {
        _proxy->destroy();
    }
}

void
SessionI::checkTopic(shared_ptr<TopicI> topic)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(_topics.find(topic->getName()) == _topics.end())
        {
            return;
        }
    }

    topic->attach(this);
}

shared_ptr<DataStormContract::SessionPrx>
SessionI::getSession() const
{
    lock_guard<mutex> lock(_mutex);
    return _session;
}

long long int
SessionI::getLastId(const string&)
{
    return -1;
}

SubscriberSessionI::SubscriberSessionI(SubscriberI* parent, shared_ptr<DataStormContract::PeerPrx> peer) :
    SessionI(parent, peer)
{
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "created session for subscriber `" << peer << "'";
    }
}

shared_ptr<TopicI>
SubscriberSessionI::getTopic(const string& topic) const
{
    return _instance->getTopicFactory()->getTopicReader(topic);
}

void
SubscriberSessionI::i(string topic, DataStormContract::DataSamplesSeq samples, const Ice::Current&)
{
    auto topicReader = _instance->getTopicFactory()->getTopicReader(topic);
    if(topicReader)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "initializating topic `" << topic << "' samples for session `" << _peer << "'";
        }

        for(const auto& sample : samples)
        {
            topicReader->queue(sample->key, sample->samples, this);
        }
    }
}

void
SubscriberSessionI::s(string topic, DataStormContract::Key key, shared_ptr<DataStormContract::DataSample> s,
                      const Ice::Current&)
{
    auto topicReader = _instance->getTopicFactory()->getTopicReader(topic);
    if(topicReader)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "queueing topic `" << topic << "' sample for session `" << _peer << "'";
        }
        topicReader->queue(key, s, this);
    }
}

void
SubscriberSessionI::f(string topic, string filter, shared_ptr<DataStormContract::DataSample> s, const Ice::Current&)
{
    auto topicReader = _instance->getTopicFactory()->getTopicReader(topic);
    if(topicReader)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "queueing topic `" << topic << "' sample for session `" << _peer << "'";
        }
        topicReader->queue(filter, s, this);
    }
}

long long int
SubscriberSessionI::getLastId(const string& topic)
{
    // Called within the topic synchronization
    auto p = _lastIds.find(topic);
    if(p != _lastIds.end())
    {
        return p->second;
    }
    return -1;
}

bool
SubscriberSessionI::setLastId(const string& topic, long long int lastId)
{
    // Called within the topic synchronization
    auto p = _lastIds.find(topic);
    if(p == _lastIds.end())
    {
        _lastIds.insert(make_pair(topic, lastId));
        return true;
    }
    else if(p->second >= lastId)
    {
        return false;
    }
    p->second = lastId;
    return true;
}

PublisherSessionI::PublisherSessionI(PublisherI* parent, shared_ptr<DataStormContract::PeerPrx> peer) :
    SessionI(parent, peer)
{
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "created session for subscriber `" << peer << "'";
    }
}

shared_ptr<TopicI>
PublisherSessionI::getTopic(const string& topic) const
{
    return _instance->getTopicFactory()->getTopicWriter(topic);
}
