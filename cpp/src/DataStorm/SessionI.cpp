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
#include <DataStorm/SampleI.h>

using namespace std;
using namespace DataStormInternal;
using namespace DataStormContract;

SessionI::SessionI(PeerI* parent, const shared_ptr<PeerPrx>& peer) :
    _instance(parent->getInstance()), _traceLevels(_instance->getTraceLevels()), _parent(parent), _peer(peer)
{
}

void
SessionI::init()
{
    auto prx = _instance->getObjectAdapter()->addWithUUID(shared_from_this());
    _proxy = Ice::uncheckedCast<SessionPrx>(prx);
}

void
SessionI::announceTopics(TopicInfoSeq topics, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "announcing topics `" << topics << "' for peer `" << _peer << "' session";
    }

    TopicInfoAndContentSeq ack;
    for(const auto& info : topics)
    {
        runWithTopic(info.name, [&](auto t)
        {
            t->attach(info.id, this, _session);
            ack.emplace_back(t->getTopicInfoAndContent(getLastId(t->getId())));
        });
    }
    if(!ack.empty())
    {
        _session->attachTopicsAsync(ack);
    }
}

void
SessionI::attachTopics(TopicInfoAndContentSeq topics, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "attaching topics `" << topics << "' for peer `" << _peer << "' session";
    }

    for(const auto& info : topics)
    {
        runWithTopic(info.name, [&](auto t)
        {
            t->attach(info.id, this, _session);
            auto keys = t->attachKeysAndFilters(info.id, info.keys, info.filters, info.lastId, this, _session);
            auto filters = t->getFilterInfoSeq();
            if(!keys.empty() || !filters.empty())
            {
                _session->attachKeysAndFiltersAsync(t->getId(), getLastId(t->getId()), keys, filters);
            }
        });
    }
}

void
SessionI::detachTopic(long long int id, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "detaching topic `" << id << "' for peer `" << _peer << "' session";
    }

    runWithTopic(id, [&](auto topic) { topic.get()->detach(id, this); });
}

void
SessionI::announceKey(long long int id, KeyInfo key, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "announcing key `" << key << '@' << id << "' for peer `" << _peer << "' session";
    }

    runWithTopic(id, [&](auto topic)
    {
        auto t = topic.get();
        auto kAndF = t->attachKey(id, key, 0, this, _session);
        if(!kAndF.first.empty() || !kAndF.second.empty())
        {
            _session->attachKeysAndFiltersAsync(t->getId(), getLastId(t->getId()), kAndF.first, kAndF.second);
        }
    });
}

void
SessionI::announceFilter(long long int id, FilterInfo filter, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "announcing filter `" << filter << '@' << id << "' for peer `" << _peer << "' session";
    }

    runWithTopic(id, [&](auto topic)
    {
        auto t = topic.get();
        auto keys = t->attachFilter(id, filter, 0, this, _session);
        if(!keys.empty())
        {
            _session->attachKeysAndFiltersAsync(t->getId(), getLastId(t->getId()), keys, {});
        }
    });
}

void
SessionI::attachKeysAndFilters(long long int id,
                               long long int lastId,
                               KeyInfoAndSamplesSeq keys,
                               FilterInfoSeq filters,
                               const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "attaching keys and filters `" << keys << ';' << filters << "' for peer `" << _peer << "' session";
    }

    runWithTopic(id, [&](auto topic)
    {
        auto t = topic.get();
        auto samples = t->attachKeysAndFilters(id, keys, filters, lastId, this, _session);
        if(!samples.empty())
        {
            Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_session)->iAsync(t->getId(), samples);
        }
    });
}

void
SessionI::detachKey(long long int id, long long int key, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "detaching key `" << key << "@" << id << "' for peer `" << _peer << "' session";
    }

    runWithTopic(id, [&](auto topic)
    {
        auto k = topic.removeKey(key);
        for(auto subscriber : k.getSubscribers())
        {
            subscriber->detachKey(id, key, this);
        }
    });
}

void
SessionI::detachFilter(long long int id, long long int filter, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "detaching filter `" << filter << "@" << id << "' for peer `" << _peer << "' session";
    }

    runWithTopic(id, [&](auto topic)
    {
        auto f = topic.removeFilter(filter);
        for(auto subscriber : f.getSubscribers())
        {
            subscriber->detachFilter(id, filter, this);
        }
    });
}

void
SessionI::destroy(const Ice::Current&)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(!_session)
        {
            return;
        }

        if(_traceLevels->session > 0)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "destroyed session for peer `" << _peer << "'";
        }

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

        for(const auto& t : _topics)
        {
            t.second.get()->detach(t.first, this, false);
        }
        _topics.clear();
    }

    _parent->removeSession(this);
}

void
SessionI::connected(const shared_ptr<SessionPrx>& session,
                    const shared_ptr<Ice::Connection>& connection,
                    const TopicInfoSeq& topics)
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
        out << "session for peer `" << _peer << "' connected";
    }

    auto prx = connection->createProxy(session->ice_getIdentity())->ice_oneway();
    _session = Ice::uncheckedCast<SessionPrx>(prx);

    if(!topics.empty())
    {
        _session->announceTopicsAsync(topics);
    }
}

void
SessionI::disconnected(exception_ptr ex)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(!_session)
        {
            return;
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
                out << "session for peer `" << _peer << "' disconnected:\n" << e.what();
            }
        }

        _session = nullptr;
        _connection = nullptr;

        for(const auto& t : _topics)
        {
            t.second.get()->detach(t.first, this, false);
        }
        _topics.clear();
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

shared_ptr<SessionPrx>
SessionI::getSession() const
{
    lock_guard<mutex> lock(_mutex);
    return _session;
}

shared_ptr<SessionPrx>
SessionI::getSessionNoLock() const
{
    return _session;
}

long long int
SessionI::getLastId(long long int) const
{
    return -1;
}

void
SessionI::subscribe(long long id, TopicI* topic)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "session for peer `" << _peer << "' subscribed to topic `" << topic->getName() << "'";
    }
    _topics.emplace(id, TopicSubscribers(topic));
}

void
SessionI::unsubscribe(long long id, bool remove)
{
    assert(_topics.find(id) != _topics.end());
    auto& topic = _topics.at(id);
    for(auto k : topic.getKeys())
    {
        for(auto e : k.second.getSubscribers())
        {
            e->detachKey(id, k.first, this, false);
        }
    }
    for(auto f : topic.getFilters())
    {
        for(auto e : f.second.getSubscribers())
        {
            e->detachKey(id, f.first, this, false);
        }
    }
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "session for peer `" << _peer << "' unsubscribed from topic `" << topic.get()->getName() << "'";
    }
    if(remove)
    {
        _topics.erase(id);
    }
}

void
SessionI::disconnect(long long id)
{
    lock_guard<mutex> lock(_mutex); // Called by TopicI::destroy
    if(!_session)
    {
        return;
    }
    unsubscribe(id, true);
}

void
SessionI::subscribeToKey(long long topic, long long int id, const shared_ptr<Key>& key, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& t = _topics.at(topic);
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "session for peer `" << _peer << "' subscribed to key `" << t.get() << "/" << key << "'";
    }
    t.getKey(id, key)->add(element);
}

void
SessionI::unsubscribeFromKey(long long topic, long long int id, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& t = _topics.at(topic);
    auto k = t.getKey(id);
    if(k)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "session for peer `" << _peer << "' unsubscribed from key `" << t.get() << "/" << k->get() << "'";
        }
        k->remove(element);
    }
}

void
SessionI::disconnectFromKey(long long topic, long long int id, DataElementI* element)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if(!_session)
    {
        return;
    }
    unsubscribeFromKey(topic, id, element);
}

void
SessionI::subscribeToFilter(long long topic, long long int id, const shared_ptr<Filter>& filter, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& t = _topics.at(topic);
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "session for peer `" << _peer << "' subscribed to filter `" << t.get() << '/' << filter << "'";
    }
    t.getFilter(id, filter)->add(element);
}

void
SessionI::unsubscribeFromFilter(long long topic, long long int id, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& t = _topics.at(topic);
    auto f = t.getFilter(id);
    if(f)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << "session for peer `" << _peer << "' unsubscribed from filter `" << t.get() << '/' << f->get() << "'";
        }
        f->remove(element);
    }
}

void
SessionI::disconnectFromFilter(long long topic, long long int id, DataElementI* element)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if(!_session)
    {
        return;
    }
    unsubscribeFromFilter(topic, id, element);
}

void
SessionI::runWithTopic(const std::string& name, function<void (const shared_ptr<TopicI>&)> fn)
{
    auto topic = getTopic(name);
    if(topic)
    {
        unique_lock<mutex> l(topic->getMutex());
        _lock = &l;
        fn(topic);
        _lock = nullptr;
    }
}

void
SessionI::runWithTopic(long long int id, function<void (TopicSubscribers&)> fn)
{
    auto t = _topics.find(id);
    if(t != _topics.end())
    {
        unique_lock<mutex> l(t->second.get()->getMutex());
        _lock = &l;
        fn(t->second);
        _lock = nullptr;
    }
}

SubscriberSessionI::SubscriberSessionI(SubscriberI* parent, const shared_ptr<PeerPrx>& peer) :
    SessionI(parent, peer)
{
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "created session for publisher `" << peer << "'";
    }
}

shared_ptr<TopicI>
SubscriberSessionI::getTopic(const string& topic) const
{
    return _instance->getTopicFactoryI()->getTopicReader(topic);
}

void
SubscriberSessionI::i(long long int id, DataSamplesSeq samplesSeq, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    for(const auto& samples : samplesSeq)
    {
        runWithTopic(id, [&](auto topic)
        {
            auto k = topic.getKey(samples.key);
            if(k)
            {
                for(const auto& s : samples.samples)
                {
                    if(setLastId(id, s->id))
                    {
                        auto impl = make_shared<SampleI>(topic.get()->getTopicFactory(), k->get(), s);
                        for(auto subscriber : k->getSubscribers())
                        {
                            subscriber->queue(impl);
                        }
                    }
                }
            }
        });
    }
}

void
SubscriberSessionI::s(long long int id, long long int key, shared_ptr<DataSample> s, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopic(id, [&](auto topic)
    {
        auto k = topic.getKey(key);
        if(k && setLastId(id, s->id))
        {
            auto impl = make_shared<SampleI>(topic.get()->getTopicFactory(), k->get(), s);
            for(auto subscriber : k->getSubscribers())
            {
                subscriber->queue(impl);
            }
        }
    });
}

void
SubscriberSessionI::f(long long int id, long long int filter, shared_ptr<DataSample> s, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopic(id, [&](auto topic)
    {
        auto f = topic.getFilter(filter);
        if(f && setLastId(id, s->id))
        {
            auto impl = make_shared<SampleI>(topic.get()->getTopicFactory(), nullptr, s);
            for(auto subscriber : f->getSubscribers())
            {
                subscriber->queue(impl);
            }
        }
    });
}

long long int
SubscriberSessionI::getLastId(long long int topic) const
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
SubscriberSessionI::setLastId(long long int topic, long long int lastId)
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

PublisherSessionI::PublisherSessionI(PublisherI* parent, const shared_ptr<PeerPrx>& peer) :
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
    return _instance->getTopicFactoryI()->getTopicWriter(topic);
}
