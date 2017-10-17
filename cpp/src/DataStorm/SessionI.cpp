// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/SessionI.h>
#include <DataStorm/SessionManager.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;
using namespace DataStormContract;

SessionI::SessionI(NodeI* parent, const shared_ptr<NodePrx>& node) :
    _instance(parent->getInstance()), _traceLevels(_instance->getTraceLevels()), _parent(parent), _node(node)
{
}

void
SessionI::init(const shared_ptr<SessionPrx>& prx)
{
    _proxy = prx;
    _id = prx->ice_getIdentity().category + "/" + prx->ice_getIdentity().name;
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": created session (peer = `" << _node << "')";
    }
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
        out << _id << ": announcing topics `" << topics << "'";
    }

    TopicInfoAndContentSeq ack;
    for(const auto& info : topics)
    {
        runWithTopics(info.name, [&](auto topic)
        {
            topic->attach(info.id, this, _session);
            ack.emplace_back(topic->getTopicInfoAndContent(getLastId(info.id)));
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
        out << _id << ": attaching topics `" << topics << "'";
    }

    for(const auto& info : topics)
    {
        runWithTopics(info.name, [&](auto topic)
        {
            topic->attach(info.id, this, _session);
            auto keys = topic->attachKeysAndFilters(info.id, info.keys, info.filters, info.lastId, this, _session);
            auto filters = topic->getFilterInfoSeq();
            if(!keys.empty() || !filters.empty())
            {
                _session->attachKeysAndFiltersAsync(topic->getId(), getLastId(info.id), keys, filters);
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
        out << _id << ": detaching topic `" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto& subscriber) { topic->detach(id, this); });

    // Erase the topic
    _topics.erase(id);
}

void
SessionI::announceKeys(long long int id, KeyInfoSeq keys, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": announcing key `[" << keys << "]@" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto& subscriber, auto& subscribers)
    {
        auto kAndF = topic->attachKeys(id, keys, 0, this, _session);
        if(!kAndF.first.empty() || !kAndF.second.empty())
        {
            _session->attachKeysAndFiltersAsync(topic->getId(), subscribers.getLastId(), kAndF.first, kAndF.second);
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
        out << _id <<  ": announcing filter `" << filter << '@' << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto& subscriber, auto& subscribers)
    {
        auto keys = topic->attachFilter(id, filter, 0, this, _session);
        if(!keys.empty())
        {
            _session->attachKeysAndFiltersAsync(topic->getId(), subscribers.getLastId(), keys, {});
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
        out << _id << ": attaching keys and filters `([" << keys << "];[" << filters << "])@" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto& subscriber)
    {
        auto samples = topic->attachKeysAndFilters(id, keys, filters, lastId, this, _session);
        for(const auto& kis : keys)
        {
            auto k = subscriber.keys.get(kis.info.id);
            if(k)
            {
                if(_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": initializing samples for key `" << k->get() << "'";
                }
                vector<shared_ptr<Sample>> samplesI;
                samplesI.reserve(kis.samples.size());
                for(auto& s : kis.samples)
                {
                    samplesI.push_back(topic->getSampleFactory()(s.id, s.type, k->get(), move(s.value), s.timestamp));
                }
                for(auto& ks : k->getSubscribers())
                {
                    if(!ks.second.initialized && kis.subscriberId == ks.first->getSubscriberId())
                    {
                        ks.second.initialized = true;
                        ks.first->initSamples(samplesI);
                    }
                }
            }
        }
        Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_session)->iAsync(topic->getId(), samples);
    });
}

void
SessionI::detachKeys(long long int id, LongSeq keys, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 2)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": detaching key `[" << keys << "]@" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto& subscriber)
    {
        for(auto key : keys)
        {
            auto k = subscriber.keys.remove(key);
            for(auto& ks : k.getSubscribers())
            {
                ks.first->detachKey(id, key, this, ks.second.facet);
            }
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
        out << _id << ": detaching filter `" << filter << "@" << id << "'";
    }

    runWithTopics(id, [&](auto topic, auto& subscriber)
    {
        auto f = subscriber.filters.remove(filter);
        for(auto& fs : f.getSubscribers())
        {
            fs.first->detachFilter(id, filter, this, fs.second.facet);
        }
    });
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

    auto prx = connection->createProxy(session->ice_getIdentity())->ice_oneway();
    _session = Ice::uncheckedCast<SessionPrx>(prx);

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": session `" << _session->ice_getIdentity() << "' connected\n" << connection->toString();
    }

    if(!topics.empty())
    {
        _session->announceTopicsAsync(topics);
    }

    for(auto c : _connectedCallbacks)
    {
        c(_proxy);
    }
    _connectedCallbacks.clear();
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
                out << _id << ": session `" << _session->ice_getIdentity() << "' disconnected:\n";
                out << _connection->toString() << "\n";
                out << e.what();
            }
        }

        for(auto& t : _topics)
        {
            runWithTopics(t.first, [&](auto topic, auto& subscriber) { topic->detach(t.first, this); });

            // Clear the subscribers but don't remove the topic in order to preserve lastId.
            t.second.clearSubscribers();
        }

        _session = nullptr;
        _connection = nullptr;
    }

    //
    // Try re-connecting if we got disconnected.
    //
    // TODO: Improve retry logic.
    //
    if(!reconnect())
    {
        _proxy->destroy();
    }
}

void
SessionI::destroyImpl()
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": destroyed session";
    }

    _instance->getSessionManager()->remove(this, _connection);

    _session = nullptr;
    _connection = nullptr;

    for(const auto& t : _topics)
    {
        runWithTopics(t.first, [&](auto topic, auto& subscriber) { topic->detach(t.first, this); });
    }

    for(auto c : _connectedCallbacks)
    {
        c(nullptr);
    }
    _connectedCallbacks.clear();
}

void
SessionI::addConnectedCallback(function<void(shared_ptr<SessionPrx>)> callback)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(!_session)
        {
            _connectedCallbacks.push_back(callback);
            return;
        }
    }
    callback(_proxy);
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
SessionI::getLastId(long long int topic) const
{
    // Called within the topic synchronization
    auto p = _topics.find(topic);
    if(p != _topics.end())
    {
        return p->second.getLastId();
    }
    return -1;
}

void
SessionI::subscribe(long long id, TopicI* topic)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed topic `" << id << "' to topic `" << topic << "'";
    }
    _topics[id].addSubscriber(topic);
}

void
SessionI::unsubscribe(long long id, TopicI* topic)
{
    assert(_topics.find(id) != _topics.end());
    auto& subscriber = _topics.at(id).getSubscriber(topic);
    for(auto& k : subscriber.keys.getAll())
    {
        for(auto& e : k.second.getSubscribers())
        {
            e.first->detachKey(id, k.first, this, e.second.facet, false);
        }
    }
    for(auto& f : subscriber.filters.getAll())
    {
        for(auto& e : f.second.getSubscribers())
        {
            e.first->detachKey(id, f.first, this, e.second.facet, false);
        }
    }
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": unsubscribed topic `" << id << "' from topic `" << topic << "'";
    }
}

void
SessionI::disconnect(long long id, TopicI* topic)
{
    lock_guard<mutex> lock(_mutex); // Called by TopicI::destroy
    if(!_session)
    {
        return;
    }

    if(_topics.find(id) == _topics.end())
    {
        return; // Peer topic detached first.
    }

    runWithTopic(id, topic, [&](auto&) { unsubscribe(id, topic); });

    auto& subscriber = _topics.at(id);
    subscriber.removeSubscriber(topic);
    if(subscriber.getSubscribers().empty())
    {
        _topics.erase(id);
    }
}

void
SessionI::subscribeToKey(long long topic, long long int id, const shared_ptr<Key>& key, DataElementI* element,
                         const string& facet)
{
    assert(_topics.find(topic) != _topics.end());
    auto& subscriber = _topics.at(topic).getSubscriber(element->getTopic());
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed key `" << id << '@' << topic << "' to `" << element << "'";
        if(!facet.empty())
        {
            out << " (facet=" << facet << ')';
        }
    }
    subscriber.keys.get(id, key)->add(element, facet);
}

void
SessionI::unsubscribeFromKey(long long topic, long long int id, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& subscriber = _topics.at(topic).getSubscriber(element->getTopic());
    auto k = subscriber.keys.get(id);
    if(k)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed key `" << id << '@' << topic << "' from `" << element << "'";
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

    runWithTopic(topic, element->getTopic(), [&](auto&) { unsubscribeFromKey(topic, id, element); });
}

void
SessionI::subscribeToFilter(long long topic, long long int id, const shared_ptr<Filter>& filter, DataElementI* element,
                            const string& facet)
{
    assert(_topics.find(topic) != _topics.end());
    auto& subscriber = _topics.at(topic).getSubscriber(element->getTopic());
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed filter `" << id << '@' << topic << "' to `" << element << "'";
        if(!facet.empty())
        {
            out << " (facet=" << facet << ')';
        }
    }
    subscriber.filters.get(id, filter)->add(element, facet);
}

void
SessionI::unsubscribeFromFilter(long long topic, long long int id, DataElementI* element)
{
    assert(_topics.find(topic) != _topics.end());
    auto& subscriber = _topics.at(topic).getSubscriber(element->getTopic());
    auto f = subscriber.filters.get(id);
    if(f)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed filter `" << id << '@' << topic << "' from `" << element << "'";
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

    runWithTopic(topic, element->getTopic(), [&](auto&) { unsubscribeFromFilter(topic, id, element); });
}

void
SessionI::runWithTopics(const std::string& name, function<void (const shared_ptr<TopicI>&)> fn)
{
    auto topics = getTopics(name);
    for(auto topic : topics)
    {
        unique_lock<mutex> l(topic->getMutex());
        _topicLock = &l;
        fn(topic);
        _topicLock = nullptr;
    }
}

void
SessionI::runWithTopics(long long int id, function<void (TopicI*, TopicSubscriber&)> fn)
{
    auto t = _topics.find(id);
    if(t != _topics.end())
    {
        for(auto& s : t->second.getSubscribers())
        {
            unique_lock<mutex> l(s.first->getMutex());
            _topicLock = &l;
            fn(s.first, s.second);
            _topicLock = nullptr;
        }
    }
}

void
SessionI::runWithTopics(long long int id, function<void (TopicI*, TopicSubscriber&, TopicSubscribers&)> fn)
{
    auto t = _topics.find(id);
    if(t != _topics.end())
    {
        for(auto& s : t->second.getSubscribers())
        {
            unique_lock<mutex> l(s.first->getMutex());
            _topicLock = &l;
            fn(s.first, s.second, t->second);
            _topicLock = nullptr;
        }
    }
}

void
SessionI::runWithTopic(long long int id, TopicI* topic, function<void (TopicSubscriber&)> fn)
{
    auto t = _topics.find(id);
    if(t != _topics.end())
    {
        auto p = t->second.getSubscribers().find(topic);
        if(p != t->second.getSubscribers().end())
        {
            unique_lock<mutex> l(topic->getMutex());
            _topicLock = &l;
            fn(p->second);
            _topicLock = nullptr;
        }
    }
}

SubscriberSessionI::SubscriberSessionI(NodeI* parent, const shared_ptr<NodePrx>& node) :
    SessionI(parent, node)
{
}

void
SubscriberSessionI::destroy(const Ice::Current&)
{
    _parent->removeSubscriberSession(this);
}

vector<shared_ptr<TopicI>>
SubscriberSessionI::getTopics(const string& name) const
{
    return _instance->getTopicFactory()->getTopicReaders(name);
}

void
SubscriberSessionI::i(long long int id, DataSamplesSeq samplesSeq, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    auto communicator = _instance->getCommunicator();
    for(const auto& samples : samplesSeq)
    {
        runWithTopics(id, [&](auto topic, auto& subscriber)
        {
            auto k = subscriber.keys.get(samples.key);
            if(k)
            {
                if(_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": initializing samples for key `" << k->get() << "'";
                }
                vector<shared_ptr<Sample>> samplesI;
                samplesI.reserve(samples.samples.size());
                for(auto& s : samples.samples)
                {
                    samplesI.push_back(topic->getSampleFactory()(s.id, s.type, k->get(), move(s.value), s.timestamp));
                }
                for(auto& ks : k->getSubscribers())
                {
                    if(!ks.second.initialized && samples.subscriberId == ks.first->getSubscriberId())
                    {
                        ks.second.initialized = true;
                        ks.first->initSamples(samplesI);
                    }
                }
            }
        });
    }
}

void
SubscriberSessionI::s(long long int id, long long int key, DataSample s, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(id, [&](auto topic, auto& subscriber, auto topicSubscribers)
    {
        auto k = subscriber.keys.get(key);
        if(k && topicSubscribers.setLastId(s.id))
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": queuing sample `" << s.id << "' for key `" << k->get() << "'";
                if(!current.facet.empty())
                {
                    out << " (facet = " << current.facet << ')';
                }
            }

            auto impl = topic->getSampleFactory()(s.id, s.type, k->get(), move(s.value), s.timestamp);
            for(auto& ks : k->getSubscribers())
            {
                if(ks.second.initialized && current.facet == ks.first->getFacet())
                {
                    ks.first->queue(impl);
                }
            }
        }
    });
}

void
SubscriberSessionI::f(long long int id, long long int filter, DataSample s, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(id, [&](auto topic, auto& subscriber, auto topicSubscribers)
    {
        auto f = subscriber.filters.get(filter);
        if(f && topicSubscribers.setLastId(s.id))
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": queuing sample `" << s.id << "' for filter `" << f->get() << "'";
                if(!current.facet.empty())
                {
                    out << " (facet = " << current.facet << ')';
                }
            }

            auto impl = topic->getSampleFactory()(s.id, s.type, nullptr, move(s.value), s.timestamp);
            if(f->get()->hasReaderMatch())
            {
                impl->decode(_instance->getCommunicator());
                if(!f->get()->readerMatch(impl))
                {
                    return;
                }
            }

            for(auto& fs : f->getSubscribers())
            {
                if(fs.second.initialized && current.facet == fs.first->getFacet())
                {
                    fs.first->queue(impl);
                }
            }
        }
    });
}

bool
SubscriberSessionI::reconnect() const
{
    return _parent->createPublisherSession(_node);
}

PublisherSessionI::PublisherSessionI(NodeI* parent, const shared_ptr<NodePrx>& node) :
    SessionI(parent, node)
{
}

void
PublisherSessionI::destroy(const Ice::Current&)
{
    _parent->removePublisherSession(this);
}

vector<shared_ptr<TopicI>>
PublisherSessionI::getTopics(const string& name) const
{
    return _instance->getTopicFactory()->getTopicWriters(name);
}

bool
PublisherSessionI::reconnect() const
{
    return _parent->createSubscriberSession(_node);
}
