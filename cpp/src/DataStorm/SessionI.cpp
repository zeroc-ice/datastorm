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

SessionI::SessionI(const std::shared_ptr<NodeI>& parent, const shared_ptr<NodePrx>& node) :
    _instance(parent->getInstance()),
    _traceLevels(_instance->getTraceLevels()),
    _parent(parent),
    _node(node),
    _destroyed(false),
    _sessionInstanceId(0)
{
}

void
SessionI::init(const shared_ptr<SessionPrx>& prx)
{
    assert(_node);
    _proxy = prx;
    _id = Ice::identityToString(prx->ice_getIdentity());
    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": created session (peer = `" << _node << "')";
    }
}

void
SessionI::announceTopics(TopicInfoSeq topics, bool initialize, const Ice::Current& current)
{
    //
    // Retain topics outside the synchronization. This is necessary to ensure the topic destructor
    // doesn't get called within the synchronization. The topic destructor can callback on the
    // session to disconnect.
    //
    vector<shared_ptr<TopicI>> retained;
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

        for(const auto& info : topics)
        {
            runWithTopics(info.name, retained, [&](const shared_ptr<TopicI>& topic)
            {
                for(auto id : info.ids)
                {
                    topic->attach(id, shared_from_this(), _session);
                }

                _session->attachTopicAsync(topic->getTopicSpec());
            });
        }

        // Reap un-visited topics
        auto p = _topics.begin();
        while(p != _topics.end())
        {
            if(p->second.reap(_sessionInstanceId))
            {
                _topics.erase(p++);
            }
            else
            {
                ++p;
            }
        }
    }
}

void
SessionI::attachTopic(TopicSpec spec, const Ice::Current& current)
{
    //
    // Retain topics outside the synchronization. This is necessary to ensure the topic destructor
    // doesn't get called within the synchronization. The topic destructor can callback on the
    // session to disconnect.
    //
    vector<shared_ptr<TopicI>> retained;
    {
        lock_guard<mutex> lock(_mutex);
        if(!_session)
        {
            return;
        }

        runWithTopics(spec.name, retained, [&](const shared_ptr<TopicI>& topic)
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": attaching topic `" << spec << "' to `" << topic.get() << "'";
            }

            topic->attach(spec.id, shared_from_this(), _session);

            if(!spec.tags.empty())
            {
                auto& subscriber = _topics.at(spec.id).getSubscriber(topic.get());
                for(const auto& tag : spec.tags)
                {
                    subscriber.tags[tag.id] = topic->getTagFactory()->decode(_instance->getCommunicator(), tag.value);
                }
            }

            auto tags = topic->getTags();
            if(!tags.empty())
            {
                _session->attachTagsAsync(topic->getId(), tags, true);
            }

            auto specs = topic->getElementSpecs(spec.id, spec.elements, shared_from_this());
            if(!specs.empty())
            {
                if(_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": matched elements `" << spec << "' on `" << topic.get() << "'";
                }
                _session->attachElementsAsync(topic->getId(), specs, true);
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

    runWithTopics(id, [&](TopicI* topic, TopicSubscriber& subscriber)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": detaching topic `" << id << "' from `" << topic << "'";
        }
        topic->detach(id, shared_from_this());
    });

    // Erase the topic
    _topics.erase(id);
}

void
SessionI::attachTags(long long int topicId, ElementInfoSeq tags, bool initialize, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(topicId, [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers& subscribers)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": attaching tags `[" << tags << "]@" << topicId << "' on topic `" << topic << "'";
        }

        if(initialize)
        {
            subscriber.tags.clear();
        }
        for(const auto& tag : tags)
        {
            subscriber.tags[tag.id] = topic->getTagFactory()->decode(_instance->getCommunicator(), tag.value);
        }
    });
}

void
SessionI::detachTags(long long int topicId, LongSeq tags, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(topicId, [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers& subscribers)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": detaching tags `[" << tags << "]@" << topicId << "' on topic `" << topic << "'";
        }

        for(auto tag : tags)
        {
            subscriber.tags.erase(tag);
        }
    });
}

void
SessionI::announceElements(long long int topicId, ElementInfoSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(topicId, [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers& subscribers)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": announcing elements `[" << elements << "]@" << topicId << "' on topic `" << topic << "'";
        }

        auto specs = topic->getElementSpecs(topicId, elements, shared_from_this());
        if(!specs.empty())
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": announcing elements matched `[" << specs << "]@" << topicId << "' on topic `" << topic << "'";
            }
            _session->attachElementsAsync(topic->getId(), specs, false);
        }
    });
}

void
SessionI::attachElements(long long int id, ElementSpecSeq elements, bool initialize, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    auto now = chrono::system_clock::now();
    runWithTopics(id, [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers& subscribers)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": attaching elements `[" << elements << "]@" << id << "' on topic `" << topic << "'";
            if(initialize)
            {
                out << " (initializing)";
            }
        }

        auto specAck = topic->attachElements(id, elements, shared_from_this(), _session, now);

        if(initialize)
        {
            // Reap unused keys and filters from the topic subscriber
            subscriber.reap(_sessionInstanceId);

            // TODO: reap keys / filters
        }

        if(!specAck.empty())
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": attaching elements matched `[" << specAck << "]@" << id << "' on topic `" << topic << "'";
            }
            _session->attachElementsAckAsync(topic->getId(), specAck);
        }
    });
}

void
SessionI::attachElementsAck(long long int id, ElementSpecAckSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }
    auto now = chrono::system_clock::now();
    runWithTopics(id, [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers& subscribers)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": attaching elements ack `[" << elements << "]@" << id << "' on topic `" << topic << "'";
        }

        auto samples = topic->attachElementsAck(id, elements, shared_from_this(), _session, now);
        if(!samples.empty())
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": initializing elements `[" << samples << "]@" << id << "' on topic `" << topic << "'";
            }
            _session->initSamplesAsync(topic->getId(), samples);
        }
    });
}

void
SessionI::detachElements(long long int id, LongSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(id, [&](TopicI* topic, TopicSubscriber& subscriber)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": detaching elements `[" << elements << "]@" << id << "' on topic `" << topic << "'";
        }

        for(auto e : elements)
        {
            auto k = subscriber.remove(e);
            for(auto& s : k.getSubscribers())
            {
                for(auto key : s.second.keys)
                {
                    if(e > 0)
                    {
                        s.first->detachKey(id, e, key, shared_from_this(), s.second.facet, true);
                    }
                    else
                    {
                        s.first->detachFilter(id, -e, key, shared_from_this(), s.second.facet, true);
                    }
                }
            }
        }
    });
}

void
SessionI::initSamples(long long int topicId, DataSamplesSeq samplesSeq, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    auto now = chrono::system_clock::now();
    auto communicator = _instance->getCommunicator();
    for(const auto& samples : samplesSeq)
    {
        runWithTopics(topicId, [&](TopicI* topic, TopicSubscriber& subscriber)
        {
            auto k = subscriber.get(samples.id);
            if(k)
            {
                if(_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": initializing samples from `" << samples.id << "'" << " on [";
                    for(auto q = k->getSubscribers().begin(); q != k->getSubscribers().end(); ++q)
                    {
                        if(q != k->getSubscribers().begin())
                        {
                            out << ", ";
                        }
                        out << q->first;
                        if(!q->second.facet.empty())
                        {
                            out << ":" << q->second.facet;
                        }
                    }
                    out << "]";
                }

                vector<shared_ptr<Sample>> samplesI;
                samplesI.reserve(samples.samples.size());
                auto sampleFactory = topic->getSampleFactory();
                for(auto& s : samples.samples)
                {
                    shared_ptr<Key> key;
                    if(s.keyValue.empty())
                    {
                        key = subscriber.keys[s.keyId].first;
                    }
                    else
                    {
                        key = topic->getKeyFactory()->decode(_instance->getCommunicator(), s.keyValue);
                    }

                    samplesI.push_back(sampleFactory->create(_id,
                                                             topicId,
                                                             samples.id,
                                                             s.id,
                                                             s.event,
                                                             key,
                                                             subscriber.tags[s.tag],
                                                             s.value,
                                                             s.timestamp));
                }
                for(auto& ks : k->getSubscribers())
                {
                    if(!ks.second.initialized)
                    {
                        ks.second.initialized = true;
                        ks.second.lastId = samplesI.empty() ? 0 : samplesI.back()->id;
                        ks.first->initSamples(samplesI, topicId, samples.id, k->priority, now, samples.id < 0);
                    }
                }
            }
        });
    }
}

void
SessionI::connected(const shared_ptr<SessionPrx>& session,
                    const shared_ptr<Ice::Connection>& connection,
                    const TopicInfoSeq& topics)
{
    lock_guard<mutex> lock(_mutex);
    if(_destroyed || _session)
    {
        assert(_connectedCallbacks.empty());
        return;
    }

    shared_ptr<Ice::ObjectPrx> prx;
    if(connection)
    {
        if(!connection->getAdapter())
        {
            connection->setAdapter(_instance->getObjectAdapter());
        }
        _instance->getSessionManager()->add(shared_from_this(), connection);
        prx = connection->createProxy(session->ice_getIdentity());
    }
    else
    {
        prx = _instance->getObjectAdapter()->createProxy(session->ice_getIdentity());
    }
    _connection = connection;
    _session = Ice::uncheckedCast<SessionPrx>(prx->ice_oneway());
    ++_sessionInstanceId;

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": session `" << _session->ice_getIdentity() << "' connected\n";
        if(_connection)
        {
            out << _connection->toString();
        }
    }

    if(!topics.empty())
    {
        try
        {
            _session->announceTopicsAsync(topics, true);
        }
        catch(const Ice::LocalException&)
        {
            // Ignore
        }
    }

    for(auto c : _connectedCallbacks)
    {
        c(_proxy);
    }
    _connectedCallbacks.clear();
}

void
SessionI::disconnected(const shared_ptr<Ice::Connection>& connection, exception_ptr ex)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(_destroyed || (connection && _connection != connection) || !_session)
        {
            return;
        }

        if(_traceLevels->session > 0)
        {
            try
            {
                if(ex)
                {
                    rethrow_exception(ex);
                }
                else
                {
                    throw Ice::CloseConnectionException(__FILE__, __LINE__);
                }
            }
            catch(const std::exception& e)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": session `" << _session->ice_getIdentity() << "' disconnected:\n";
                out << (_connection ? _connection->toString() : "<no connection>") << "\n";
                out << e.what();
            }
        }

        for(auto& t : _topics)
        {
            runWithTopics(t.first, [&](TopicI* topic, TopicSubscriber& subscriber)
            {
                topic->detach(t.first, shared_from_this());
            });
        }

        _session = nullptr;
        _connection = nullptr;
    }

    //
    // Try re-connecting if we got disconnected.
    //
    // TODO: Improve retry logic.
    //
    if(connection && !reconnect())
    {
        _proxy->destroy();
    }
}

void
SessionI::destroyImpl(const exception_ptr& ex)
{
    lock_guard<mutex> lock(_mutex);
    assert(!_destroyed);
    _destroyed = true;

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": destroyed session";
        if(ex)
        {
            try
            {
                rethrow_exception(ex);
            }
            catch(const exception& e)
            {
                out << "\n:" << e.what();
            }
            catch(...)
            {
                out << "\n: unexpected exception";
            }
        }
    }

    if(_session)
    {
        if(_connection)
        {
            _instance->getSessionManager()->remove(shared_from_this(), _connection);
        }

        _session = nullptr;
        _connection = nullptr;

        for(const auto& t : _topics)
        {
            runWithTopics(t.first, [&](TopicI* topic, TopicSubscriber& subscriber)
                          {
                              topic->detach(t.first, shared_from_this());
                          });
        }
        _topics.clear();
    }

    for(auto c : _connectedCallbacks)
    {
        c(nullptr);
    }
    _connectedCallbacks.clear();
}

void
SessionI::addConnectedCallback(function<void(shared_ptr<SessionPrx>)> callback,
                               const shared_ptr<Ice::Connection>& connection)
{
    {
        lock_guard<mutex> lock(_mutex);
        assert(!_destroyed);
        if(!_session || connection != _connection)
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

void
SessionI::subscribe(long long id, TopicI* topic)
{
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed topic `" << id << "' to topic `" << topic << "'";
    }
    _topics[id].addSubscriber(topic, _sessionInstanceId);
}

void
SessionI::unsubscribe(long long id, TopicI* topic)
{
    assert(_topics.find(id) != _topics.end());
    auto& subscriber = _topics.at(id).getSubscriber(topic);
    for(auto& k : subscriber.getAll())
    {
        for(auto& e : k.second.getSubscribers())
        {
            for(auto key : e.second.keys)
            {
                if(k.first > 0)
                {
                    e.first->detachKey(id, k.first, key, shared_from_this(), e.second.facet, false);
                }
                else
                {
                    e.first->detachFilter(id, -k.first, key, shared_from_this(), e.second.facet, false);
                }
            }
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

    runWithTopic(id, topic, [&](TopicSubscriber&) { this->unsubscribe(id, topic); });

    auto& subscriber = _topics.at(id);
    subscriber.removeSubscriber(topic);
    if(subscriber.getSubscribers().empty())
    {
        _topics.erase(id);
    }
}

void
SessionI::subscribeToKey(long long topicId, long long int elementId, const std::shared_ptr<DataElementI>& element,
                         const string& facet, const shared_ptr<Key>& key, long long int keyId, int priority)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed element `e" << elementId << '@' << topicId << "' to `" << element << "'";
        if(!facet.empty())
        {
            out << " (facet=" << facet << ')';
        }
    }

    subscriber.add(elementId, priority)->addSubscriber(element, key, facet, _sessionInstanceId);

    auto& p = subscriber.keys[keyId];
    if(!p.first)
    {
        p.first = key;
    }
    p.second.insert(elementId);
}

void
SessionI::unsubscribeFromKey(long long topicId, long long int elementId, const std::shared_ptr<DataElementI>& element,
                             long long int keyId)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    auto k = subscriber.get(elementId);
    if(k)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed element `e" << elementId << '@' << topicId << "' from `" << element << "'";
        }
        k->removeSubscriber(element);
    }

    auto& p = subscriber.keys[keyId];
    p.second.erase(elementId);
    if(p.second.empty())
    {
        subscriber.keys.erase(keyId);
    }
}

void
SessionI::disconnectFromKey(long long topicId, long long int elementId, const std::shared_ptr<DataElementI>& element,
                            long long int keyId)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if(!_session)
    {
        return;
    }

    runWithTopic(topicId, element->getTopic(),
                 [&](TopicSubscriber&) { this->unsubscribeFromKey(topicId, elementId, element, keyId); });
}

void
SessionI::subscribeToFilter(long long topicId, long long int elementId, const std::shared_ptr<DataElementI>& element,
                            const string& facet, const shared_ptr<Key>& key, int priority)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed element `e" << elementId << '@' << topicId << "' to `" << element << "'";
        if(!facet.empty())
        {
            out << " (facet=" << facet << ')';
        }
    }
    subscriber.add(-elementId, priority)->addSubscriber(element, key, facet, _sessionInstanceId);
}

void
SessionI::unsubscribeFromFilter(long long topicId, long long int elementId, const std::shared_ptr<DataElementI>& element,
                                long long int filterId)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    auto f = subscriber.get(-elementId);
    if(f)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed element `e" << elementId << '@' << topicId << "' from `" << element << "'";
        }
        f->removeSubscriber(element);
    }
}

void
SessionI::disconnectFromFilter(long long topicId, long long int elementId, const std::shared_ptr<DataElementI>& element,
                               long long int filterId)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if(!_session)
    {
        return;
    }

    runWithTopic(topicId, element->getTopic(),
                 [&](TopicSubscriber&) { this->unsubscribeFromFilter(topicId, elementId, element, filterId); });
}

LongLongDict
SessionI::getLastIds(long long topicId, long long int keyId, const std::shared_ptr<DataElementI>& element)
{
    LongLongDict lastIds;
    auto p = _topics.find(topicId);
    if(p != _topics.end())
    {
        auto& subscriber = p->second.getSubscriber(element->getTopic());
        for(auto id : subscriber.keys[keyId].second)
        {
            lastIds.emplace(id, subscriber.get(id)->getSubscriber(element)->lastId);
        }
    }
    return lastIds;
}

vector<shared_ptr<Sample>>
SessionI::subscriberInitialized(long long int topicId,
                                long long int elementId,
                                const DataSampleSeq& samples,
                                const shared_ptr<Key>& key,
                                const std::shared_ptr<DataElementI>& element)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    auto s = subscriber.get(elementId)->getSubscriber(element);
    assert(s);

    if(_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": initialized `" << element << "' from `e" << elementId << '@' << topicId << "'";
    }
    s->initialized = true;
    s->lastId = samples.empty() ? 0 : samples.back().id;

    vector<shared_ptr<Sample>> samplesI;
    samplesI.reserve(samples.size());
    auto sampleFactory = element->getTopic()->getSampleFactory();
    auto keyFactory = element->getTopic()->getKeyFactory();
    for(auto& s : samples)
    {
        assert(!key || key == subscriber.keys[s.keyId].first);

        samplesI.push_back(sampleFactory->create(_id,
                                                 topicId,
                                                 elementId,
                                                 s.id,
                                                 s.event,
                                                 key ? key : keyFactory->decode(_instance->getCommunicator(), s.keyValue),
                                                 subscriber.tags[s.tag],
                                                 s.value,
                                                 s.timestamp));
    }
    return samplesI;
}

void
SessionI::runWithTopics(const std::string& name,
                        vector<shared_ptr<TopicI>>& retained,
                        function<void (const shared_ptr<TopicI>&)> fn)
{
    auto topics = getTopics(name);
    for(auto topic : topics)
    {
        retained.push_back(topic);
        unique_lock<mutex> l(topic->getMutex());
        if(topic->isDestroyed())
        {
            continue;
        }
        _topicLock = &l;
        fn(topic);
        _topicLock = nullptr;
        l.unlock();
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
            if(s.first->isDestroyed())
            {
                continue;
            }
            _topicLock = &l;
            fn(s.first, s.second);
            _topicLock = nullptr;
            l.unlock();
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
            if(s.first->isDestroyed())
            {
                continue;
            }
            unique_lock<mutex> l(s.first->getMutex());
            _topicLock = &l;
            fn(s.first, s.second, t->second);
            _topicLock = nullptr;
            l.unlock();
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
            if(topic->isDestroyed())
            {
                return;
            }
            _topicLock = &l;
            fn(p->second);
            _topicLock = nullptr;
            l.unlock();
        }
    }
}

SubscriberSessionI::SubscriberSessionI(const std::shared_ptr<NodeI>& parent, const shared_ptr<NodePrx>& node) :
    SessionI(parent, node)
{
}

void
SubscriberSessionI::destroy(const Ice::Current&)
{
    _parent->removeSubscriberSession(dynamic_pointer_cast<SubscriberSessionI>(shared_from_this()), nullptr);
}

vector<shared_ptr<TopicI>>
SubscriberSessionI::getTopics(const string& name) const
{
    return _instance->getTopicFactory()->getTopicReaders(name);
}

void
SubscriberSessionI::s(long long int topicId, long long int elementId, DataSample s, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session || current.con != _connection)
    {
        if(current.con != _connection)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": discarding sample `" << s.id << "' from `" << elementId << '@' << topicId << "'";
            out << current.con->toString() << " " << (_connection ? _connection->toString() : "null");
        }
        return;
    }
    auto now = chrono::system_clock::now();
    runWithTopics(topicId, [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers& topicSubscribers)
    {
        auto e = subscriber.get(elementId);
        if(e)
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": queuing sample `" << s.id << "' from `" << elementId << '@' << topicId << "'";
                if(!current.facet.empty())
                {
                    out << " facet=" << current.facet;
                }
                out << " to [";
                for(auto q = e->getSubscribers().begin(); q != e->getSubscribers().end(); ++q)
                {
                    if(q != e->getSubscribers().begin())
                    {
                        out << ", ";
                    }
                    out << q->first;
                    if(!q->second.facet.empty())
                    {
                        out << ":" << q->second.facet;
                    }
                }
                out << "]";
            }

            shared_ptr<Key> key;
            if(s.keyValue.empty())
            {
                key = subscriber.keys[s.keyId].first;
            }
            else
            {
                key = topic->getKeyFactory()->decode(_instance->getCommunicator(), s.keyValue);
            }

            auto impl = topic->getSampleFactory()->create(_id,
                                                          topicId,
                                                          elementId,
                                                          s.id,
                                                          s.event,
                                                          key,
                                                          subscriber.tags[s.tag],
                                                          s.value,
                                                          s.timestamp);
            for(auto& es : e->getSubscribers())
            {
                if(es.second.initialized && (s.keyId <= 0 || es.second.keys.find(key) != es.second.keys.end()))
                {
                    es.second.lastId = s.id;
                    es.first->queue(impl, e->priority, shared_from_this(), current.facet, now, !s.keyValue.empty());
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

PublisherSessionI::PublisherSessionI(const std::shared_ptr<NodeI>& parent, const shared_ptr<NodePrx>& node) :
    SessionI(parent, node)
{
}

void
PublisherSessionI::destroy(const Ice::Current&)
{
    _parent->removePublisherSession(dynamic_pointer_cast<PublisherSessionI>(shared_from_this()), nullptr);
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
