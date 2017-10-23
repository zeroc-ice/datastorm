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

    for(const auto& info : topics)
    {
        runWithTopics(info.name, [&](auto topic)
        {
            for(auto id : info.ids)
            {
                topic->attach(id, this, _session);
            }
            _session->attachTopicAsync(topic->getTopicSpec());
        });
    }
}

void
SessionI::attachTopic(TopicSpec spec, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(spec.name, [&](auto topic)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": attaching topic `" << spec << "' to `" << topic.get() << "'";
        }

        topic->attach(spec.id, this, _session);
        auto specs = topic->getElementSpecs(spec.elements);
        if(!specs.empty())
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": matched elements `" << spec << "' on `" << topic.get() << "'";
            }
            _session->attachElementsAsync(topic->getId(), getLastId(spec.id), specs);
        }
    });
}

void
SessionI::detachTopic(long long int id, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(id, [&](auto topic, auto& subscriber)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": detaching topic `" << id << "' from `" << topic << "'";
        }
        topic->detach(id, this);
    });

    // Erase the topic
    _topics.erase(id);
}

void
SessionI::announceElements(long long int topicId, ElementInfoSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }


    runWithTopics(topicId, [&](auto topic, auto& subscriber, auto& subscribers)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": announcing elements `[" << elements << "]@" << topicId << "' on topic `" << topic << "'";
        }

        auto specs = topic->getElementSpecs(elements);
        if(!specs.empty())
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": matched elements `[" << specs << "]@" << topicId << "' on topic `" << topic << "'";
            }
            _session->attachElementsAsync(topic->getId(), getLastId(topicId), specs);
        }
    });
}

void
SessionI::attachElements(long long int id, long long int lastId, ElementSpecSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(id, [&](auto topic, auto& subscriber, auto& subscribers)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": attaching elements `[" << elements << "]@" << id << "' on topic `" << topic << "'";
        }

        auto specAck = topic->attachElements(id, lastId, elements, this, _session);
        if(!specAck.empty())
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": matched elements `[" << specAck << "]@" << id << "' on topic `" << topic << "'";
            }
            _session->attachElementsAckAsync(topic->getId(), getLastId(id), specAck);
        }
    });
}

void
SessionI::attachElementsAck(long long int id, long long int lastId, ElementSpecAckSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(id, [&](auto topic, auto& subscriber, auto& subscribers)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": attaching elements ack `[" << elements << "]@" << id << "' on topic `" << topic << "'";
        }

        auto samples = topic->attachElementsAck(id, lastId, elements, this, _session);
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

    runWithTopics(id, [&](auto topic, auto& subscriber)
    {
        if(_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": detaching elements `[" << elements << "]@" << id << "' on topic `" << topic << "'";
        }

        for(auto e : elements)
        {
            if(e > 0)
            {
                auto k = subscriber.keys.remove(e);
                for(auto& ks : k.getSubscribers())
                {
                    ks.first->detachKey(id, e, this, ks.second.facet);
                }
            }
            else
            {
                auto f = subscriber.filters.remove(-e);
                for(auto& fs : f.getSubscribers())
                {
                    fs.first->detachFilter(id, -e, this, fs.second.facet);
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

    auto communicator = _instance->getCommunicator();
    for(const auto& samples : samplesSeq)
    {
        runWithTopics(topicId, [&](auto topic, auto& subscriber)
        {
            if(samples.id > 0)
            {
                auto k = subscriber.keys.get(samples.id);
                if(k)
                {
                    if(_traceLevels->session > 2)
                    {
                        Trace out(_traceLevels, _traceLevels->sessionCat);
                        out << _id << ": initializing samples from `" << samples.id << ":" << k->get() << "@" << topicId;
                        out << " on ";
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
                        samplesI.push_back(sampleFactory->create(_id,
                                                                 topicId,
                                                                 samples.id,
                                                                 s.id,
                                                                 s.event,
                                                                 k->get(),
                                                                 s.value,
                                                                 s.timestamp));
                    }
                    for(auto& ks : k->getSubscribers())
                    {
                        if(!ks.second.initialized)
                        {
                            ks.second.initialized = true;
                            ks.first->initSamples(samplesI, nullptr, 0, 0);
                        }
                    }
                }
            }
            else
            {
                auto f = subscriber.filters.get(-samples.id);
                if(f)
                {
                    if(_traceLevels->session > 2)
                    {
                        Trace out(_traceLevels, _traceLevels->sessionCat);
                        out << _id << ": initializing samples from `" << samples.id << ":" << f->get() << "@" << topicId;
                        out << " on ";
                        for(auto q = f->getSubscribers().begin(); q != f->getSubscribers().end(); ++q)
                        {
                            if(q != f->getSubscribers().begin())
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
                        samplesI.push_back(sampleFactory->create(_id,
                                                                 topicId,
                                                                 samples.id,
                                                                 s.id,
                                                                 s.event,
                                                                 nullptr,
                                                                 s.value,
                                                                 s.timestamp));
                    }
                    for(auto& fs : f->getSubscribers())
                    {
                        if(!fs.second.initialized)
                        {
                            fs.second.initialized = true;
                            fs.first->initSamples(samplesI, nullptr, 0, 0);
                        }
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
            e.first->detachFilter(id, f.first, this, e.second.facet, false);
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
SessionI::subscribeToKey(long long topicId, long long int elementId, const shared_ptr<Key>& key, DataElementI* element,
                         const string& facet)
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
    subscriber.keys.get(elementId, key)->add(element, facet);
}

void
SessionI::unsubscribeFromKey(long long topicId, long long int elementId, DataElementI* element)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    auto k = subscriber.keys.get(elementId);
    if(k)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed element `e" << elementId << '@' << topicId << "' from `" << element << "'";
        }
        k->remove(element);
    }
}

void
SessionI::disconnectFromKey(long long topicId, long long int elementId, DataElementI* element)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if(!_session)
    {
        return;
    }

    runWithTopic(topicId, element->getTopic(), [&](auto&) { unsubscribeFromKey(topicId, elementId, element); });
}

void
SessionI::subscribeToFilter(long long topicId, long long int elementId, const shared_ptr<Filter>& filter,
                            DataElementI* element, const string& facet)
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
    subscriber.filters.get(elementId, filter)->add(element, facet);
}

void
SessionI::unsubscribeFromFilter(long long topicId, long long int elementId, DataElementI* element)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    auto f = subscriber.filters.get(elementId);
    if(f)
    {
        if(_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed element `e" << elementId << '@' << topicId << "' from `" << element << "'";
        }
        f->remove(element);
    }
}

void
SessionI::disconnectFromFilter(long long topicId, long long int elementId, DataElementI* element)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if(!_session)
    {
        return;
    }

    runWithTopic(topicId, element->getTopic(), [&](auto&) { unsubscribeFromFilter(topicId, elementId, element); });
}

void
SessionI::subscriberInitialized(long long topicId, long long int elementId, DataElementI* element)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    if(elementId > 0)
    {
        auto k = subscriber.keys.get(elementId);
        if(k)
        {
            auto p = k->getSubscribers().find(element);
            if(p != k->getSubscribers().end())
            {
                if(_traceLevels->session > 1)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": initialized `" << p->first << "' from `" << elementId << '@' << topicId << "'";
                }
                p->second.initialized = true;
            }
        }
    }
    else
    {
        auto f = subscriber.filters.get(-elementId);
        if(f)
        {
            auto p = f->getSubscribers().find(element);
            if(p != f->getSubscribers().end())
            {
                if(_traceLevels->session > 1)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": initialized `" << p->first << "' from `" << elementId << '@' << topicId << "'";
                }
                p->second.initialized = true;
            }
        }
    }
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
        l.unlock();
        topic->flushQueue();
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
            l.unlock();
            s.first->flushQueue();
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
            l.unlock();
            s.first->flushQueue();
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
            l.unlock();
            topic->flushQueue();
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
SubscriberSessionI::s(long long int topicId, long long int element, DataSample s, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if(!_session)
    {
        return;
    }

    runWithTopics(topicId, [&](auto topic, auto& subscriber, auto topicSubscribers)
    {
        if(element <= 0)
        {
            return;
        }
        auto e = subscriber.keys.get(element);
        if(e && topicSubscribers.setLastId(s.id))
        {
            if(_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": queuing sample `" << s.id << "' from `" << element << '@' << topicId << "'";
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
            auto impl = topic->getSampleFactory()->create(_id,
                                                          topicId,
                                                          element,
                                                          s.id,
                                                          s.event,
                                                          e->get(),
                                                          s.value,
                                                          s.timestamp);
            for(auto& es : e->getSubscribers())
            {
                if(es.second.initialized && es.first->getFacet() == current.facet)
                {
                    es.first->queue(impl);
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
