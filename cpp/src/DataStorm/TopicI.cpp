// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/TopicI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;
using namespace DataStormContract;

namespace
{

template<typename K, typename V> vector<V>
toSeq(const map<K, V>& map)
{
    vector<V> seq;
    seq.reserve(map.size());
    for(const auto& p : map)
    {
        seq.push_back(p.second);
    }
    return seq;
}

}

TopicI::TopicI(const weak_ptr<TopicFactoryI>& factory,
               const shared_ptr<KeyFactory>& keyFactory,
               const shared_ptr<FilterFactory>& filterFactory,
               typename Sample::FactoryType sampleFactory,
               const string& name,
               long long int id) :
    _factory(factory),
    _keyFactory(keyFactory),
    _filterFactory(filterFactory),
    _sampleFactory(move(sampleFactory)),
    _name(name),
    _instance(factory.lock()->getInstance()),
    _traceLevels(_instance->getTraceLevels()),
    _id(id),
    _forwarder(Ice::uncheckedCast<SessionPrx>(_instance->getForwarderManager()->add(this))),
    _listenerCount(0),
    _waiters(0),
    _notified(0),
   _nextSampleId(0)
{
}

TopicI::~TopicI()
{
    disconnect();
    _instance->getForwarderManager()->remove(_forwarder->ice_getIdentity());
}

string
TopicI::getName() const
{
    return _name;
}

shared_ptr<KeyFactory>
TopicI::getKeyFactory() const
{
    return _keyFactory;
}

shared_ptr<FilterFactory>
TopicI::getFilterFactory() const
{
    return _filterFactory;
}

void
TopicI::destroy()
{
    {
        lock_guard<mutex> lock(_mutex);
        _forwarder->detachTopic(_id); // Must be called before disconnect()
    }
    disconnect();
}

KeyInfoSeq
TopicI::getKeyInfoSeq() const
{
    KeyInfoSeq keys;
    keys.reserve(_keyElements.size());
    for(const auto& element : _keyElements)
    {
        keys.push_back({ element.first->getId(), element.first->encode(_instance->getCommunicator()) });
    }
    return keys;
}

FilterInfoSeq
TopicI::getFilterInfoSeq() const
{
    FilterInfoSeq filters;
    filters.reserve(_filteredElements.size());
    for(const auto& element : _filteredElements)
    {
        filters.push_back({ element.first->getId(), element.first->encode(_instance->getCommunicator()) });
    }
    return filters;
}

TopicInfoAndContent
TopicI::getTopicInfoAndContent(long long int lastId) const
{
    return { _id, _name, lastId, getKeyInfoSeq(), getFilterInfoSeq() };
}

void
TopicI::attach(long long id, SessionI* session, const shared_ptr<SessionPrx>& prx)
{
    auto p = _listeners.find({ session });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session }, Listener(prx)).first;
    }

    if(p->second.topics.insert(id).second)
    {
        ++_listenerCount;
        session->subscribe(id, this);
        notifyListenerWaiters(session->getTopicLock());
    }
}

void
TopicI::detach(long long id, SessionI* session, bool unsubscribe)
{
    auto p = _listeners.find({ session });
    if(p != _listeners.end() && p->second.topics.erase(id))
    {
        --_listenerCount;
        session->unsubscribe(id, this, unsubscribe);
        notifyListenerWaiters(session->getTopicLock());
        if(p->second.topics.empty())
        {
            _listeners.erase(p);
        }
    }
}

KeyInfoAndSamplesSeq
TopicI::attachKeysAndFilters(long long int id,
                             const KeyInfoSeq& keys,
                             const FilterInfoSeq& filters,
                             long long int lastId,
                             SessionI* session,
                             const shared_ptr<SessionPrx>& prx)
{
    map<pair<shared_ptr<Key>, long long int>, KeyInfoAndSamples> ackKeys;
    map<shared_ptr<Filter>, FilterInfo> ackFilters;
    for(const auto& info : keys)
    {
        attachKeyImpl(id, info, -1, {}, lastId, session, prx, ackKeys, ackFilters);
    }
    for(const auto& info : filters)
    {
        attachFilterImpl(id, info, lastId, session, prx, ackKeys);
    }
    return toSeq(ackKeys);
}

DataSamplesSeq
TopicI::attachKeysAndFilters(long long int id,
                             const KeyInfoAndSamplesSeq& keys,
                             const FilterInfoSeq& filters,
                             long long int lastId,
                             SessionI* session,
                             const shared_ptr<SessionPrx>& prx)
{
    map<pair<shared_ptr<Key>, long long int>, KeyInfoAndSamples> ackKeys;
    map<shared_ptr<Filter>, FilterInfo> ackFilters;
    for(const auto& info : keys)
    {
        attachKeyImpl(id, info.info, info.subscriberId, info.samples, lastId, session, prx, ackKeys, ackFilters);
    }
    for(const auto& info : filters)
    {
        attachFilterImpl(id, info, lastId, session, prx, ackKeys);
    }
    DataSamplesSeq samples;
    for(const auto& k : ackKeys)
    {
        if(!k.second.samples.empty())
        {
            samples.push_back({ k.second.info.id, k.second.subscriberId, move(k.second.samples) });
        }
    }
    return samples;
}

pair<KeyInfoAndSamplesSeq, FilterInfoSeq>
TopicI::attachKeys(long long int id,
                   const KeyInfoSeq& infos,
                   long long int lastId,
                   SessionI* session,
                   const shared_ptr<SessionPrx>& prx)
{
    map<pair<shared_ptr<Key>, long long int>, KeyInfoAndSamples> ackKeys;
    map<shared_ptr<Filter>, FilterInfo> ackFilters;
    for(const auto& info : infos)
    {
        attachKeyImpl(id, info, -1, {}, lastId, session, prx, ackKeys, ackFilters);
    }
    return { toSeq(ackKeys), toSeq(ackFilters) };
}

KeyInfoAndSamplesSeq
TopicI::attachFilter(long long int id,
                     const FilterInfo& info,
                     long long int lastId,
                     SessionI* session,
                     const shared_ptr<SessionPrx>& prx)
{
    map<pair<shared_ptr<Key>, long long int>, KeyInfoAndSamples> ackKeys;
    attachFilterImpl(id, info, lastId, session, prx, ackKeys);
    return toSeq(ackKeys);
}

void
TopicI::waitForListeners(int count) const
{
    unique_lock<mutex> lock(_mutex);
    ++_waiters;
    while(true)
    {
        if(count < 0 && _listenerCount == 0)
        {
            --_waiters;
            return;
        }
        else if(count >= 0 && _listenerCount >= count)
        {
            --_waiters;
            return;
        }
        _cond.wait(lock);
        ++_notified;
    }
}

bool
TopicI::hasListeners() const
{
    unique_lock<mutex> lock(_mutex);
    return _listenerCount > 0;
}

void
TopicI::notifyListenerWaiters(unique_lock<mutex>& lock) const
{
    if(_waiters > 0)
    {
        _notified = 0;
        _cond.notify_all();
        _cond.wait(lock, [&]() { return _notified < _waiters; }); // Wait until all the waiters are notified.
    }
}

void
TopicI::disconnect()
{
    map<ListenerKey, Listener> listeners;
    {
        unique_lock<mutex> lock(_mutex);
        listeners.swap(_listeners);
        _listenerCount = 0;
        notifyListenerWaiters(lock);
    }
    for(auto s : listeners)
    {
        for(auto t : s.second.topics)
        {
            s.first.session->disconnect(t, this);
        }
    }
}

void
TopicI::attachKeyImpl(long long int id,
                      const KeyInfo& info,
                      long long int subscriberId,
                      const DataSampleSeq& samples,
                      long long int lastId,
                      SessionI* session,
                      const shared_ptr<SessionPrx>& prx,
                      map<pair<shared_ptr<Key>, long long int>, KeyInfoAndSamples>& ackKeys,
                      map<shared_ptr<Filter>, FilterInfo>& ackFilters)
{
    auto key = _keyFactory->decode(_instance->getCommunicator(), info.key);

    vector<shared_ptr<Sample>> samplesI;
    function<void(const shared_ptr<DataElementI>&)> queueSamples = [&](auto reader) {
        if(!samples.empty())
        {
            if(samplesI.empty())
            {
                for(const auto& sample : samples)
                {
                    samplesI.push_back(_sampleFactory(sample.id, sample.type, key, sample.value, sample.timestamp));
                }
            }
            reader->initSamples(samplesI);
        }
    };

    if(subscriberId < 0)
    {
        auto p = _keyElements.find(key);
        if(p != _keyElements.end())
        {
            DataSampleSeq samples;
            bool ack = ackKeys.find({ key, -1 }) == ackKeys.end(); // Don't ack twice the same element
            bool attached = false;
            for(auto k : p->second)
            {
                if(k->attachKey(id, info.id, key, session, prx))
                {
                    attached = true;
                    queueSamples(k);
                    if(ack)
                    {
                        auto s = k->getSamples(lastId, nullptr);
                        samples.insert(samples.end(), s.begin(), s.end());
                    }
                }
            }
            if(attached && ack)
            {
                sort(samples.begin(), samples.end(), [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; });
                ackKeys.emplace(make_pair(key, -1),
                                KeyInfoAndSamples
                                {
                                    {
                                        p->first->getId(),
                                        p->first->encode(_instance->getCommunicator())
                                    },
                                    -1,
                                    samples
                                });
            }
        }
    }

    for(const auto& element : _filteredElements)
    {
        if(subscriberId < 0  || subscriberId == element.first->getId())
        {
            if(element.first->match(key))
            {
                bool attached = false;
                for(auto f : element.second)
                {
                    if(f->attachKey(id, info.id, key, session, prx))
                    {
                        if(!element.first->hasWriterMatch() || subscriberId == element.first->getId())
                        {
                            queueSamples(f);
                        }
                        attached = true;
                    }
                }
                if(attached && ackFilters.find(element.first) == ackFilters.end())
                {
                    ackFilters.emplace(element.first,
                                       FilterInfo
                                       {
                                           element.first->getId(),
                                           element.first->encode(_instance->getCommunicator())
                                       });
                }
            }
        }
    }
}

void
TopicI::attachFilterImpl(long long int id,
                         const FilterInfo& info,
                         long long int lastId,
                         SessionI* session,
                         const shared_ptr<SessionPrx>& prx,
                         map<pair<shared_ptr<Key>, long long int>, KeyInfoAndSamples>& ackKeys)
{
    auto filter = _filterFactory->decode(_instance->getCommunicator(), info.filter);

    for(const auto& element : _keyElements)
    {
        if(filter->match(element.first))
        {
            DataSampleSeq samples;
            long long int subscriberId = filter->hasWriterMatch() ? info.id : -1;
            bool ack = ackKeys.find({ element.first, subscriberId }) == ackKeys.end();
            bool attached = false;
            for(auto k : element.second)
            {
                if(k->attachFilter(id, info.id, filter, session, prx))
                {
                    attached = true;
                    if(ack)
                    {
                        auto s = k->getSamples(lastId, filter);
                        samples.insert(samples.end(), s.begin(), s.end());
                    }
                }
            }
            if(attached && ack)
            {
                sort(samples.begin(), samples.end(), [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; });
                ackKeys.emplace(make_pair(element.first, subscriberId),
                                KeyInfoAndSamples
                                {
                                    {
                                        element.first->getId(),
                                        element.first->encode(_instance->getCommunicator())
                                    },
                                    subscriberId,
                                    samples
                                });
            }
        }
    }
}

void
TopicI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    // Forwarder proxy must be called with the mutex locked!
    for(auto listener : _listeners)
    {
        listener.second.proxy->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
    }
}

TopicReaderI::TopicReaderI(const shared_ptr<TopicFactoryI>& factory,
                           const shared_ptr<KeyFactory>& keyFactory,
                           const shared_ptr<FilterFactory>& filterFactory,
                           typename Sample::FactoryType sampleFactory,
                           const string& name,
                           long long int id) :
    TopicI(factory, keyFactory, filterFactory, move(sampleFactory), name, id)
{
}

shared_ptr<DataReader>
TopicReaderI::createFilteredDataReader(const shared_ptr<Filter>& filter)
{
    lock_guard<mutex> lock(_mutex);
    return addFiltered(make_shared<FilteredDataReaderI>(this, filter), filter);
}

shared_ptr<DataReader>
TopicReaderI::createDataReader(const vector<shared_ptr<Key>>& keys)
{
    lock_guard<mutex> lock(_mutex);
    return add(make_shared<KeyDataReaderI>(this, keys), keys);
}

void
TopicReaderI::waitForWriters(int count) const
{
    waitForListeners(count);
}

bool
TopicReaderI::hasWriters() const
{
    return hasListeners();
}

void
TopicReaderI::destroy()
{
    TopicI::destroy();

    auto factory = _factory.lock();;
    if(factory)
    {
        factory->removeTopicReader(_name, shared_from_this());
    }
}

void
TopicReaderI::removeFiltered(const shared_ptr<Filter>& filter, const shared_ptr<FilteredDataElementI>& element)
{
    auto p = _filteredElements.find(filter);
    if(p != _filteredElements.end())
    {
        p->second.erase(element);
        if(p->second.empty())
        {
            _filteredElements.erase(p);
        }
    }
}

void
TopicReaderI::remove(const vector<shared_ptr<Key>>& keys, const shared_ptr<KeyDataElementI>& element)
{
    for(auto key : keys)
    {
        auto p = _keyElements.find(key);
        if(p != _keyElements.end())
        {
            p->second.erase(element);
            if(p->second.empty())
            {
                _keyElements.erase(p);
            }
        }
    }
}

TopicWriterI::TopicWriterI(const shared_ptr<TopicFactoryI>& factory,
                           const shared_ptr<KeyFactory>& keyFactory,
                           const shared_ptr<FilterFactory>& filterFactory,
                           typename Sample::FactoryType sampleFactory,
                           const string& name,
                           long long int id) :
    TopicI(factory, keyFactory, filterFactory, move(sampleFactory), name, id)
{
}

shared_ptr<DataWriter>
TopicWriterI::createFilteredDataWriter(const shared_ptr<Filter>& filter)
{
    lock_guard<mutex> lock(_mutex);
    return addFiltered(make_shared<FilteredDataWriterI>(this, filter), filter);
}

shared_ptr<DataWriter>
TopicWriterI::createDataWriter(const vector<shared_ptr<Key>>& keys)
{
    lock_guard<mutex> lock(_mutex);
    return add(make_shared<KeyDataWriterI>(this, keys), keys);
}

void
TopicWriterI::waitForReaders(int count) const
{
    waitForListeners(count);
}

bool
TopicWriterI::hasReaders() const
{
    return hasListeners();
}

void
TopicWriterI::destroy()
{
    TopicI::destroy();

    auto factory = _factory.lock();
    if(factory)
    {
        factory->removeTopicWriter(_name, shared_from_this());
    }
}

void
TopicWriterI::removeFiltered(const shared_ptr<Filter>& filter, const shared_ptr<FilteredDataElementI>& element)
{
    auto p = _filteredElements.find(filter);
    if(p != _filteredElements.end())
    {
        p->second.erase(element);
        if(p->second.empty())
        {
            _filteredElements.erase(p);
        }
    }
}

void
TopicWriterI::remove(const vector<shared_ptr<Key>>& keys, const shared_ptr<KeyDataElementI>& element)
{
    for(auto key : keys)
    {
        auto p = _keyElements.find(key);
        if(p != _keyElements.end())
        {
            p->second.erase(element);
            if(p->second.empty())
            {
                _keyElements.erase(p);
            }
        }
    }
}
