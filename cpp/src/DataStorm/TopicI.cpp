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
               const shared_ptr<SampleFactory>& sampleFactory,
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
    _nextId(0),
    _nextFilteredId(0),
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

TopicSpec
TopicI::getTopicSpec() const
{
    TopicSpec spec;
    spec.id = _id;
    spec.name = _name;
    spec.elements.reserve(_keyElements.size() + _filteredElements.size());
    for(auto k : _keyElements)
    {
        spec.elements.push_back({ k.first->getId(), k.first->encode(_instance->getCommunicator()) });
    }
    for(auto f : _filteredElements)
    {
        spec.elements.push_back({ -f.first->getId(), f.first->encode(_instance->getCommunicator()) });
    }
    return spec;
}

ElementSpecSeq
TopicI::getElementSpecs(const ElementInfoSeq& infos)
{
    ElementSpecSeq specs;
    for(const auto& info : infos)
    {
        if(info.valueId > 0) // Key
        {
            auto key = _keyFactory->decode(_instance->getCommunicator(), info.value);
            auto p = _keyElements.find(key);
            if(p != _keyElements.end())
            {
                ElementDataSeq elements;
                for(auto k : p->second)
                {
                    elements.push_back({ k->getId(), k->getFacet(), k->getSampleFilterCriteria() });
                }
                specs.push_back({ move(elements), key->getId(), {}, info.valueId });
            }
            for(auto e : _filteredElements)
            {
                if(e.first->match(key))
                {
                    ElementDataSeq elements;
                    for(auto f : e.second)
                    {
                        elements.push_back({ f->getId(), f->getFacet(), f->getSampleFilterCriteria() });
                    }
                    specs.push_back({ move(elements),
                                      -e.first->getId(),
                                      e.first->encode(_instance->getCommunicator()),
                                      info.valueId });
                }
            }
        }
        else if(_filterFactory) // Filter
        {
            auto filter = _filterFactory->decode(_instance->getCommunicator(), info.value);
            for(auto e : _keyElements)
            {
                if(filter->match(e.first))
                {
                    ElementDataSeq elements;
                    for(auto k : e.second)
                    {
                        elements.push_back({ k->getId(), k->getFacet(), k->getSampleFilterCriteria() });
                    }
                    specs.push_back({ move(elements),
                                      e.first->getId(),
                                      e.first->encode(_instance->getCommunicator()),
                                      info.valueId });
                }
            }
        }
    }
    return specs;
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
TopicI::detach(long long id, SessionI* session)
{
    auto p = _listeners.find({ session });
    if(p != _listeners.end() && p->second.topics.erase(id))
    {
        --_listenerCount;
        session->unsubscribe(id, this);
        notifyListenerWaiters(session->getTopicLock());
        if(p->second.topics.empty())
        {
            _listeners.erase(p);
        }
    }
}

ElementSpecAckSeq
TopicI::attachElements(long long int topicId,
                       long long int lastId,
                       const ElementSpecSeq& elements,
                       SessionI* session,
                       const shared_ptr<SessionPrx>& prx)
{
    ElementSpecAckSeq specs;
    for(const auto& spec : elements)
    {
        if(spec.peerValueId > 0) // Key
        {
            auto key = _keyFactory->get(spec.peerValueId);
            auto p = _keyElements.find(key);
            if(p != _keyElements.end())
            {
                shared_ptr<Filter> filter;
                if(spec.valueId < 0) // Filter
                {
                    if(!_filterFactory)
                    {
                        // TODO
                    }
                    filter = _filterFactory->decode(_instance->getCommunicator(), spec.value);
                }
                for(auto e : p->second)
                {
                    ElementDataAckSeq datas;
                    for(const auto& data : spec.elements)
                    {
                        auto sampleFilter = e->createSampleFilter(data.sampleFilter);
                        bool attached = false;
                        if(spec.valueId > 0) // Key
                        {
                            attached = e->attachKey(topicId, data.id, key, sampleFilter, session, prx, data.facet);
                        }
                        else if(filter->match(key))
                        {
                            attached = e->attachFilter(topicId, data.id, filter, sampleFilter, session, prx, data.facet);
                        }
                        if(attached)
                        {
                            datas.push_back({ e->getId(),
                                              e->getFacet(),
                                              e->getSampleFilterCriteria(),
                                              e->getSamples(lastId, sampleFilter),
                                              data.id });
                        }
                    }
                    if(!datas.empty())
                    {
                        specs.push_back({ move(datas),
                                          key->getId(),
                                          spec.valueId < 0 ? key->encode(_instance->getCommunicator()) : ByteSeq(),
                                          spec.valueId });
                    }
                }
            }
        }
        else if(_filterFactory) // Filter
        {
            auto filter = _filterFactory->get(-spec.peerValueId);
            auto p = _filteredElements.find(filter);
            if(p != _filteredElements.end())
            {
                shared_ptr<Key> key;
                if(spec.valueId > 0) // Key
                {
                    key = _keyFactory->decode(_instance->getCommunicator(), spec.value);
                }

                for(auto e : p->second)
                {
                    ElementDataAckSeq datas;
                    for(const auto& data : spec.elements)
                    {
                        auto sampleFilter = e->createSampleFilter(data.sampleFilter);
                        bool attached = false;
                        if(spec.valueId < 0) // Filter
                        {
                            attached = e->attachFilter(topicId, data.id, filter, sampleFilter, session, prx, data.facet);
                        }
                        else if(filter->match(key))
                        {
                            attached = e->attachKey(topicId, data.id, key, sampleFilter, session, prx, data.facet);
                        }
                        if(attached)
                        {
                            datas.push_back({ e->getId(),
                                              e->getFacet(),
                                              e->getSampleFilterCriteria(),
                                              e->getSamples(lastId, sampleFilter),
                                              data.id });
                        }
                    }
                    if(!datas.empty())
                    {
                        specs.push_back({ move(datas),
                                          -filter->getId(),
                                          spec.valueId > 0 ? filter->encode(_instance->getCommunicator()) : ByteSeq(),
                                          spec.valueId });
                    }
                }
            }
        }
    }
    return specs;
}

DataSamplesSeq
TopicI::attachElementsAck(long long int topicId,
                          long long int lastId,
                          const ElementSpecAckSeq& elements,
                          SessionI* session,
                          const shared_ptr<SessionPrx>& prx)
{
    DataSamplesSeq samples;
    auto initSamples = [&](auto element, auto elementId, const auto& key, const auto& facet, const auto& samples, auto& samplesI)
    {
        if(samplesI.empty() && !samples.empty())
        {
            samplesI.reserve(samples.size());
            for(auto& s : samples)
            {
                samplesI.push_back(_sampleFactory->create(session->getId(),
                                                          topicId,
                                                          elementId,
                                                          s.id,
                                                          s.event,
                                                          key,
                                                          s.value,
                                                          s.timestamp));
            }
        }
        element->initSamples(samplesI, session, topicId, elementId);
    };

    for(const auto& spec : elements)
    {
        if(spec.peerValueId > 0) // Key
        {
            auto key = _keyFactory->get(spec.peerValueId);
            auto p = _keyElements.find(key);
            if(p != _keyElements.end())
            {
                shared_ptr<Filter> filter;
                if(spec.valueId < 0)
                {
                    if(!_filterFactory)
                    {
                        // TODO
                    }
                    filter = _filterFactory->decode(_instance->getCommunicator(), spec.value);
                }

                vector<std::shared_ptr<Sample>> samplesI;
                for(auto e : p->second)
                {
                    for(const auto& data : spec.elements)
                    {
                        if(data.peerId == e->getId())
                        {
                            auto sampleFilter = e->createSampleFilter(data.sampleFilter);
                            if(spec.valueId > 0) // Key
                            {
                                e->attachKey(topicId, data.id, key, sampleFilter, session, prx, data.facet);
                                initSamples(e, data.id, key, data.facet, data.samples, samplesI);
                            }
                            else if(filter->match(key)) // Filter
                            {
                                e->attachFilter(topicId, data.id, filter, sampleFilter, session, prx, data.facet);
                                initSamples(e, data.id, nullptr, data.facet, data.samples, samplesI);
                            }
                            samples.push_back({ e->getId(), e->getSamples(lastId, sampleFilter) });
                            break;
                        }
                    }
                }
            }
        }
        else // Filter
        {
            auto filter = _filterFactory->get(-spec.peerValueId);
            auto p = _filteredElements.find(filter);
            if(p != _filteredElements.end())
            {
                shared_ptr<Key> key;
                if(spec.valueId > 0) // Key
                {
                    key = _keyFactory->decode(_instance->getCommunicator(), spec.value);
                }

                vector<std::shared_ptr<Sample>> samplesI;
                for(auto e : p->second)
                {
                    for(const auto& data : spec.elements)
                    {
                        if(data.peerId == e->getId())
                        {
                            auto sampleFilter = e->createSampleFilter(data.sampleFilter);
                            if(spec.valueId < 0) // Filter
                            {
                                e->attachFilter(topicId, data.id, filter, sampleFilter, session, prx, data.facet);
                                initSamples(e, data.id, nullptr, data.facet, data.samples, samplesI);
                            }
                            else if(filter->match(key))
                            {
                                e->attachKey(topicId, data.id, key, sampleFilter, session, prx, data.facet);
                                initSamples(e, data.id, key, data.facet, data.samples, samplesI);
                            }
                            samples.push_back({ e->getId(), e->getSamples(lastId, sampleFilter) });
                        }
                    }
                }
            }
        }
    }
    return samples;
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
TopicI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    // Forwarder proxy must be called with the mutex locked!
    for(auto listener : _listeners)
    {
        listener.second.proxy->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
    }
}

void
TopicI::add(const shared_ptr<KeyDataElementI>& element, const vector<shared_ptr<Key>>& keys)
{
    ElementInfoSeq infos;
    for(const auto& key : keys)
    {
        auto p = _keyElements.find(key);
        if(p == _keyElements.end())
        {
            p = _keyElements.emplace(key, set<shared_ptr<KeyDataElementI>>()).first;
        }
        assert(element);
        infos.push_back({ key->getId(), key->encode(_instance->getCommunicator()) });
        p->second.insert(element);
    }
    if(!infos.empty())
    {
        _forwarder->announceElements(_id, infos);
    }
}

void
TopicI::addFiltered(const shared_ptr<FilteredDataElementI>& element, const shared_ptr<Filter>& filter)
{
    auto p = _filteredElements.find(filter);
    if(p == _filteredElements.end())
    {
        p = _filteredElements.emplace(filter, set<shared_ptr<FilteredDataElementI>>()).first;
    }
    assert(element);
    p->second.insert(element);
    _forwarder->announceElements(_id, { { -filter->getId(), filter->encode(_instance->getCommunicator()) } });
}

TopicReaderI::TopicReaderI(const shared_ptr<TopicFactoryI>& factory,
                           const shared_ptr<KeyFactory>& keyFactory,
                           const shared_ptr<FilterFactory>& filterFactory,
                           const shared_ptr<SampleFactory>& sampleFactory,
                           const string& name,
                           long long int id) :
    TopicI(factory, keyFactory, filterFactory, move(sampleFactory), name, id)
{
}

shared_ptr<DataReader>
TopicReaderI::createFiltered(const shared_ptr<Filter>& filter, vector<unsigned char> sampleFilter)
{
    lock_guard<mutex> lock(_mutex);
    auto element = make_shared<FilteredDataReaderI>(this, ++_nextFilteredId, filter, move(sampleFilter));
    addFiltered(element, filter);
    return element;
}

shared_ptr<DataReader>
TopicReaderI::create(const vector<shared_ptr<Key>>& keys, vector<unsigned char> sampleFilter)
{
    lock_guard<mutex> lock(_mutex);
    auto element = make_shared<KeyDataReaderI>(this, ++_nextId, keys, move(sampleFilter));
    add(element, keys);
    return element;
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
                           const shared_ptr<SampleFactory>& sampleFactory,
                           const string& name,
                           long long int id) :
    TopicI(factory, keyFactory, filterFactory, move(sampleFactory), name, id)
{
}

shared_ptr<DataWriter>
TopicWriterI::createFiltered(const shared_ptr<Filter>& filter, const shared_ptr<FilterFactory>& sampleFilterFactory)
{
    lock_guard<mutex> lock(_mutex);
    auto element = make_shared<FilteredDataWriterI>(this, ++_nextFilteredId, filter, sampleFilterFactory);
    addFiltered(element, filter);
    return element;
}

shared_ptr<DataWriter>
TopicWriterI::create(const shared_ptr<Key>& key, const shared_ptr<FilterFactory>& sampleFilterFactory)
{
    lock_guard<mutex> lock(_mutex);
    auto element = make_shared<KeyDataWriterI>(this, ++_nextId, key, sampleFilterFactory);
    add(element, { key });
    return element;
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
