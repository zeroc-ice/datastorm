// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/TopicI.h>
#include <DataStorm/SampleI.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/PeerI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;

TopicI::TopicI(const weak_ptr<TopicFactoryI>& factory,
               const shared_ptr<KeyFactory>& keyFactory,
               const shared_ptr<FilterFactory>& filterFactory,
               const string& name,
               long long int id) :
    _factory(factory),
    _keyFactory(keyFactory),
    _filterFactory(filterFactory),
    _name(name),
    _instance(factory.lock()->getInstance()),
    _traceLevels(_instance->getTraceLevels()),
    _id(id),
    _forwarder(Ice::uncheckedCast<DataStormContract::SessionPrx>(_instance->getForwarderManager()->add(this))),
    _nextSampleId(0)
{
}

TopicI::~TopicI()
{
    disconnect();
    _instance->getForwarderManager()->remove(_forwarder->ice_getIdentity());
}

void
TopicI::disconnect()
{
    map<Listener, shared_ptr<DataStormContract::SessionPrx>> sessions;
    {
        lock_guard<mutex> lock(_mutex);
        _sessions.swap(sessions);
    }
    for(auto s : sessions)
    {
        s.first.session->disconnect(s.first.id);
    }
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

DataStormContract::KeyInfoSeq
TopicI::getKeyInfoSeq() const
{
    DataStormContract::KeyInfoSeq keys;
    if(!_keyElements.empty())
    {
        keys.reserve(_keyElements.size());
        for(const auto& element : _keyElements)
        {
            keys.push_back(element.second->getKeyInfo());
        }
    }
    return keys;
}

DataStormContract::FilterInfoSeq
TopicI::getFilterInfoSeq() const
{
    DataStormContract::FilterInfoSeq filters;
    if(!_filteredElements.empty())
    {
        filters.reserve(_filteredElements.size());
        for(const auto& element : _filteredElements)
        {
            filters.push_back(element.second->getFilterInfo());
        }
    }
    return filters;
}

DataStormContract::TopicInfoAndContent
TopicI::getTopicInfoAndContent(long long int lastId) const
{
    return { _id, _name, lastId, getKeyInfoSeq(), getFilterInfoSeq() };
}

void
TopicI::attach(long long id, SessionI* session, const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    if(_sessions.emplace(Listener { id, session }, prx).second)
    {
        session->subscribe(id, this);
    }
}

void
TopicI::detach(long long id, SessionI* session, bool unsubscribe)
{
    assert(_sessions.find({ id, session }) != _sessions.end());
    _sessions.erase({ id, session });
    session->unsubscribe(id, unsubscribe);
}

DataStormContract::KeyInfoAndSamplesSeq
TopicI::attachKeysAndFilters(long long int id,
                             const DataStormContract::KeyInfoSeq& keys,
                             const DataStormContract::FilterInfoSeq& filters,
                             long long int lastId,
                             SessionI* session,
                             const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    DataStormContract::KeyInfoAndSamplesSeq ackKeys;
    DataStormContract::FilterInfoSeq ackFilters;
    for(const auto& info : keys)
    {
        attachKeyImpl(id, info, {}, lastId, session, prx, ackKeys, ackFilters);
    }
    for(const auto& info : filters)
    {
        attachFilterImpl(id, info, lastId, session, prx, ackKeys);
    }
    return ackKeys;
}

DataStormContract::DataSamplesSeq
TopicI::attachKeysAndFilters(long long int id,
                             const DataStormContract::KeyInfoAndSamplesSeq& keys,
                             const DataStormContract::FilterInfoSeq& filters,
                             long long int lastId,
                             SessionI* session,
                             const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    DataStormContract::KeyInfoAndSamplesSeq ackKeys;
    DataStormContract::FilterInfoSeq ackFilters;
    for(const auto& info : keys)
    {
        attachKeyImpl(id, info.info, info.samples, lastId, session, prx, ackKeys, ackFilters);
    }
    for(const auto& info : filters)
    {
        attachFilterImpl(id, info, lastId, session, prx, ackKeys);
    }
    DataStormContract::DataSamplesSeq samples;
    for(const auto& k : ackKeys)
    {
        if(!k.samples.empty())
        {
            samples.emplace_back(DataStormContract::DataSamples { k.info.id, move(k.samples) });
        }
    }
    return samples;
}

pair<DataStormContract::KeyInfoAndSamplesSeq, DataStormContract::FilterInfoSeq>
TopicI::attachKey(long long int id,
                  const DataStormContract::KeyInfo& info,
                  long long int lastId,
                  SessionI* session,
                  const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    DataStormContract::KeyInfoAndSamplesSeq ackKeys;
    DataStormContract::FilterInfoSeq ackFilters;
    attachKeyImpl(id, info, {}, lastId, session, prx, ackKeys, ackFilters);
    return { ackKeys, ackFilters };
}

DataStormContract::KeyInfoAndSamplesSeq
TopicI::attachFilter(long long int id,
                     const DataStormContract::FilterInfo& info,
                     long long int lastId,
                     SessionI* session,
                     const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    DataStormContract::KeyInfoAndSamplesSeq ackKeys;
    attachFilterImpl(id, info, lastId, session, prx, ackKeys);
    return ackKeys;
}

void
TopicI::attachKeyImpl(long long int id,
                      const DataStormContract::KeyInfo& info,
                      const DataStormContract::DataSampleSeq& samples,
                      long long int lastId,
                      SessionI* session,
                      const shared_ptr<DataStormContract::SessionPrx>& prx,
                      DataStormContract::KeyInfoAndSamplesSeq& ackKeys,
                      DataStormContract::FilterInfoSeq& ackFilters)
{
    auto key = _keyFactory->unmarshal(_instance->getCommunicator(), info.key);
    auto p = _keyElements.find(key);
    if(p != _keyElements.end())
    {
        if(p->second->attachKey(id, info.id, key, session, prx))
        {
            if(!samples.empty())
            {
                for(const auto& sample : samples)
                {
                    p->second->queue(make_shared<SampleI>(_instance->getCommunicator(), key, sample));
                }
            }
            ackKeys.push_back(p->second->getKeyInfoAndSamples(lastId));
        }
    }

    for(const auto& element : _filteredElements)
    {
        if(element.first->match(key))
        {
            if(element.second->attachKey(id, info.id, key, session, prx))
            {
                ackFilters.push_back(element.second->getFilterInfo());
            }
        }
    }
}

void
TopicI::attachFilterImpl(long long int id,
                         const DataStormContract::FilterInfo& info,
                         long long int lastId,
                         SessionI* session,
                         const shared_ptr<DataStormContract::SessionPrx>& prx,
                         DataStormContract::KeyInfoAndSamplesSeq& ack)
{
    auto filter = _filterFactory->unmarshal(_instance->getCommunicator(), info.filter);

    for(const auto& element : _keyElements)
    {
        if(filter->match(element.first))
        {
            if(element.second->attachFilter(id, info.id, filter, session, prx))
            {
                ack.push_back(element.second->getKeyInfoAndSamples(lastId));
            }
        }
    }
}

void
TopicI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    // Forwarder proxy must be called with the mutex locked!
    for(auto session : _sessions)
    {
        session.second->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
    }
}

TopicReaderI::TopicReaderI(const shared_ptr<TopicFactoryI>& factory,
                           const shared_ptr<KeyFactory>& keyFactory,
                           const shared_ptr<FilterFactory>& filterFactory,
                           const string& name,
                           long long int id) :
    TopicI(factory, keyFactory, filterFactory, name, id)
{
}

shared_ptr<DataReader>
TopicReaderI::getFilteredDataReader(const shared_ptr<Filter>& filter)
{
    lock_guard<mutex> lock(_mutex);
    return getFiltered<FilteredDataReaderI>(filter);
}

shared_ptr<DataReader>
TopicReaderI::getDataReader(const shared_ptr<Key>& key)
{
    lock_guard<mutex> lock(_mutex);
    return get<KeyDataReaderI>(key);
}

void
TopicReaderI::destroy()
{
    TopicI::destroy();

    auto factory = _factory.lock();;
    if(factory)
    {
        factory->removeTopicReader(_name);
    }
}

void
TopicReaderI::removeFiltered(const shared_ptr<Filter>& filter)
{
    _filteredElements.erase(filter);
}

void
TopicReaderI::remove(const shared_ptr<Key>& key)
{
    _keyElements.erase(key);
}

shared_ptr<KeyDataElementI>
TopicReaderI::makeElement(const shared_ptr<Key>& key)
{
    auto element = make_shared<KeyDataReaderI>(this, key);
    _forwarder->announceKey(_id, element->getKeyInfo());
    return element;
}

shared_ptr<FilteredDataElementI>
TopicReaderI::makeFilteredElement(const shared_ptr<Filter>& filter)
{
    auto element = make_shared<FilteredDataReaderI>(this, filter);
    _forwarder->announceFilter(_id, element->getFilterInfo());
    return element;
}

TopicWriterI::TopicWriterI(const shared_ptr<TopicFactoryI>& factory,
                           const shared_ptr<KeyFactory>& keyFactory,
                           const shared_ptr<FilterFactory>& filterFactory,
                           const string& name,
                           long long int id) :
    TopicI(factory, keyFactory, filterFactory, name, id)
{
}

shared_ptr<DataWriter>
TopicWriterI::getFilteredDataWriter(const shared_ptr<Filter>& filter)
{
    lock_guard<mutex> lock(_mutex);
    return getFiltered<FilteredDataWriterI>(filter);
}

shared_ptr<DataWriter>
TopicWriterI::getDataWriter(const shared_ptr<Key>& key)
{
    lock_guard<mutex> lock(_mutex);
    return get<KeyDataWriterI>(key);
}

void
TopicWriterI::destroy()
{
    TopicI::destroy();

    auto factory = _factory.lock();
    if(factory)
    {
        factory->removeTopicWriter(_name);
    }
}

void
TopicWriterI::removeFiltered(const shared_ptr<Filter>& filter)
{
    _filteredElements.erase(filter);
}

void
TopicWriterI::remove(const shared_ptr<Key>& key)
{
    _keyElements.erase(key);
}

shared_ptr<KeyDataElementI>
TopicWriterI::makeElement(const shared_ptr<Key>& key)
{
    auto element = make_shared<KeyDataWriterI>(this, key);
    _forwarder->announceKey(_id, element->getKeyInfo());
    return element;
}

shared_ptr<FilteredDataElementI>
TopicWriterI::makeFilteredElement(const shared_ptr<Filter>& filter)
{
    auto element = make_shared<FilteredDataWriterI>(this, filter);
    _forwarder->announceFilter(_id, element->getFilterInfo());
    return element;
}

TopicFactoryI::TopicFactoryI(const shared_ptr<Ice::Communicator>& communicator)
{
    _instance = make_shared<Instance>(communicator);
    _traceLevels = _instance->getTraceLevels();
}

void
TopicFactoryI::init()
{
    _instance->init(shared_from_this());
}

shared_ptr<TopicReader>
TopicFactoryI::getTopicReader(const string& name,
                              function<shared_ptr<KeyFactory>()> createKeyFactory,
                              function<shared_ptr<FilterFactory>()> createFilterFactory)
{
    shared_ptr<TopicReaderI> reader;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _readers.find(name);
        if(p != _readers.end())
        {
            return p->second;
        }

        if(!_subscriber)
        {
            _subscriber = make_shared<SubscriberI>(_instance);
            _subscriber->init();
        }
        reader = make_shared<TopicReaderI>(shared_from_this(),
                                           createKeyFactory(),
                                           createFilterFactory(),
                                           name,
                                           _nextReaderId++);
        _readers.insert(make_pair(name, reader));
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << "added topic reader `" << name << "'";
        }
    }
    _subscriber->getForwarder()->announceTopics({ { reader->getId(), name } });
    _instance->getTopicLookup()->announceTopicSubscriberAsync(reader->getName(), _subscriber->getProxy());
    return reader;
}

shared_ptr<TopicWriter>
TopicFactoryI::getTopicWriter(const string& name,
                              function<shared_ptr<KeyFactory>()> createKeyFactory,
                              function<shared_ptr<FilterFactory>()> createFilterFactory)
{
    shared_ptr<TopicWriterI> writer;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _writers.find(name);
        if(p != _writers.end())
        {
            return p->second;
        }

        if(!_publisher)
        {
            _publisher = make_shared<PublisherI>(_instance);
            _publisher->init();
        }
        writer = make_shared<TopicWriterI>(shared_from_this(),
                                           createKeyFactory(),
                                           createFilterFactory(),
                                           name,
                                           _nextWriterId++);
        _writers.insert(make_pair(name, writer));
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << "added topic writer `" << name << "'";
        }
    }
    _publisher->getForwarder()->announceTopics({ { writer->getId(), name } });
    _instance->getTopicLookup()->announceTopicPublisherAsync(writer->getName(), _publisher->getProxy());
    return writer;
}

void
TopicFactoryI::destroy(bool ownsCommunicator)
{
    if(ownsCommunicator)
    {
        _instance->getCommunicator()->destroy();
    }
}

void
TopicFactoryI::removeTopicReader(const string& name)
{
    lock_guard<mutex> lock(_mutex);
    if(_traceLevels->topic > 0)
    {
        Trace out(_traceLevels, _traceLevels->topicCat);
        out << "removed topic reader `" << name << "'";
    }
    _readers.erase(name);
}

void
TopicFactoryI::removeTopicWriter(const string& name)
{
    lock_guard<mutex> lock(_mutex);
    if(_traceLevels->topic > 0)
    {
        Trace out(_traceLevels, _traceLevels->topicCat);
        out << "removed topic writer `" << name << "'";
    }
    _writers.erase(name);
}

shared_ptr<TopicReaderI>
TopicFactoryI::getTopicReader(const string& name) const
{
    lock_guard<mutex> lock(_mutex);
    auto p = _readers.find(name);
    if(p == _readers.end())
    {
        return nullptr;
    }
    return p->second;
}

shared_ptr<TopicWriterI>
TopicFactoryI::getTopicWriter(const string& name) const
{
    lock_guard<mutex> lock(_mutex);
    auto p = _writers.find(name);
    if(p == _writers.end())
    {
        return nullptr;
    }
    return p->second;
}

void
TopicFactoryI::createSession(const string& topic, const shared_ptr<DataStormContract::PublisherPrx>& publisher)
{
    auto reader = getTopicReader(topic);
    if(reader)
    {
        {
            lock_guard<mutex> lock(_mutex);
            if(!_subscriber)
            {
                _subscriber = make_shared<SubscriberI>(_instance);
            }
        }
        _subscriber->createSession(publisher);
    }
}

void
TopicFactoryI::createSession(const string& topic, const shared_ptr<DataStormContract::SubscriberPrx>& subscriber)
{
    auto writer = getTopicWriter(topic);
    if(writer)
    {
        {
            lock_guard<mutex> lock(_mutex);
            if(!_publisher)
            {
                _publisher = make_shared<PublisherI>(_instance);
            }
        }
        _publisher->createSession(subscriber);
    }
}

DataStormContract::TopicInfoSeq
TopicFactoryI::getTopicReaders() const
{
    lock_guard<mutex> lock(_mutex);
    DataStormContract::TopicInfoSeq readers;
    readers.reserve(_readers.size());
    for(const auto& p : _readers)
    {
        readers.push_back({ p.second->getId(), p.first });
    }
    return readers;
}

DataStormContract::TopicInfoSeq
TopicFactoryI::getTopicWriters() const
{
    lock_guard<mutex> lock(_mutex);
    DataStormContract::TopicInfoSeq writers;
    writers.reserve(_writers.size());
    for(const auto& p : _writers)
    {
        writers.push_back({ p.second->getId(), p.first});
    }
    return writers;
}

shared_ptr<Ice::Communicator>
TopicFactoryI::getCommunicator() const
{
    return _instance->getCommunicator();
}

shared_ptr<TopicFactory>
DataStormInternal::createTopicFactory(const shared_ptr<Ice::Communicator>& communicator)
{
    return make_shared<TopicFactoryI>(communicator);
}
