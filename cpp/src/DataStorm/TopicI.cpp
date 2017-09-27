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

TopicI::TopicI(weak_ptr<TopicFactoryI> factory, shared_ptr<KeyFactory> keyFactory, const string& name) :
    _factory(factory),
    _keyFactory(keyFactory),
    _name(name),
    _instance(factory.lock()->getInstance()),
    _traceLevels(_instance->getTraceLevels()),
    _id(0)
{
}

shared_ptr<DataStorm::TopicFactory>
TopicI::getTopicFactory() const
{
    return _instance->getTopicFactory();
}

void
TopicI::destroy()
{
    _impl->destroy();
}

void
TopicI::attach(SessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    if(_traceLevels->topic > 0)
    {
        Trace out(_traceLevels, _traceLevels->topicCat);
        out << "attached session `" << session << "' to topic `" << this << "'";
    }

    _impl->addSession(session);

    auto prx = session->getSession();
    if(!prx)
    {
        return;
    }

    //
    // Initialize keys.
    //
    DataStormContract::KeyInfoSeq keys;
    if(!_keyElements.empty())
    {
        keys.reserve(_keyElements.size());
        for(const auto& element : _keyElements)
        {
            keys.push_back(element.second->getKeyInfo());
        }
    }

    //
    // Initialize filters.
    //
    vector<string> filters;
    if(!_filteredElements.empty())
    {
        filters.reserve(_filteredElements.size());
        for(const auto& element : _filteredElements)
        {
            filters.push_back(element.first);
        }
    }

    if(!keys.empty() || !filters.empty())
    {
        prx->initKeysAndFiltersAsync(_name, session->getLastId(_name), keys, filters);
    }
}

void
TopicI::detach(SessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    if(_traceLevels->topic > 0)
    {
        Trace out(_traceLevels, _traceLevels->topicCat);
        out << "detached session `" << session << "' from topic `" << this << "'";
    }
    _impl->removeSession(session);
}

void
TopicI::initKeysAndFilters(const DataStormContract::KeyInfoSeq& keys, const DataStormContract::StringSeq& filters,
                           long long int lastId, SessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    keysAndFiltersAttached(keys, filters, lastId, session);
}

void
TopicI::attachKey(const DataStormContract::KeyInfo& key, long long int lastId, SessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    keyAttached(key, lastId, session);
}

void
TopicI::detachKey(const DataStormContract::Key& key, SessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    keyDetached(key, session);
}

void
TopicI::attachFilter(const string& filter, long long int lastId, SessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    filterAttached(filter, lastId, session);
}

void
TopicI::detachFilter(const string& filter, SessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    filterDetached(filter, session);
}

shared_ptr<TopicSubscriber>
TopicI::getSubscriber() const
{
    return dynamic_pointer_cast<TopicSubscriber>(_impl);
}

shared_ptr<TopicPublisher>
TopicI::getPublisher() const
{
    return dynamic_pointer_cast<TopicPublisher>(_impl);
}

TopicReaderI::TopicReaderI(shared_ptr<TopicFactoryI> factory, shared_ptr<KeyFactory> keyFactory, const string& name,
                           SubscriberI* subscriber) :
    TopicI(factory, keyFactory, name)
{
    _impl = subscriber->createTopicSubscriber(this);
}

shared_ptr<DataReader>
TopicReaderI::getFilteredDataReader(const string& filter)
{
    lock_guard<mutex> lock(_mutex);
    auto reader = getFiltered<FilteredDataReaderI>(filter);
    _impl->forEachPeerKey([&](const shared_ptr<Key>& key) { _elements[key].insert(reader); }, filter);
    return reader;
}

shared_ptr<DataReader>
TopicReaderI::getDataReader(const shared_ptr<Key>& key)
{
    lock_guard<mutex> lock(_mutex);
    auto reader = get<KeyDataReaderI>(key);
    _elements[key].insert(reader);
    return reader;
}

void
TopicReaderI::destroy()
{
    TopicI::destroy();

    auto factory = _factory.lock();
    if(factory)
    {
        factory->removeTopicReader(_name);
    }
}

void
TopicReaderI::removeFiltered(const string& filter, const shared_ptr<FilteredDataReaderI>& reader)
{
    lock_guard<mutex> lock(_mutex);
    _filteredElements.erase(filter);
    _impl->forEachPeerKey([&](const shared_ptr<Key>& key) { _elements[key].erase(reader); }, filter);
}

void
TopicReaderI::remove(const shared_ptr<Key>& key, const shared_ptr<KeyDataReaderI>& reader)
{
    lock_guard<mutex> lock(_mutex);
    _keyElements.erase(key);
    _elements[key].erase(reader);
}

void
TopicReaderI::keysAndFiltersAttached(const DataStormContract::KeyInfoSeq& keys,
                                     const DataStormContract::StringSeq& filters,
                                     long long int,
                                     SessionI* session)
{
    vector<DataStormContract::Key> k;
    for(const auto& info : keys)
    {
        shared_ptr<Key> key = _keyFactory->unmarshal(info.key);
        if(_impl->addKeyListener(key, session))
        {
            for(const auto& element : _filteredElements)
            {
                if(key->match(element.first))
                {
                    _elements[key].insert(dynamic_pointer_cast<DataReaderI>(element.second));
                    k.push_back(info.key);
                }
            }
        }
    }
    for(auto filter : filters)
    {
        _impl->addFilteredListener(filter, session);
    }
}

void
TopicReaderI::keyAttached(const DataStormContract::KeyInfo& info, long long int, SessionI* session)
{
    shared_ptr<Key> key = _keyFactory->unmarshal(info.key);
    if(_impl->addKeyListener(key, session))
    {
        for(const auto& element : _filteredElements)
        {
            if(key->match(element.first))
            {
                _elements[key].insert(dynamic_pointer_cast<DataReaderI>(element.second));
            }
        }
    }
}

void
TopicReaderI::keyDetached(const DataStormContract::Key& k, SessionI* session)
{
    shared_ptr<Key> key = _keyFactory->unmarshal(k);
    _impl->removeKeyListener(key, session);
    for(const auto& element : _filteredElements)
    {
        if(key->match(element.first))
        {
            _elements[key].erase(dynamic_pointer_cast<DataReaderI>(element.second));
        }
    }
}

void
TopicReaderI::filterAttached(const string& filter, long long int, SessionI* session)
{
    _impl->addFilteredListener(filter, session);
}

void
TopicReaderI::filterDetached(const string& filter, SessionI* session)
{
    _impl->removeFilteredListener(filter, session);
}

void
TopicReaderI::queue(const DataStormContract::Key& k, const DataStormContract::DataSampleSeq& samples,
                    SubscriberSessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    shared_ptr<Key> key = _keyFactory->unmarshal(k);
    auto p = _elements.find(key);
    if(p == _elements.end())
    {
        return;
    }

    for(const auto& s : samples)
    {
        if(session->setLastId(_name, s->id))
        {
            auto sample = make_shared<SampleI>(key, s);
            for(auto& reader : p->second)
            {
                reader->queue(sample);
            }
        }
    }
    _cond.notify_all();
}

void
TopicReaderI::queue(const DataStormContract::Key& k, const shared_ptr<DataStormContract::DataSample>& s,
                    SubscriberSessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    if(session->setLastId(_name, s->id))
    {
        shared_ptr<Key> key = _keyFactory->unmarshal(k);
        auto p = _elements.find(key);
        if(p == _elements.end())
        {
            return;
        }

        auto sample = make_shared<SampleI>(key, s);
        for(const auto& reader : p->second)
        {
            reader->queue(sample);
        }
        _cond.notify_all();
    }
}

void
TopicReaderI::queue(const string& filter, const shared_ptr<DataStormContract::DataSample>& s,
                    SubscriberSessionI* session)
{
    lock_guard<mutex> lock(_mutex);
    if(session->setLastId(_name, s->id))
    {
        for(const auto& element : _elements)
        {
            if(element.first->match(filter))
            {
                auto sample = make_shared<SampleI>(element.first, s);
                for(const auto& reader : element.second)
                {
                    reader->queue(sample);
                }
            }
        }
        _cond.notify_all();
    }
}

TopicWriterI::TopicWriterI(shared_ptr<TopicFactoryI> factory, shared_ptr<KeyFactory> keyFactory, const string& name,
                           PublisherI* publisher) :
    TopicI(factory, keyFactory, name)
{
    _impl = publisher->createTopicPublisher(this);
}

shared_ptr<DataWriter>
TopicWriterI::getFilteredDataWriter(const string& filter)
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
TopicWriterI::removeFiltered(const string& filter, const shared_ptr<FilteredDataWriterI>&)
{
    lock_guard<mutex> lock(_mutex);
    _filteredElements.erase(filter);
}

void
TopicWriterI::remove(const shared_ptr<Key>& key, const shared_ptr<KeyDataWriterI>&)
{
    lock_guard<mutex> lock(_mutex);
    _keyElements.erase(key);
}

void
TopicWriterI::keysAndFiltersAttached(const DataStormContract::KeyInfoSeq& keys,
                                     const DataStormContract::StringSeq& filters,
                                     long long int lastId,
                                     SessionI* session)
{
    DataStormContract::DataSamplesSeq samples;
    auto prx = Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(session->getSession());
    for(const auto& info : keys)
    {
        auto key = _keyFactory->unmarshal(info.key);
        _impl->addKeyListener(key, session);
        if(prx)
        {
            auto q = _keyElements.find(key);
            if(q != _keyElements.end())
            {
                dynamic_pointer_cast<KeyDataWriterI>(q->second)->init(lastId, samples);
            }
        }
    }
    for(auto filter : filters)
    {
        _impl->addFilteredListener(filter, session);
        for(const auto& e : _keyElements)
        {
            if(e.first->match(filter))
            {
                dynamic_pointer_cast<KeyDataWriterI>(e.second)->init(lastId, samples);
            }
        }
    }
    if(!samples.empty())
    {
        prx->iAsync(_name, samples);
    }
}

void
TopicWriterI::keyAttached(const DataStormContract::KeyInfo& info, long long int lastId, SessionI* session)
{
    auto key = _keyFactory->unmarshal(info.key);
    _impl->addKeyListener(key, session);

    DataStormContract::DataSamplesSeq samples;
    auto prx = Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(session->getSession());
    if(prx)
    {
        //
        // Send samples to the reader
        //
        auto p = _keyElements.find(key);
        if(p != _keyElements.end())
        {
            dynamic_pointer_cast<KeyDataWriterI>(p->second)->init(lastId, samples);
        }
    }
    if(!samples.empty())
    {
        prx->iAsync(_name, samples);
    }
}

void
TopicWriterI::keyDetached(const DataStormContract::Key& key, SessionI* session)
{
    _impl->removeKeyListener(_keyFactory->unmarshal(key), session);
}

void
TopicWriterI::filterAttached(const string& filter, long long int lastId, SessionI* session)
{
    _impl->addFilteredListener(filter, session);

    auto prx = Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(session->getSession());
    if(prx)
    {
        DataStormContract::DataSamplesSeq samples;
        for(const auto& e : _keyElements)
        {
            if(e.first->match(filter))
            {
                dynamic_pointer_cast<KeyDataWriterI>(e.second)->init(lastId, samples);
            }
        }
        if(!samples.empty())
        {
            prx->iAsync(_name, samples);
        }
    }
}

void
TopicWriterI::filterDetached(const string& filter, SessionI* session)
{
    _impl->removeFilteredListener(filter, session);
}

TopicFactoryI::TopicFactoryI(shared_ptr<Ice::Communicator> communicator)
{
    _instance = make_shared<Instance>(communicator);
    _traceLevels = _instance->getTraceLevels();
}

void
TopicFactoryI::init(weak_ptr<DataStorm::TopicFactory> factory)
{
    _instance->init(factory, shared_from_this());
}

shared_ptr<TopicReader>
TopicFactoryI::createTopicReader(const string& name, shared_ptr<KeyFactory> keyFactory)
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
        reader = make_shared<TopicReaderI>(shared_from_this(), keyFactory, name, _subscriber.get());
        _readers.insert(make_pair(name, reader));
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << "added topic reader `" << name << "'";
        }
    }
    _subscriber->getForwarder()->addTopicAsync(name);
    _subscriber->checkSessions(reader);
    return reader;
}

shared_ptr<TopicWriter>
TopicFactoryI::createTopicWriter(const string& name, shared_ptr<KeyFactory> keyFactory)
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
        writer = make_shared<TopicWriterI>(shared_from_this(), keyFactory, name, _publisher.get());
        _writers.insert(make_pair(name, writer));
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << "added topic writer `" << name << "'";
        }
    }
    _publisher->getForwarder()->addTopicAsync(name);
    _publisher->checkSessions(writer);
    return writer;
}

void
TopicFactoryI::waitForShutdown()
{
    _instance->getCommunicator()->waitForShutdown();
}

void
TopicFactoryI::shutdown()
{
    _instance->getCommunicator()->shutdown();
}

void
TopicFactoryI::destroy()
{
    _instance->getCommunicator()->destroy();
}

void
TopicFactoryI::removeTopicReader(const string& name)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << "removed topic reader `" << name << "'";
        }
        _readers.erase(name);
    }
    _subscriber->getForwarder()->removeTopicAsync(name);
}

void
TopicFactoryI::removeTopicWriter(const string& name)
{
    {
        lock_guard<mutex> lock(_mutex);
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << "removed topic writer `" << name << "'";
        }
        _writers.erase(name);
    }
    _publisher->getForwarder()->removeTopicAsync(name);
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
TopicFactoryI::createSession(const string& topic, shared_ptr<DataStormContract::PublisherPrx> publisher)
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
TopicFactoryI::createSession(const string& topic, shared_ptr<DataStormContract::SubscriberPrx> subscriber)
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

vector<string>
TopicFactoryI::getTopicReaders() const
{
    lock_guard<mutex> lock(_mutex);
    vector<string> readers;
    readers.reserve(_readers.size());
    for(const auto& p : _readers)
    {
        readers.push_back(p.first);
    }
    return readers;
}

vector<string>
TopicFactoryI::getTopicWriters() const
{
    lock_guard<mutex> lock(_mutex);
    vector<string> writers;
    writers.reserve(_writers.size());
    for(const auto& p : _writers)
    {
        writers.push_back(p.first);
    }
    return writers;
}

shared_ptr<Ice::Communicator>
TopicFactoryI::getCommunicator() const
{
    return _instance->getCommunicator();
}

shared_ptr<TopicFactory>
DataStormInternal::createTopicFactory(shared_ptr<Ice::Communicator> communicator)
{
    return make_shared<TopicFactoryI>(communicator);
}
