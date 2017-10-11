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
    keys.reserve(_keyElements.size());
    for(const auto& element : _keyElements)
    {
        keys.push_back({ element.first->getId(), element.first->encode(_instance->getCommunicator()) });
    }
    return keys;
}

DataStormContract::FilterInfoSeq
TopicI::getFilterInfoSeq() const
{
    DataStormContract::FilterInfoSeq filters;
    filters.reserve(_filteredElements.size());
    for(const auto& element : _filteredElements)
    {
        filters.push_back({ element.first->getId(), element.first->encode(_instance->getCommunicator()) });
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
    if(_sessions.erase({ id, session })) // Session might already be detached if topic was destroyed.
    {
        session->unsubscribe(id, unsubscribe);
    }
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
            samples.push_back({ k.info.id, move(k.samples) });
        }
    }
    return samples;
}

pair<DataStormContract::KeyInfoAndSamplesSeq, DataStormContract::FilterInfoSeq>
TopicI::attachKeys(long long int id,
                   const DataStormContract::KeyInfoSeq& infos,
                   long long int lastId,
                   SessionI* session,
                   const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    DataStormContract::KeyInfoAndSamplesSeq ackKeys;
    DataStormContract::FilterInfoSeq ackFilters;
    for(const auto& info : infos)
    {
        attachKeyImpl(id, info, {}, lastId, session, prx, ackKeys, ackFilters);
    }
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
    auto key = _keyFactory->decode(_instance->getCommunicator(), info.key);

    vector<shared_ptr<Sample>> samplesI;
    function<void(const shared_ptr<DataElementI>&)> queueSamples = [&](auto reader) {
        if(!samples.empty())
        {
            if(samplesI.empty())
            {
                for(const auto& sample : samples)
                {
                    if(session->setLastId(id, sample.id))
                    {
                        samplesI.push_back(_sampleFactory(sample.type, key, sample.value, sample.timestamp));
                    }
                }
            }
            for(const auto& sample : samplesI)
            {
                reader->queue(sample);
            }
        }
    };

    auto p = _keyElements.find(key);
    if(p != _keyElements.end())
    {
        DataStormContract::DataSampleSeq samples;
        bool attached = false;
        for(auto k : p->second)
        {
            if(k->attachKey(id, info.id, key, session, prx))
            {
                queueSamples(k);
                auto s = k->getSamples(lastId, nullptr);
                samples.insert(samples.end(), s.begin(), s.end());
                attached = true;
            }
        }
        if(attached)
        {
            sort(samples.begin(), samples.end(), [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; });
            ackKeys.push_back({ { p->first->getId(), p->first->encode(_instance->getCommunicator()) }, samples });
        }
    }

    for(const auto& element : _filteredElements)
    {
        if(element.first->match(key))
        {
            bool attached = false;
            for(auto f : element.second)
            {
                if(f->attachKey(id, info.id, key, session, prx))
                {
                    queueSamples(f);
                    attached = true;
                }
            }
            if(attached)
            {
                ackFilters.push_back({ element.first->getId(), element.first->encode(_instance->getCommunicator())} );
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
    auto filter = _filterFactory->decode(_instance->getCommunicator(), info.filter);

    for(const auto& element : _keyElements)
    {
        if(filter->match(element.first))
        {
            DataStormContract::DataSampleSeq samples;
            bool attached = false;
            for(auto k : element.second)
            {
                if(k->attachFilter(id, info.id, filter, session, prx))
                {
                    auto s = k->getSamples(lastId, filter);
                    samples.insert(samples.end(), s.begin(), s.end());
                    attached = true;
                }
            }
            if(attached)
            {
                sort(samples.begin(), samples.end(), [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; });
                ack.push_back({ { element.first->getId(), element.first->encode(_instance->getCommunicator()) }, samples });
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
