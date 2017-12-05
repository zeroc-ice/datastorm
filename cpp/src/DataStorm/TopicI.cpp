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

int
toInt(const std::string& v, int value = 0)
{
    istringstream is(v);
    is >> value;
    return value;
}

static Topic::Updater noOpUpdater = [](const shared_ptr<Sample>& previous,
                                       const shared_ptr<Sample>& next,
                                       const shared_ptr<Ice::Communicator>&)
{
    next->setValue(previous);
};

// The always match filter always matches the value, it's used by the any key reader/writer.
class AlwaysMatchFilter : public Filter
{
public:

    virtual std::string toString() const
    {
        return "f1:alwaysmatch";
    }

    virtual std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>&) const
    {
        return std::vector<unsigned char>();
    }

    virtual long long int getId() const
    {
        return 1; // 1 is reserved for the match all filter.
    }

    virtual bool match(const std::shared_ptr<Filterable>&) const
    {
        return true;
    }
};
const auto alwaysMatchFilter = make_shared<AlwaysMatchFilter>();

}

TopicI::TopicI(const weak_ptr<TopicFactoryI>& factory,
               const shared_ptr<KeyFactory>& keyFactory,
               const shared_ptr<FilterFactory>& filterFactory,
               const shared_ptr<TagFactory>& tagFactory,
               const shared_ptr<SampleFactory>& sampleFactory,
               const string& name,
               long long int id) :
    _factory(factory),
    _keyFactory(keyFactory),
    _filterFactory(filterFactory),
    _tagFactory(tagFactory),
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
    spec.tags = getTags();
    return spec;
}

ElementInfoSeq
TopicI::getTags() const
{
    ElementInfoSeq tags;
    tags.reserve(_updaters.size());
    for(auto u : _updaters)
    {
        tags.push_back({ u.first->getId(), u.first->encode(_instance->getCommunicator()) });
    }
    return tags;
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
                    elements.push_back({ k->getId(), k->getConfig() });
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
                        elements.push_back({ f->getId(), f->getConfig() });
                    }
                    specs.push_back({ move(elements),
                                      -e.first->getId(),
                                      e.first->encode(_instance->getCommunicator()),
                                      info.valueId });
                }
            }
        }
        else
        {
            shared_ptr<Filter> filter;
            if(info.value.empty())
            {
                filter = alwaysMatchFilter;
            }
            else if(!_filterFactory)
            {
                return specs;
            }
            else
            {
                filter = _filterFactory->decode(_instance->getCommunicator(), info.value);
            }

            for(auto e : _keyElements)
            {
                if(filter->match(e.first))
                {
                    ElementDataSeq elements;
                    for(auto k : e.second)
                    {
                        elements.push_back({ k->getId(), k->getConfig() });
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
                       const ElementSpecSeq& elements,
                       SessionI* session,
                       const shared_ptr<SessionPrx>& prx,
                       const chrono::time_point<chrono::system_clock>& now)
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
                    if(spec.value.empty())
                    {
                        filter = alwaysMatchFilter;
                    }
                    else if(!_filterFactory)
                    {
                        continue; // TODO: better handle this inconsistency? Warn?
                    }
                    else
                    {
                        filter = _filterFactory->decode(_instance->getCommunicator(), spec.value);
                    }
                }
                for(auto e : p->second)
                {
                    ElementDataAckSeq acks;
                    for(const auto& data : spec.elements)
                    {
                        if(spec.valueId > 0) // Key
                        {
                            e->attach(topicId, key, nullptr, session, prx, data, now, acks);
                        }
                        else if(filter->match(key))
                        {
                            e->attach(topicId, nullptr, filter, session, prx, data, now, acks);
                        }
                    }
                    if(!acks.empty())
                    {
                        specs.push_back({ move(acks),
                                          key->getId(),
                                          spec.valueId < 0 ? key->encode(_instance->getCommunicator()) : ByteSeq(),
                                          spec.valueId });
                    }
                }
            }
        }
        else
        {
            shared_ptr<Filter> filter;
            if(spec.peerValueId == -1)
            {
                filter = alwaysMatchFilter;
            }
            else if(!_filterFactory)
            {
                return specs;
            }
            else
            {
                filter = _filterFactory->get(-spec.peerValueId);
            }

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
                    ElementDataAckSeq acks;
                    for(const auto& data : spec.elements)
                    {
                        if(spec.valueId < 0) // Filter
                        {
                            e->attach(topicId, nullptr, filter, session, prx, data, now, acks);
                        }
                        else if(filter->match(key))
                        {
                            e->attach(topicId, key, nullptr, session, prx, data, now, acks);
                        }
                    }
                    if(!acks.empty())
                    {
                        specs.push_back({ move(acks),
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
                          const ElementSpecAckSeq& elements,
                          SessionI* session,
                          const shared_ptr<SessionPrx>& prx,
                          const chrono::time_point<chrono::system_clock>& now)
{
    DataSamplesSeq samples;
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
                    if(spec.value.empty())
                    {
                        filter = alwaysMatchFilter;
                    }
                    else if(!_filterFactory)
                    {
                        continue; // TODO: better handle this inconsistency? Warn?
                    }
                    else
                    {
                        filter = _filterFactory->decode(_instance->getCommunicator(), spec.value);
                    }
                }

                vector<std::shared_ptr<Sample>> samplesI;
                for(auto e : p->second)
                {
                    for(const auto& data : spec.elements)
                    {
                        if(data.peerId == e->getId())
                        {
                            if(spec.valueId > 0) // Key
                            {
                                e->attach(topicId, key, nullptr, session, prx, data, now, samples);
                            }
                            else if(filter->match(key)) // Filter
                            {
                                e->attach(topicId, nullptr, filter, session, prx, data, now, samples);
                            }
                            break;
                        }
                    }
                }
            }
        }
        else // Filter
        {
            shared_ptr<Filter> filter;
            if(spec.peerValueId == -1)
            {
                filter = alwaysMatchFilter;
            }
            else if(!_filterFactory)
            {
                return samples;
            }
            else
            {
                filter = _filterFactory->get(-spec.peerValueId);
            }

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
                    for(const auto& data : spec.elements)
                    {
                        if(data.peerId == e->getId())
                        {
                            if(spec.valueId < 0) // Filter
                            {
                                e->attach(topicId, nullptr, filter, session, prx, data, now, samples);
                            }
                            else if(filter->match(key))
                            {
                                e->attach(topicId, key, nullptr, session, prx, data, now, samples);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
    return samples;
}

void
TopicI::queue(const shared_ptr<DataElementI>& element, function<void()> callback)
{
    _callbackQueue.push_back(make_pair(element, callback));
}

void
TopicI::flushQueue()
{
    for(auto p : _callbackQueue)
    {
        try
        {
            p.second();
        }
        catch(...)
        {
            assert(false); // TODO: XXX
        }
    }
    _callbackQueue.clear();
}

void
TopicI::setUpdater(const shared_ptr<Tag>& tag, Updater updater)
{
    unique_lock<mutex> lock(_mutex);
    if(updater)
    {
        _updaters[tag] = updater;
        _forwarder->attachTags(_id, { { tag->getId(), tag->encode(_instance->getCommunicator()) } });
    }
    else
    {
        _updaters.erase(tag);
        _forwarder->detachTags(_id, { tag->getId() });
    }
}

const Topic::Updater&
TopicI::getUpdater(const shared_ptr<Tag>& tag) const
{
    // Called with mutex locked
    auto p = _updaters.find(tag);
    if(p != _updaters.end())
    {
        return p->second;
    }
    return noOpUpdater;
}

void
TopicI::setUpdaters(map<shared_ptr<Tag>, Updater> updaters)
{
    unique_lock<mutex> lock(_mutex);
    _updaters = move(updaters);
}

map<shared_ptr<Tag>, Topic::Updater>
TopicI::getUpdaters() const
{
    unique_lock<mutex> lock(_mutex);
    return _updaters;
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
        else if(count >= 0 && _listenerCount >= static_cast<size_t>(count))
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
TopicI::add(const shared_ptr<DataElementI>& element, const vector<shared_ptr<Key>>& keys)
{
    ElementInfoSeq infos;
    for(const auto& key : keys)
    {
        auto p = _keyElements.find(key);
        if(p == _keyElements.end())
        {
            p = _keyElements.emplace(key, set<shared_ptr<DataElementI>>()).first;
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
TopicI::addFiltered(const shared_ptr<DataElementI>& element, const shared_ptr<Filter>& filter)
{
    auto p = _filteredElements.find(filter);
    if(p == _filteredElements.end())
    {
        p = _filteredElements.emplace(filter, set<shared_ptr<DataElementI>>()).first;
    }
    assert(element);
    p->second.insert(element);
    _forwarder->announceElements(_id, { { -filter->getId(), filter->encode(_instance->getCommunicator()) } });
}

void
TopicI::parseConfigImpl(const Ice::PropertyDict& properties, const string& prefix, DataStorm::Config& config) const
{
    // Set defaults
    config.sampleCount = -1;
    config.sampleLifetime = 0;
    config.clearHistory = DataStorm::ClearHistoryPolicy::OnAll;

    // Override defaults with properties
    auto p = properties.find(prefix + ".SampleLifetime");
    if(p != properties.end())
    {
        config.sampleLifetime = toInt(p->second);
    }
    p = properties.find(prefix + ".SampleCount");
    if(p != properties.end())
    {
        config.sampleCount = toInt(p->second);
    }
    p = properties.find(prefix + ".ClearHistory");
    if(p != properties.end())
    {
        if(p->second == "OnAdd")
        {
            config.clearHistory = DataStorm::ClearHistoryPolicy::OnAdd;
        }
        else if(p->second == "OnRemove")
        {
            config.clearHistory = DataStorm::ClearHistoryPolicy::OnRemove;
        }
        else if(p->second == "OnAll")
        {
            config.clearHistory = DataStorm::ClearHistoryPolicy::OnAll;
        }
        else if(p->second == "OnAllExceptPartialUpdate")
        {
            config.clearHistory = DataStorm::ClearHistoryPolicy::OnAllExceptPartialUpdate;
        }
        else if(p->second == "Never")
        {
            config.clearHistory = DataStorm::ClearHistoryPolicy::Never;
        }
    }
}

TopicReaderI::TopicReaderI(const shared_ptr<TopicFactoryI>& factory,
                           const shared_ptr<KeyFactory>& keyFactory,
                           const shared_ptr<FilterFactory>& filterFactory,
                           const shared_ptr<TagFactory>& tagFactory,
                           const shared_ptr<SampleFactory>& sampleFactory,
                           const string& name,
                           long long int id) :
    TopicI(factory, keyFactory, filterFactory, tagFactory, move(sampleFactory), name, id)
{
    _defaultConfig = parseConfig("DataStorm.Topic." + name);
}

shared_ptr<DataReader>
TopicReaderI::createFiltered(const shared_ptr<Filter>& filter,
                             DataStorm::ReaderConfig config,
                             vector<unsigned char> sampleFilter)
{
    lock_guard<mutex> lock(_mutex);
    auto element = make_shared<FilteredDataReaderI>(this, ++_nextFilteredId, filter, move(sampleFilter),
                                                    mergeConfigs(move(config)));
    addFiltered(element, filter);
    return element;
}

shared_ptr<DataReader>
TopicReaderI::create(const vector<shared_ptr<Key>>& keys,
                     DataStorm::ReaderConfig config,
                     vector<unsigned char> sampleFilter)
{
    lock_guard<mutex> lock(_mutex);
    auto element = make_shared<KeyDataReaderI>(this, ++_nextId, keys, move(sampleFilter),
                                               mergeConfigs(move(config)));
    if(keys.empty())
    {
        addFiltered(element, alwaysMatchFilter);
    }
    else
    {
        add(element, keys);
    }
    return element;
}

void
TopicReaderI::setDefaultConfig(DataStorm::ReaderConfig config)
{
    lock_guard<mutex> lock(_mutex);
    _defaultConfig = mergeConfigs(move(config));
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

    auto factory = _factory.lock();
    if(factory)
    {
        factory->removeTopicReader(_name, shared_from_this());
    }
}

void
TopicReaderI::removeFiltered(const shared_ptr<Filter>& filter, const shared_ptr<DataElementI>& element)
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
TopicReaderI::remove(const vector<shared_ptr<Key>>& keys, const shared_ptr<DataElementI>& element)
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

DataStorm::ReaderConfig
TopicReaderI::parseConfig(const string& prefix) const
{
    DataStorm::ReaderConfig config;
    auto properties = _instance->getCommunicator()->getProperties()->getPropertiesForPrefix(prefix);
    parseConfigImpl(properties, prefix, config);
    return config;
}

DataStorm::ReaderConfig
TopicReaderI::mergeConfigs(DataStorm::ReaderConfig config) const
{
    if(!config.sampleCount && _defaultConfig.sampleCount)
    {
        config.sampleCount = _defaultConfig.sampleCount;
    }
    if(!config.sampleLifetime && _defaultConfig.sampleLifetime)
    {
        config.sampleLifetime = _defaultConfig.sampleLifetime;
    }
    if(!config.clearHistory && _defaultConfig.clearHistory)
    {
        config.clearHistory = _defaultConfig.clearHistory;
    }
    if(!config.discardPolicy && _defaultConfig.discardPolicy)
    {
        config.discardPolicy = _defaultConfig.discardPolicy;
    }
    return config;
}

TopicWriterI::TopicWriterI(const shared_ptr<TopicFactoryI>& factory,
                           const shared_ptr<KeyFactory>& keyFactory,
                           const shared_ptr<FilterFactory>& filterFactory,
                           const std::shared_ptr<TagFactory>& tagFactory,
                           const shared_ptr<SampleFactory>& sampleFactory,
                           const string& name,
                           long long int id) :
    TopicI(factory, keyFactory, filterFactory, tagFactory, move(sampleFactory), name, id)
{
    _defaultConfig = parseConfig("DataStorm.Topic." + name);
}

shared_ptr<DataWriter>
TopicWriterI::create(const vector<shared_ptr<Key>>& keys,
                     DataStorm::WriterConfig config,
                     const shared_ptr<FilterFactory>& sampleFilterFactory)
{
    lock_guard<mutex> lock(_mutex);
    auto element = make_shared<KeyDataWriterI>(this, ++_nextId, keys, sampleFilterFactory, mergeConfigs(move(config)));
    if(keys.empty())
    {
        addFiltered(element, alwaysMatchFilter);
    }
    else
    {
        add(element, keys);
    }
    return element;
}

void
TopicWriterI::setDefaultConfig(DataStorm::WriterConfig config)
{
    lock_guard<mutex> lock(_mutex);
    _defaultConfig = mergeConfigs(move(config));
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
TopicWriterI::remove(const vector<shared_ptr<Key>>& keys, const shared_ptr<DataElementI>& element)
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

DataStorm::WriterConfig
TopicWriterI::parseConfig(const string& prefix) const
{
    DataStorm::WriterConfig config;
    auto properties = _instance->getCommunicator()->getProperties()->getPropertiesForPrefix(prefix);
    parseConfigImpl(properties, prefix, config);
    return config;
}

DataStorm::WriterConfig
TopicWriterI::mergeConfigs(DataStorm::WriterConfig config) const
{
    if(!config.sampleCount && _defaultConfig.sampleCount)
    {
        config.sampleCount = _defaultConfig.sampleCount;
    }
    if(!config.sampleLifetime && _defaultConfig.sampleLifetime)
    {
        config.sampleLifetime = _defaultConfig.sampleLifetime;
    }
    if(!config.clearHistory && _defaultConfig.clearHistory)
    {
        config.clearHistory = _defaultConfig.clearHistory;
    }
    if(!config.priority && _defaultConfig.priority)
    {
        config.priority = _defaultConfig.priority;
    }
    return config;
}
