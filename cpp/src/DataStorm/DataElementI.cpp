// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/DataElementI.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TraceUtil.h>
#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormInternal;
using namespace DataStormContract;

namespace
{

DataSample
toSample(const shared_ptr<Sample>& sample, const shared_ptr<Ice::Communicator>& communicator, bool marshalKey)
{
    return { sample->id,
             marshalKey ? 0 : sample->key->getId(),
             marshalKey ? sample->key->encode(communicator) : Ice::ByteSeq {},
             chrono::time_point_cast<chrono::microseconds>(sample->timestamp).time_since_epoch().count(),
             sample->tag ? sample->tag->getId() : 0,
             sample->event,
             sample->encode(communicator) };
}

void
cleanOldSamples(deque<shared_ptr<Sample>>& samples,
                const chrono::time_point<chrono::system_clock>& now,
                int lifetime)
{
    chrono::time_point<chrono::system_clock> staleTime = now - chrono::milliseconds(lifetime);
    auto p = stable_partition(samples.begin(), samples.end(),
                              [&](const shared_ptr<Sample>& s) { return s->timestamp < staleTime; });
    if(p != samples.begin())
    {
        samples.erase(samples.begin(), p);
    }
}

}

DataElementI::DataElementI(TopicI* parent, long long int id, const DataStorm::Config& config) :
    _traceLevels(parent->getInstance()->getTraceLevels()),
    _id(id),
    _listenerCount(0),
    _config(make_shared<ElementConfig>()),
    _executor(parent->getInstance()->getCallbackExecutor()),
    _parent(parent->shared_from_this()),
    _waiters(0),
    _notified(0)
{
    if(config.sampleCount)
    {
        _config->sampleCount = *config.sampleCount;
    }
    if(config.sampleLifetime)
    {
        _config->sampleLifetime = *config.sampleLifetime;
    }
    if(config.clearHistory)
    {
        _config->clearHistory = static_cast<ClearHistoryPolicy>(*config.clearHistory);
    }
}

void
DataElementI::init()
{
    _forwarder = Ice::uncheckedCast<SessionPrx>(_parent->getInstance()->getForwarderManager()->add(shared_from_this()));
}

DataElementI::~DataElementI()
{
    assert(_listeners.empty());
    assert(_listenerCount == 0);
}

void
DataElementI::destroy()
{
    {
        unique_lock<mutex> lock(_parent->_mutex);
        destroyImpl(); // Must be called first.
    }
    disconnect();
    _parent->getInstance()->getForwarderManager()->remove(_forwarder->ice_getIdentity());
}

void
DataElementI::attach(long long int topicId,
                     long long int id,
                     const shared_ptr<Key>& key,
                     const shared_ptr<Filter>& filter,
                     SessionI* session,
                     const shared_ptr<SessionPrx>& prx,
                     const ElementData& data,
                     const chrono::time_point<chrono::system_clock>& now,
                     ElementDataAckSeq& acks)
{
    shared_ptr<Filter> sampleFilter;
    if(data.config->sampleFilter)
    {
        auto info = *data.config->sampleFilter;
        sampleFilter = _parent->getSampleFilterFactories()->decode(getCommunicator(), info.name, info.criteria);
    }
    string facet = data.config->facet ? *data.config->facet : string();
    if((id > 0 && attachKey(topicId, id, data.id, key, sampleFilter, session, prx, facet)) ||
       (id < 0 && attachFilter(topicId, id, data.id, filter, sampleFilter, session, prx, facet)))
    {
        auto q = data.lastIds.find(_id);
        long long lastId = q != data.lastIds.end() ? q->second : 0;
        LongLongDict lastIds = key ? session->getLastIds(topicId, id, shared_from_this()) : LongLongDict();
        DataSamples samples = getSamples(key, sampleFilter, data.config, lastId, now);
        acks.push_back({ _id, _config, lastIds, samples.samples, data.id });
    }
}

void
DataElementI::attach(long long int topicId,
                     long long int id,
                     const shared_ptr<Key>& key,
                     const shared_ptr<Filter>& filter,
                     SessionI* session,
                     const shared_ptr<SessionPrx>& prx,
                     const ElementDataAck& data,
                     const chrono::time_point<chrono::system_clock>& now,
                     DataSamplesSeq& samples)
{
    shared_ptr<Filter> sampleFilter;
    if(data.config->sampleFilter)
    {
        auto info = *data.config->sampleFilter;
        sampleFilter = _parent->getSampleFilterFactories()->decode(getCommunicator(), info.name, info.criteria);
    }
    string facet = data.config->facet ? *data.config->facet : string();
    if((id > 0 && attachKey(topicId, id, data.id, key, sampleFilter, session, prx, facet)) ||
       (id < 0 && attachFilter(topicId, id, data.id, filter, sampleFilter, session, prx, facet)))
    {
        auto q = data.lastIds.find(_id);
        long long lastId = q != data.lastIds.end() ? q->second : 0;
        samples.push_back(getSamples(key, sampleFilter, data.config, lastId, now));
    }
    auto samplesI = session->subscriberInitialized(topicId, id > 0 ? data.id : -data.id, data.samples, key, shared_from_this());
    if(!samplesI.empty())
    {
        initSamples(samplesI, topicId, data.id, now, id < 0);
    }
}

bool
DataElementI::attachKey(long long int topicId,
                        long long int keyId,
                        long long int elementId,
                        const shared_ptr<Key>& key,
                        const shared_ptr<Filter>& sampleFilter,
                        SessionI* session,
                        const shared_ptr<SessionPrx>& prx,
                        const string& facet)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": attachKey element " << elementId << "@" << topicId << " " << key;
        if(!facet.empty())
        {
            out << " (facet = " << facet << ")";
        }
    }

    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, facet }, Listener(prx, facet)).first;
    }
    if(p->second.keys.add(topicId, elementId, key, sampleFilter))
    {
        ++_listenerCount;
        session->subscribeToKey(topicId, keyId, elementId, key, shared_from_this(), facet);
        notifyListenerWaiters(session->getTopicLock());
        if(_onConnect)
        {
            _executor->queue(shared_from_this(), [=] { _onConnect(make_tuple(session->getId(), topicId, elementId)); });
        }
        return true;
    }
    else
    {
        return false;
    }
}

void
DataElementI::detachKey(long long int topicId,
                        long long int elementId,
                        const shared_ptr<Key>& key,
                        SessionI* session,
                        const string& facet)
{

    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": detachKey element " << elementId << "@" << topicId << " " << key;
        if(!facet.empty())
        {
            out << " (facet = " << facet << ")";
        }
    }

    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p != _listeners.end() && p->second.keys.remove(topicId, elementId, key))
    {
        --_listenerCount;
        if(_onDisconnect)
        {
            _executor->queue(shared_from_this(), [=] { _onDisconnect(make_tuple(session->getId(), topicId, elementId)); });
        }
        notifyListenerWaiters(session->getTopicLock());
        if(p->second.empty())
        {
            _listeners.erase(p);
        }
    }
}

bool
DataElementI::attachFilter(long long int topicId,
                           long long int filterId,
                           long long int elementId,
                           const shared_ptr<Filter>& filter,
                           const shared_ptr<Filter>& sampleFilter,
                           SessionI* session,
                           const shared_ptr<SessionPrx>& prx,
                           const string& facet)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": attachFilter element " << elementId << "@" << topicId << " " << filter;
        if(!facet.empty())
        {
            out << " (facet = " << facet << ")";
        }
    }

    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, facet }, Listener(prx, facet)).first;
    }
    if(p->second.filters.add(topicId, elementId, filter, sampleFilter))
    {
        ++_listenerCount;
        session->subscribeToFilter(topicId, filterId, elementId, filter, shared_from_this(), facet);
        if(_onConnect)
        {
            _executor->queue(shared_from_this(), [=] { _onConnect(make_tuple(session->getId(), topicId, elementId)); });
        }
        notifyListenerWaiters(session->getTopicLock());
        return true;
    }
    else
    {
        return false;
    }
}

void
DataElementI::detachFilter(long long int topicId,
                           long long int elementId,
                           const shared_ptr<Filter>& filter,
                           SessionI* session,
                           const string& facet)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": detachFilter element " << elementId << "@" << topicId << " " << filter;
        if(!facet.empty())
        {
            out << " (facet = " << facet << ")";
        }
    }

    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p != _listeners.end() && p->second.filters.remove(topicId, elementId, filter))
    {
        --_listenerCount;
        if(_onDisconnect)
        {
            _executor->queue(shared_from_this(), [=] { _onDisconnect(make_tuple(session->getId(), topicId, elementId)); });
        }
        notifyListenerWaiters(session->getTopicLock());
        if(p->second.empty())
        {
            _listeners.erase(p);
        }
    }
}

void
DataElementI::onConnect(function<void(tuple<string, long long int, long long int>)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onConnect = move(callback);
}

void
DataElementI::onDisconnect(function<void(tuple<string, long long int, long long int>)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onDisconnect = move(callback);
}

vector<shared_ptr<Key>>
DataElementI::getConnectedKeys() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    set<shared_ptr<Key>> keys;
    for(const auto& listener : _listeners)
    {
        for(const auto& key : listener.second.keys.subscribers)
        {
            keys.insert(key.second.elements.begin(), key.second.elements.end());
        }
    }
    return vector<shared_ptr<Key>>(keys.begin(), keys.end());
}

void
DataElementI::initSamples(const vector<shared_ptr<Sample>>&,
                          long long int,
                          long long int,
                          const chrono::time_point<chrono::system_clock>&,
                          bool)
{
}

DataSamples
DataElementI::getSamples(const shared_ptr<Key>&,
                         const shared_ptr<Filter>&,
                         const shared_ptr<DataStormContract::ElementConfig>&,
                         long long int,
                         const chrono::time_point<chrono::system_clock>&)
{
    return {};
}

void
DataElementI::queue(const shared_ptr<Sample>&, const string&, const chrono::time_point<chrono::system_clock>&, bool)
{
    assert(false);
}

shared_ptr<DataStormContract::ElementConfig>
DataElementI::getConfig() const
{
    return _config;
}

void
DataElementI::waitForListeners(int count) const
{
    unique_lock<mutex> lock(_parent->_mutex);
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
        _parent->_cond.wait(lock);
        ++_notified;
    }
}

bool
DataElementI::hasListeners() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    return _listenerCount > 0;
}

shared_ptr<Ice::Communicator>
DataElementI::getCommunicator() const
{
    return _parent->getInstance()->getCommunicator();
}

void
DataElementI::notifyListenerWaiters(unique_lock<mutex>& lock) const
{
    if(_waiters > 0)
    {
        _notified = 0;
        _parent->_cond.notify_all();
        _parent->_cond.wait(lock, [&]() { return _notified < _waiters; }); // Wait until all the waiters are notified.
    }
}

void
DataElementI::disconnect()
{
    map<ListenerKey, Listener> listeners;
    {
        unique_lock<mutex> lock(_parent->_mutex);
        listeners.swap(_listeners);
        notifyListenerWaiters(lock);
        _listenerCount = 0;
    }
    for(const auto& listener : listeners)
    {
        for(const auto& ks : listener.second.keys.subscribers)
        {
            listener.first.session->disconnectFromKey(ks.first.first, ks.first.second, shared_from_this());
        }
        for(const auto& fs : listener.second.filters.subscribers)
        {
            listener.first.session->disconnectFromFilter(fs.first.first, fs.first.second, shared_from_this());
        }
    }
}

void
DataElementI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    for(const auto& listener : _listeners)
    {
        if(!_sample || listener.second.matchOne(_sample)) // If there's at least one subscriber interested in the update
        {
            listener.second.proxy->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
        }
    }
}

DataReaderI::DataReaderI(TopicReaderI* topic, long long int id, const string& sampleFilter,
                         vector<unsigned char> sampleFilterCriteria, const DataStorm::ReaderConfig& config) :
    DataElementI(topic, id, config),
    _parent(topic),
    _discardPolicy(config.discardPolicy ? *config.discardPolicy : DataStorm::DiscardPolicy::None)
{
    if(!sampleFilter.empty())
    {
        _config->sampleFilter = FilterInfo { sampleFilter, move(sampleFilterCriteria) };
    }
}

int
DataReaderI::getInstanceCount() const
{
    lock_guard<mutex> lock(_parent->_mutex);
    return _instanceCount;
}

vector<shared_ptr<Sample>>
DataReaderI::getAllUnread()
{
    lock_guard<mutex> lock(_parent->_mutex);
    vector<shared_ptr<Sample>> unread(_samples.begin(), _samples.end());
    _samples.clear();
    return unread;
}

void
DataReaderI::waitForUnread(unsigned int count) const
{
    unique_lock<mutex> lock(_parent->_mutex);
    while(_samples.size() < count)
    {
        _parent->_cond.wait(lock);
    }
}

bool
DataReaderI::hasUnread() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    return !_samples.empty();
}

shared_ptr<Sample>
DataReaderI::getNextUnread()
{
    unique_lock<mutex> lock(_parent->_mutex);
    _parent->_cond.wait(lock, [&]() { return !_samples.empty(); });
    shared_ptr<Sample> sample = _samples.front();
    _samples.pop_front();
    return sample;
}

void
DataReaderI::initSamples(const vector<shared_ptr<Sample>>& samples,
                         long long int topic,
                         long long int element,
                         const chrono::time_point<chrono::system_clock>& now,
                         bool checkKey)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": initialized " << samples.size() << " samples from `" << element << '@' << topic << "'";
    }

    vector<shared_ptr<Sample>> valid;
    shared_ptr<Sample> previous = !_samples.empty() ? _samples.back() : nullptr;
    for(const auto& sample : samples)
    {
        if(checkKey && !matchKey(sample->key))
        {
            continue;
        }
        else if(_discardPolicy == DataStorm::DiscardPolicy::SendTime && sample->timestamp <= _lastSendTime)
        {
            continue;
        }
        valid.push_back(sample);

        if(sample->event == DataStorm::SampleEvent::PartialUpdate)
        {
            if(!sample->hasValue())
            {
                _parent->getUpdater(sample->tag)(previous, sample, _parent->getInstance()->getCommunicator());
            }
        }
        else
        {
            sample->decode(_parent->getInstance()->getCommunicator());
        }
        previous = sample;
    }

    if(_traceLevels->data > 2 && valid.size() < samples.size())
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": discarded " << samples.size() - valid.size() << " samples from `" << element << '@' << topic << "'";
    }

    if(_onInit)
    {
        _executor->queue(shared_from_this(), [this, valid] { _onInit(valid); });
    }

    if(valid.empty())
    {
        return;
    }
    _lastSendTime = valid.back()->timestamp;

    if(_config->sampleLifetime && *_config->sampleLifetime > 0)
    {
        cleanOldSamples(_samples, now, *_config->sampleLifetime);
    }

    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            size_t count = _samples.size();
            size_t maxCount = static_cast<size_t>(*_config->sampleCount);
            if(count + valid.size() > maxCount)
            {
                count = count + valid.size() - maxCount;
                while(!_samples.empty() && count-- > 0)
                {
                    _samples.pop_front();
                }
                assert(_samples.size() + valid.size() == maxCount);
            }
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }

    if(_config->clearHistory && *_config->clearHistory == ClearHistoryPolicy::OnAll)
    {
        _samples.clear();
        _samples.push_back(valid.back());
    }
    else
    {
        for(const auto& s : valid)
        {
            if(_config->clearHistory &&
               ((s->event == DataStorm::SampleEvent::Add && *_config->clearHistory == ClearHistoryPolicy::OnAdd) ||
                (s->event == DataStorm::SampleEvent::Remove && *_config->clearHistory == ClearHistoryPolicy::OnRemove) ||
                (s->event != DataStorm::SampleEvent::PartialUpdate &&
                 *_config->clearHistory == ClearHistoryPolicy::OnAllExceptPartialUpdate)))
            {
                _samples.clear();
            }
            _samples.push_back(s);
        }
    }

    _parent->_cond.notify_all();
}

void
DataReaderI::queue(const shared_ptr<Sample>& sample,
                   const string& facet,
                   const chrono::time_point<chrono::system_clock>& now,
                   bool checkKey)
{
    if(_config->facet && *_config->facet != facet)
    {
        if(_traceLevels->data > 1)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": skipped sample " << sample->id << " (facet doesn't match)";
        }
        return;
    }
    else if(checkKey && !matchKey(sample->key))
    {
        if(_traceLevels->data > 1)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": skipped sample " << sample->id << " (key doesn't match)";
        }
        return;
    }

    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": queued sample " << sample->id << " listeners=" << _listenerCount;
    }

    if(!sample->hasValue())
    {
        if(sample->event == DataStorm::SampleEvent::PartialUpdate)
        {
            shared_ptr<Sample> previous = !_samples.empty() ? _samples.back() : nullptr;
            _parent->getUpdater(sample->tag)(previous, sample, _parent->getInstance()->getCommunicator());
        }
        else
        {
            sample->decode(_parent->getInstance()->getCommunicator());
        }
    }

    if(_discardPolicy == DataStorm::DiscardPolicy::SendTime && sample->timestamp <= _lastSendTime)
    {
        if(_traceLevels->data > 2)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": discarded sample " << sample->id;
        }
        return;
    }
    _lastSendTime = sample->timestamp;

    if(_onSample)
    {
        _executor->queue(shared_from_this(), [this, sample] { _onSample(sample); });
    }

    if(_config->sampleLifetime && *_config->sampleLifetime > 0)
    {
        cleanOldSamples(_samples, now, *_config->sampleLifetime);
    }

    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            size_t count = _samples.size();
            size_t maxCount = static_cast<size_t>(*_config->sampleCount);
            if(count + 1 > maxCount)
            {
                if(!_samples.empty())
                {
                    _samples.pop_front();
                }
                assert(_samples.size() + 1 == maxCount);
            }
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }

    if(_config->clearHistory &&
       (*_config->clearHistory == ClearHistoryPolicy::OnAll ||
        (sample->event == DataStorm::SampleEvent::Add && *_config->clearHistory == ClearHistoryPolicy::OnAdd) ||
        (sample->event == DataStorm::SampleEvent::Remove && *_config->clearHistory == ClearHistoryPolicy::OnRemove) ||
        (sample->event != DataStorm::SampleEvent::PartialUpdate &&
         *_config->clearHistory == ClearHistoryPolicy::OnAllExceptPartialUpdate)))
    {
        _samples.clear();
    }
    _samples.push_back(sample);
    _parent->_cond.notify_all();
}

void
DataReaderI::onInit(function<void(const vector<shared_ptr<Sample>>&)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onInit = move(callback);
}

void
DataReaderI::onSample(function<void(const shared_ptr<Sample>&)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onSample = move(callback);
}

DataWriterI::DataWriterI(TopicWriterI* topic, long long int id, const DataStorm::WriterConfig& config) :
    DataElementI(topic, id, config),
    _parent(topic)
{
}

void
DataWriterI::init()
{
    DataElementI::init();
    _subscribers = Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_forwarder);
}

void
DataWriterI::publish(const shared_ptr<Key>& key, const shared_ptr<Sample>& sample)
{
    lock_guard<mutex> lock(_parent->_mutex);
    if(sample->event == DataStorm::SampleEvent::PartialUpdate)
    {
        assert(!sample->hasValue());
        shared_ptr<Sample> previous = !_samples.empty() ? _samples.back() : nullptr;
        _parent->getUpdater(sample->tag)(previous, sample, _parent->getInstance()->getCommunicator());
    }

    sample->id = ++_parent->_nextSampleId;
    sample->timestamp = chrono::system_clock::now();

    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": publishing sample " << sample->id << " listeners=" << _listenerCount;
    }
    send(key, sample);

    if(_config->sampleLifetime && *_config->sampleLifetime > 0)
    {
        cleanOldSamples(_samples, sample->timestamp, *_config->sampleLifetime);
    }

    if(_config->sampleCount)
    {
        if(*_config->sampleCount > 0)
        {
            if(_samples.size() + 1 > static_cast<size_t>(*_config->sampleCount))
            {
                _samples.pop_front();
            }
        }
        else if(*_config->sampleCount == 0)
        {
            return; // Don't keep history
        }
    }

    if(_config->clearHistory &&
       (*_config->clearHistory == ClearHistoryPolicy::OnAll ||
        (sample->event == DataStorm::SampleEvent::Add && *_config->clearHistory == ClearHistoryPolicy::OnAdd) ||
        (sample->event == DataStorm::SampleEvent::Remove && *_config->clearHistory == ClearHistoryPolicy::OnRemove) ||
        (sample->event != DataStorm::SampleEvent::PartialUpdate &&
         *_config->clearHistory == ClearHistoryPolicy::OnAllExceptPartialUpdate)))
    {
        _samples.clear();
    }
    _samples.push_back(sample);
}

KeyDataReaderI::KeyDataReaderI(TopicReaderI* topic,
                               long long int id,
                               const vector<shared_ptr<Key>>& keys,
                               const string& sampleFilter,
                               const vector<unsigned char> sampleFilterCriteria,
                               const DataStorm::ReaderConfig& config) :
    DataReaderI(topic, id, sampleFilter, sampleFilterCriteria, config),
    _keys(keys)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": created key reader";
    }

    //
    // If sample filtering is enabled, ensure the updates are received using a session
    // facet specific to this reader.
    //
    if(_config->sampleFilter)
    {
        ostringstream os;
        os << "fa" << _id;
        _config->facet = os.str();
    }
}

void
KeyDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed key reader";
    }
    try
    {
        _forwarder->detachElements(_parent->getId(), { _keys.empty() ? -_id : _id });
    }
    catch(const std::exception&)
    {
        _parent->forwarderException();
    }
    _parent->remove(_keys, shared_from_this());
}

void
KeyDataReaderI::waitForWriters(int count)
{
    waitForListeners(count);
}

bool
KeyDataReaderI::hasWriters()
{
    return hasListeners();
}

string
KeyDataReaderI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ":[";
    for(auto q = _keys.begin(); q != _keys.end(); ++q)
    {
        if(q != _keys.begin())
        {
            os << ",";
        }
        os << (*q)->toString();
    }
    os << "]@" << _parent->getId();
    return os.str();
}

bool
KeyDataReaderI::matchKey(const shared_ptr<Key>& key) const
{
    return _keys.empty() || find(_keys.begin(), _keys.end(), key) != _keys.end();
}

KeyDataWriterI::KeyDataWriterI(TopicWriterI* topic,
                               long long int id,
                               const vector<shared_ptr<Key>>& keys,
                               const DataStorm::WriterConfig& config) :
    DataWriterI(topic, id, config),
    _keys(keys)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": created key writer";
    }
}

void
KeyDataWriterI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed key writer";
    }
    try
    {
        _forwarder->detachElements(_parent->getId(), { _keys.empty() ? -_id : _id });
    }
    catch(const std::exception&)
    {
        _parent->forwarderException();
    }
    _parent->remove(_keys, shared_from_this());
}

void
KeyDataWriterI::waitForReaders(int count) const
{
    waitForListeners(count);
}

bool
KeyDataWriterI::hasReaders() const
{
    return hasListeners();
}

shared_ptr<Sample>
KeyDataWriterI::getLast() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    return _samples.empty () ? nullptr : _samples.back();
}

vector<std::shared_ptr<Sample>>
KeyDataWriterI::getAll() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    vector<shared_ptr<Sample>> all(_samples.begin(), _samples.end());
    return all;
}

string
KeyDataWriterI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ":[";
    for(auto q = _keys.begin(); q != _keys.end(); ++q)
    {
        if(q != _keys.begin())
        {
            os << ",";
        }
        os << (*q)->toString();
    }
    os << "]@" << _parent->getId();
    return os.str();
}

DataSamples
KeyDataWriterI::getSamples(const shared_ptr<Key>& key,
                           const shared_ptr<Filter>& sampleFilter,
                           const shared_ptr<DataStormContract::ElementConfig>& config,
                           long long int lastId,
                           const chrono::time_point<chrono::system_clock>& now)
{
    DataSamples samples;
    samples.id = _keys.empty() ? -_id : _id;

    if(config->sampleCount && *config->sampleCount == 0)
    {
        return samples;
    }

    if(_config->sampleLifetime && *_config->sampleLifetime > 0)
    {
        cleanOldSamples(_samples, now, *_config->sampleLifetime);
    }

    chrono::time_point<chrono::system_clock> staleTime = chrono::time_point<chrono::system_clock>::min();
    if(config->sampleLifetime && *config->sampleLifetime > 0)
    {
        staleTime = now - chrono::milliseconds(*config->sampleLifetime);
    }

    shared_ptr<Sample> first;
    for(auto p = _samples.rbegin(); p != _samples.rend(); ++p)
    {
        if((*p)->timestamp < staleTime)
        {
            break;
        }
        if((*p)->id <= lastId)
        {
            break;
        }

        if((!key || key == (*p)->key) && (!sampleFilter || sampleFilter->match(*p)))
        {
            first = *p;
            samples.samples.push_front(toSample(*p, getCommunicator(), _keys.empty()));
            if(config->sampleCount &&
               *config->sampleCount > 0 && static_cast<size_t>(*config->sampleCount) == samples.samples.size())
            {
                break;
            }

            if(config->clearHistory &&
               (*config->clearHistory == ClearHistoryPolicy::OnAll ||
                ((*p)->event == DataStorm::SampleEvent::Add && *config->clearHistory == ClearHistoryPolicy::OnAdd) ||
                ((*p)->event == DataStorm::SampleEvent::Remove && *config->clearHistory == ClearHistoryPolicy::OnRemove) ||
                ((*p)->event != DataStorm::SampleEvent::PartialUpdate &&
                     *config->clearHistory == ClearHistoryPolicy::OnAllExceptPartialUpdate)))
            {
                break;
            }
        }
    }
    if(!samples.samples.empty())
    {
        // If the first sample is a partial update, transform it to an full Update
        if(first->event == DataStorm::SampleEvent::PartialUpdate)
        {
            samples.samples[0] = {
                first->id,
                samples.samples[0].keyId,
                samples.samples[0].keyValue,
                chrono::time_point_cast<chrono::microseconds>(first->timestamp).time_since_epoch().count(),
                0,
                DataStorm::SampleEvent::Update,
                first->encodeValue(getCommunicator()) };
        }
    }
    return samples;
}

void
KeyDataWriterI::send(const shared_ptr<Key>& key, const shared_ptr<Sample>& sample) const
{
    assert(key || _keys.size() == 1);
    _sample = sample;
    _sample->key = key ? key : _keys[0];
    _subscribers->s(_parent->getId(), _keys.empty() ? -_id : _id, toSample(sample, getCommunicator(), _keys.empty()));
    _sample = nullptr;
}

FilteredDataReaderI::FilteredDataReaderI(TopicReaderI* topic,
                                         long long int id,
                                         const shared_ptr<Filter>& filter,
                                         const string& sampleFilter,
                                         vector<unsigned char> sampleFilterCriteria,
                                         const DataStorm::ReaderConfig& config) :
    DataReaderI(topic, id, sampleFilter, sampleFilterCriteria, config),
    _filter(filter)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": created filtered reader";
    }

    //
    // If sample filtering is enabled, ensure the updates are received using a session
    // facet specific to this reader.
    //
    if(_config->sampleFilter)
    {
        ostringstream os;
        os << "fa" << _id;
        _config->facet = os.str();
    }
}

void
FilteredDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed filter reader";
    }
    try
    {
        _forwarder->detachElements(_parent->getId(), { -_id });
    }
    catch(const std::exception&)
    {
        _parent->forwarderException();
    }
    _parent->removeFiltered(_filter, shared_from_this());
}

void
FilteredDataReaderI::waitForWriters(int count)
{
     waitForListeners(count);
}

bool
FilteredDataReaderI::hasWriters()
{
     return hasListeners();
}

string
FilteredDataReaderI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ":" << _filter->toString() << "@" << _parent->getId();
    return os.str();
}

bool
FilteredDataReaderI::matchKey(const shared_ptr<Key>& key) const
{
    return _filter->match(key);
}
