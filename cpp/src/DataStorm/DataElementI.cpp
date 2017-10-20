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

using namespace std;
using namespace DataStormInternal;
using namespace DataStormContract;

namespace
{

DataSample
toSample(const shared_ptr<Sample>& sample, const shared_ptr<Ice::Communicator>& communicator)
{
    return { sample->id, sample->timestamp, sample->event, sample->encode(communicator) };
}

}

DataElementI::DataElementI(TopicI* parent, long long int id) :
    _traceLevels(parent->getInstance()->getTraceLevels()),
    _forwarder(Ice::uncheckedCast<SessionPrx>(parent->getInstance()->getForwarderManager()->add(this))),
    _id(id),
    _listenerCount(0),
    _parent(parent),
    _waiters(0),
    _notified(0)
{
}

DataElementI::~DataElementI()
{
    disconnect();
    _parent->getInstance()->getForwarderManager()->remove(_forwarder->ice_getIdentity());
}

void
DataElementI::destroy()
{
    {
        unique_lock<mutex> lock(_parent->_mutex);
        destroyImpl(); // Must be called first.
    }
    disconnect();
}

bool
DataElementI::attachKey(long long int topicId,
                        long long int elementId,
                        const shared_ptr<Key>& key,
                        const shared_ptr<Filter>& sampleFilter,
                        SessionI* session,
                        const shared_ptr<SessionPrx>& prx,
                        const string& facet)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, facet }, Listener(prx, facet)).first;
    }
    if(p->second.keys.add(topicId, elementId, sampleFilter))
    {
        ++_listenerCount;
        session->subscribeToKey(topicId, elementId, key, this, facet);
        notifyListenerWaiters(session->getTopicLock());
        if(_onConnect)
        {
            _parent->queue(shared_from_this(), [=] { _onConnect(make_tuple(session->getId(), topicId, elementId)); });
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
                        SessionI* session,
                        const string& facet,
                        bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p != _listeners.end() && p->second.keys.remove(topicId, elementId))
    {
        --_listenerCount;
        if(unsubscribe)
        {
            session->unsubscribeFromKey(topicId, elementId, this);
        }
        if(_onDisconnect)
        {
            _parent->queue(shared_from_this(), [=] { _onDisconnect(make_tuple(session->getId(), topicId, elementId)); });
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
                           long long int elementId,
                           const shared_ptr<Filter>& filter,
                           const shared_ptr<Filter>& sampleFilter,
                           SessionI* session,
                           const shared_ptr<SessionPrx>& prx,
                           const string& facet)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, facet }, Listener(prx, facet)).first;
    }
    if(p->second.filters.add(topicId, elementId, sampleFilter))
    {
        ++_listenerCount;
        session->subscribeToFilter(topicId, elementId, filter, this, facet);
        if(_onConnect)
        {
            _parent->queue(shared_from_this(), [=] { _onConnect(make_tuple(session->getId(), topicId, elementId)); });
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
                           SessionI* session,
                           const string& facet,
                           bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p != _listeners.end() && p->second.filters.remove(topicId, elementId))
    {
        --_listenerCount;
        if(unsubscribe)
        {
            session->unsubscribeFromFilter(topicId, elementId, this);
        }
        if(_onDisconnect)
        {
            _parent->queue(shared_from_this(), [=] { _onDisconnect(make_tuple(session->getId(), topicId, elementId)); });
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
    _onConnect = std::move(callback);
}

void
DataElementI::onDisconnect(function<void(tuple<string, long long int, long long int>)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onDisconnect = std::move(callback);
}

void
DataElementI::initSamples(const vector<shared_ptr<Sample>>&, SessionI* s, long long int topic, long long int element)
{
    if(s)
    {
        s->subscriberInitialized(topic, element, this);
    }
}

DataSampleSeq
DataElementI::getSamples(long long int, const std::shared_ptr<Filter>&) const
{
    return {};
}

vector<unsigned char>
DataElementI::getSampleFilterCriteria() const
{
    return {};
}

void
DataElementI::queue(const shared_ptr<Sample>&, const string&)
{
    assert(false);
}

std::shared_ptr<Filter>
DataElementI::createSampleFilter(vector<unsigned char>) const
{
    return nullptr;
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
        else if(count >= 0 && _listenerCount >= count)
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
            listener.first.session->disconnectFromKey(ks.first.first, ks.first.second, this);
        }
        for(const auto& fs : listener.second.filters.subscribers)
        {
            listener.first.session->disconnectFromFilter(fs.first.first, fs.first.second, this);
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

DataReaderI::DataReaderI(TopicReaderI* topic, long long int id, vector<unsigned char> sampleFilterCriteria) :
    DataElementI(topic, id),
    _parent(topic),
    _sampleFilterCriteria(move(sampleFilterCriteria))
{
}

int
DataReaderI::getInstanceCount() const
{
    lock_guard<mutex> lock(_parent->_mutex);
    return _instanceCount;
}

vector<shared_ptr<Sample>>
DataReaderI::getAll()
{
    getAllUnread(); // Read and add unread

    lock_guard<mutex> lock(_parent->_mutex);
    return vector<shared_ptr<Sample>>(_all.begin(), _all.end());
}

vector<shared_ptr<Sample>>
DataReaderI::getAllUnread()
{
    lock_guard<mutex> lock(_parent->_mutex);
    vector<shared_ptr<Sample>> unread(_unread.begin(), _unread.end());
    for(const auto& s : unread)
    {
        s->decode(getCommunicator());
        _all.push_back(s);
    }
    _unread.clear();
    return unread;
}

void
DataReaderI::waitForUnread(unsigned int count) const
{
    unique_lock<mutex> lock(_parent->_mutex);
    while(_unread.size() < count)
    {
        _parent->_cond.wait(lock);
    }
}

bool
DataReaderI::hasUnread() const
{
    unique_lock<mutex> lock(_parent->_mutex);
    return !_unread.empty();
}

shared_ptr<Sample>
DataReaderI::getNextUnread()
{
    unique_lock<mutex> lock(_parent->_mutex);
    _parent->_cond.wait(lock, [&]() { return !_unread.empty(); });
    shared_ptr<Sample> sample = _unread.front();
    _unread.pop_front();
    _all.push_back(sample);
    sample->decode(getCommunicator());
    return sample;
}

void
DataReaderI::initSamples(const vector<shared_ptr<Sample>>& samples,
                         SessionI* session,
                         long long int topic,
                         long long int element)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": initialized " << samples.size() << " samples from `" << element << '@' << topic << "'";
    }
    _unread.insert(_unread.end(), samples.begin(), samples.end());
    if(session)
    {
        session->subscriberInitialized(topic, element, this);
    }
    _parent->_cond.notify_all();
}

vector<unsigned char>
DataReaderI::getSampleFilterCriteria() const
{
    return _sampleFilterCriteria;
}

void
DataReaderI::queue(const shared_ptr<Sample>& sample, const string&)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": queued sample " << sample->id << " listeners=" << _listenerCount;
    }
    _unread.push_back(sample);
    if(_onSample)
    {
        _parent->queue(shared_from_this(), [this, sample] { _onSample(sample); });
    }
    _parent->_cond.notify_all();
}

void
DataReaderI::onSample(function<void(const shared_ptr<Sample>&)> callback)
{
    unique_lock<mutex> lock(_parent->_mutex);
    _onSample = std::move(callback);
}

DataWriterI::DataWriterI(TopicWriterI* topic, long long int id, const std::shared_ptr<FilterFactory>& sampleFilterFactory) :
    DataElementI(topic, id),
    _parent(topic),
    _sampleFilterFactory(sampleFilterFactory),
    _subscribers(Ice::uncheckedCast<SubscriberSessionPrx>(_forwarder))
{
}

void
DataWriterI::publish(const shared_ptr<Sample>& sample)
{
    lock_guard<mutex> lock(_parent->_mutex);
    sample->id = ++_parent->_nextSampleId;
    sample->timestamp = IceUtil::Time::now().toMilliSeconds();
    _all.push_back(sample);
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": publishing sample " << sample->id << " listeners=" << _listenerCount;
    }
    send(_all.back());
}

std::shared_ptr<Filter>
DataWriterI::createSampleFilter(vector<unsigned char> sampleFilter) const
{
    if(_sampleFilterFactory && !sampleFilter.empty())
    {
        return _sampleFilterFactory->decode(_parent->getInstance()->getCommunicator(), move(sampleFilter));
    }
    return nullptr;
}

KeyDataReaderI::KeyDataReaderI(TopicReaderI* topic,
                               long long int id,
                               const vector<shared_ptr<Key>>& keys,
                               const vector<unsigned char> sampleFilterCriteria) :
    DataReaderI(topic, id, sampleFilterCriteria),
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
    if(!_sampleFilterCriteria.empty())
    {
        ostringstream os;
        os << "fa" << _id;
        _facet = os.str();
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
    _forwarder->detachElements(_parent->getId(), { _id });
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

KeyDataWriterI::KeyDataWriterI(TopicWriterI* topic,
                               long long int id,
                               const shared_ptr<Key>& key,
                               const std::shared_ptr<FilterFactory>& sampleFilterFactory) :
    DataWriterI(topic, id, sampleFilterFactory),
    _key(key)
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
    _forwarder->detachElements(_parent->getId(), { _id });
    _parent->remove({ _key }, shared_from_this());
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

string
KeyDataWriterI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ":" << _key->toString() << "@" << _parent->getId();
    return os.str();
}

DataSampleSeq
KeyDataWriterI::getSamples(long long int lastId, const shared_ptr<Filter>& filter) const
{
    DataSampleSeq samples;
    if(lastId < 0)
    {
        samples.reserve(_all.size());
    }
    for(const auto& p : _all)
    {
        if(p->id > lastId)
        {
            if(!filter || filter->match(p))
            {
                samples.push_back(toSample(p, nullptr)); // The sample should already be encoded
            }
        }
    }
    return samples;
}

void
KeyDataWriterI::send(const shared_ptr<Sample>& sample) const
{
    _sample = sample;
    _sample->key = _key;
    _subscribers->s(_parent->getId(), _id, toSample(sample, getCommunicator()));
    _sample = nullptr;
}

FilteredDataReaderI::FilteredDataReaderI(TopicReaderI* topic,
                                         long long int id,
                                         const shared_ptr<Filter>& filter,
                                         vector<unsigned char> sampleFilterCriteria) :
    DataReaderI(topic, id, sampleFilterCriteria),
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
    if(!_sampleFilterCriteria.empty())
    {
        ostringstream os;
        os << "fa" << _id;
        _facet = os.str();
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
    _forwarder->detachElements(_parent->getId(), { -_id });
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

FilteredDataWriterI::FilteredDataWriterI(TopicWriterI* topic,
                                         long long int id,
                                         const shared_ptr<Filter>& filter,
                                         const shared_ptr<FilterFactory>& sampleFilterFactory) :
    DataWriterI(topic, id, sampleFilterFactory),
    _filter(filter)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": created filtered writer";
    }
}

void
FilteredDataWriterI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed filter writer";
    }
    _forwarder->detachElements(_parent->getId(), { -_id });
    _parent->removeFiltered(_filter, shared_from_this());
}

void
FilteredDataWriterI::waitForReaders(int count) const
{
    waitForListeners(count);
}

bool
FilteredDataWriterI::hasReaders() const
{
    return hasListeners();
}

string
FilteredDataWriterI::toString() const
{
    ostringstream os;
    os << 'e' << _id << ":" << _filter->toString() << "@" << _parent->getId();
    return os.str();
}

void
FilteredDataWriterI::send(const shared_ptr<Sample>& sample) const
{
    // _sample = sample;
    // _subscribers->f(_parent->getId(), _id, toSample(sample, getCommunicator()));
    // _sample = nullptr;
}
