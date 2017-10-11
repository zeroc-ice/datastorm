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

namespace
{

DataStormContract::DataSample
toSample(const shared_ptr<Sample>& sample, const shared_ptr<Ice::Communicator>& communicator)
{
    return { sample->id, sample->timestamp, sample->type, sample->encode(communicator) };
}

}

DataElementI::DataElementI(TopicI* parent) :
    _traceLevels(parent->getInstance()->getTraceLevels()),
    _forwarder(Ice::uncheckedCast<DataStormContract::SessionPrx>(parent->getInstance()->getForwarderManager()->add(this))),
    _parent(parent),
    _listenerCount(0),
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
DataElementI::attachKey(long long int topic,
                        long long int id,
                        const shared_ptr<Key>& key,
                        SessionI* session,
                        const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session }, Listener(prx)).first;
    }
    if(p->second.keys.add(topic, id, key))
    {
        ++_listenerCount;
        session->subscribeToKey(topic, id, key, this);
        notifyListenerWaiters(session->getLock());
        return true;
    }
    else
    {
        return false;
    }
}

void
DataElementI::detachKey(long long int topic, long long int id, SessionI* session, bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session });
    if(p != _listeners.end() && p->second.keys.remove(topic, id))
    {
        --_listenerCount;
        if(unsubscribe)
        {
            session->unsubscribeFromKey(topic, id, this);
        }
        notifyListenerWaiters(session->getLock());
        if(p->second.empty())
        {
            _listeners.erase(p);
        }
    }
}

bool
DataElementI::attachFilter(long long int topic,
                           long long int id,
                           const shared_ptr<Filter>& filter,
                           SessionI* session,
                           const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session }, Listener(prx)).first;
    }
    if(p->second.filters.add(topic, id, filter))
    {
        ++_listenerCount;
        session->subscribeToFilter(topic, id, filter, this);
        notifyListenerWaiters(session->getLock());
        return true;
    }
    else
    {
        return false;
    }
}

void
DataElementI::detachFilter(long long int topic, long long int id, SessionI* session, bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session });
    if(p != _listeners.end() && p->second.filters.remove(topic, id))
    {
        --_listenerCount;
        if(unsubscribe)
        {
            session->unsubscribeFromFilter(topic, id, this);
        }
        notifyListenerWaiters(session->getLock());
        if(p->second.empty())
        {
            _listeners.erase(p);
        }
    }
}

void
DataElementI::queue(const shared_ptr<Sample>&)
{
    assert(false);
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

DataReaderI::DataReaderI(TopicReaderI* topic) :
    DataElementI(topic),
    _parent(topic)
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
    _all.insert(_all.end(), unread.begin(), unread.end());
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
DataReaderI::queue(const shared_ptr<Sample>& sample)
{
    _unread.push_back(sample);
    _parent->_cond.notify_all();
}

DataWriterI::DataWriterI(TopicWriterI* topic) :
    DataElementI(topic),
    _parent(topic),
    _subscribers(Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_forwarder))
{
}

void
DataWriterI::publish(const shared_ptr<Sample>& sample)
{
    lock_guard<mutex> lock(_parent->_mutex);
    sample->id = ++_parent->_nextSampleId;
    sample->timestamp = IceUtil::Time::now().toMilliSeconds();
    _all.push_back(sample);
    send(_all.back());
}

KeyDataReaderI::KeyDataReaderI(TopicReaderI* parent, const vector<shared_ptr<Key>>& keys) :
    DataElementI(parent),
    DataReaderI(parent),
    _keys(keys)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created key data reader `" << _parent << '/' << _keys << "'";
    }
}

void
KeyDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed key data reader `" << _parent << '/' << _keys << "'";
    }
    DataStormContract::LongSeq ids;
    for(auto k : _keys)
    {
        ids.emplace_back(k->getId());
    }
    _forwarder->detachKeys(_parent->getId(), ids);
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

DataStormContract::DataSampleSeq
KeyDataReaderI::getSamples(long long int, const shared_ptr<Filter>&) const
{
    return {};
}

KeyDataWriterI::KeyDataWriterI(TopicWriterI* topic, const vector<shared_ptr<Key>>& keys) :
    DataElementI(topic),
    DataWriterI(topic),
    _keys(keys)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created key data writer `" << _parent << '/' << _keys << "'";
    }
}

void
KeyDataWriterI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed key data writer `" << _parent << '/' << _keys << "'";
    }
    DataStormContract::LongSeq ids;
    for(auto k : _keys)
    {
        ids.emplace_back(k->getId());
    }
    _forwarder->detachKeys(_parent->getId(), ids);
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

DataStormContract::DataSampleSeq
KeyDataWriterI::getSamples(long long int lastId, const shared_ptr<Filter>& filter) const
{
    DataStormContract::DataSampleSeq samples;
    if(lastId < 0)
    {
        samples.reserve(_all.size());
    }
    for(const auto& p : _all)
    {
        if(p->id > lastId)
        {
            if(!filter || filter->match(p, true))
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
    DataStormContract::DataSample s = toSample(sample, getCommunicator());
    for(auto k : _keys)
    {
        _sample->key = k;
        _subscribers->s(_parent->getId(), k->getId(), s);
    }
    _sample = nullptr;
}

FilteredDataReaderI::FilteredDataReaderI(TopicReaderI* topic, const shared_ptr<Filter>& filter) :
    DataElementI(topic),
    DataReaderI(topic),
    _filter(filter)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created filtered data reader `" << _parent << '/' << _filter << "'";
    }
}

void
FilteredDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed filter data reader `" << _parent << '/' << _filter << "'";
    }
    _forwarder->detachFilter(_parent->getId(), _filter->getId());
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

void
FilteredDataReaderI::queue(const shared_ptr<Sample>& sample)
{
    sample->decode(getCommunicator());
    if(_filter->match(sample, false))
    {
        DataReaderI::queue(sample);
    }
}

FilteredDataWriterI::FilteredDataWriterI(TopicWriterI* topic, const shared_ptr<Filter>& filter) :
    DataElementI(topic),
    DataWriterI(topic),
    _filter(filter)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created filtered data writer `" << _parent << '/' << _filter << "'";
    }
}

void
FilteredDataWriterI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed filter data writer `" << _parent << '/' << _filter << "'";
    }
    _forwarder->detachFilter(_parent->getId(), _filter->getId());
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

void
FilteredDataWriterI::send(const shared_ptr<Sample>& sample) const
{
    _sample = sample;
    _subscribers->f(_parent->getId(), _filter->getId(), toSample(sample, getCommunicator()));
    _sample = nullptr;
}
