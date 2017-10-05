// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/DataElementI.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/PeerI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormInternal;

DataElementI::DataElementI(TopicI* parent) :
    _traceLevels(parent->getInstance()->getTraceLevels()),
    _forwarder(Ice::uncheckedCast<DataStormContract::SessionPrx>(parent->getInstance()->getForwarderManager()->add(this))),
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
DataElementI::attachKey(long long int topic,
                        long long int id,
                        const shared_ptr<Key>& key, SessionI* session,
                        const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    // No locking necessary, called by the session with the mutex locked
    if(_keys.emplace(Listener { topic, id, session }, prx).second)
    {
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
    _keys.erase({ topic, id, session });
    if(unsubscribe)
    {
        session->unsubscribeFromKey(topic, id, this);
    }
    notifyListenerWaiters(session->getLock());
}

bool
DataElementI::attachFilter(long long int topic, long long int id,
                           const shared_ptr<Filter>& filter,
                           SessionI* session,
                           const shared_ptr<DataStormContract::SessionPrx>& prx)
{
    // No locking necessary, called by the session with the mutex locked
    if(_filters.emplace(Listener { topic, id, session }, prx).second)
    {
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
    _filters.erase({ topic, id, session });
    if(unsubscribe)
    {
        session->unsubscribeFromFilter(topic, id, this);
    }
    notifyListenerWaiters(session->getLock());
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
        if(count < 0 && (_filters.size() + _keys.size() == 0))
        {
            --_waiters;
            return;
        }
        else if(count >= 0 && (_filters.size() + _keys.size() >= count))
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
    return !_keys.empty() || !_filters.empty();
}

shared_ptr<DataStorm::TopicFactory>
DataElementI::getTopicFactory() const
{
    return _parent->getTopicFactory();
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
    map<Listener, shared_ptr<DataStormContract::SessionPrx>> keys;
    map<Listener, shared_ptr<DataStormContract::SessionPrx>> filters;
    {
        unique_lock<mutex> lock(_parent->_mutex);
        keys.swap(_keys);
        filters.swap(_filters);
        notifyListenerWaiters(lock);
    }
    for(const auto& key : keys)
    {
        key.first.session->disconnectFromKey(key.first.topic, key.first.id, this);
    }
    for(const auto& filter : filters)
    {
        filter.first.session->disconnectFromFilter(filter.first.topic, filter.first.id, this);
    }
}

void
DataElementI::forward(const Ice::ByteSeq& inEncaps, const Ice::Current& current) const
{
    for(const auto& k : _keys)
    {
        k.second->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
    }
    for(const auto& f : _filters)
    {
        f.second->ice_invokeAsync(current.operation, current.mode, inEncaps, current.ctx);
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
DataReaderI::getAll() const
{
    lock_guard<mutex> lock(_parent->_mutex);
    return vector<shared_ptr<Sample>>(_all.begin(), _all.end());
}

vector<shared_ptr<Sample>>
DataReaderI::getAllUnread()
{
    lock_guard<mutex> lock(_parent->_mutex);
    vector<shared_ptr<Sample>> unread(_unread.begin(), _unread.end());
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
    return sample;
}

void
DataReaderI::queue(const shared_ptr<Sample>& sample)
{
    // No need for locking, the parent is locked when this is called.
    if(sample->type == DataStorm::SampleType::Add)
    {
        ++_instanceCount;
    }
    _unread.push_back(sample);
    _all.push_back(sample);
    _parent->_cond.notify_all();
}

DataWriterI::DataWriterI(TopicWriterI* topic) :
    DataElementI(topic),
    _parent(topic),
    _subscribers(Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_forwarder))
{
}

void
DataWriterI::add(const vector<unsigned char>& value)
{
    publish(make_shared<DataStormContract::DataSample>(0, 0, DataStormContract::SampleType::Add, value));
}

void
DataWriterI::update(const vector<unsigned char>& value)
{
    publish(make_shared<DataStormContract::DataSample>(0, 0, DataStormContract::SampleType::Update, value));
}

void
DataWriterI::remove()
{
    publish(make_shared<DataStormContract::DataSample>(0, 0, DataStormContract::SampleType::Remove, Ice::ByteSeq()));
}

void
DataWriterI::publish(const shared_ptr<DataStormContract::DataSample>& sample)
{
    lock_guard<mutex> lock(_parent->_mutex);
    sample->id = ++_parent->_nextSampleId;
    sample->timestamp = IceUtil::Time::now().toMilliSeconds();
    _all.push_back(sample);
    send(_all.back());
}

KeyDataReaderI::KeyDataReaderI(TopicReaderI* parent, const shared_ptr<Key>& key) :
    DataElementI(parent),
    DataReaderI(parent),
    _key(key)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created key data reader `" << _parent << '/' << _key << "'";
    }
}

void
KeyDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed key data reader `" << _parent << '/' << _key << "'";
    }
    _forwarder->detachKey(_parent->getId(), _key->getId());
    _parent->remove(_key);
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

DataStormContract::KeyInfo
KeyDataReaderI::getKeyInfo() const
{
    return { _key->getId(), _key->marshal(_parent->getTopicFactory()), 0 };
}

DataStormContract::KeyInfoAndSamples
KeyDataReaderI::getKeyInfoAndSamples(long long int) const
{
    return { getKeyInfo(), {} };
}

KeyDataWriterI::KeyDataWriterI(TopicWriterI* topic, const shared_ptr<Key>& key) :
    DataElementI(topic),
    DataWriterI(topic),
    _key(key)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created key data writer `" << _parent << '/' << _key << "'";
    }
}

void
KeyDataWriterI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed key data writer `" << _parent << '/' << _key << "'";
    }
    _forwarder->detachKey(_parent->getId(), _key->getId());
    _parent->remove(_key);
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

DataStormContract::KeyInfo
KeyDataWriterI::getKeyInfo() const
{
    return { _key->getId(), _key->marshal(_parent->getTopicFactory()), 0 };
}

DataStormContract::KeyInfoAndSamples
KeyDataWriterI::getKeyInfoAndSamples(long long int lastId) const
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
            samples.push_back(p);
        }
    }
    return { getKeyInfo(), samples };
}

void
KeyDataWriterI::send(const shared_ptr<DataStormContract::DataSample>& sample) const
{
    _subscribers->s(_parent->getId(), _key->getId(), sample);
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
    _parent->removeFiltered(_filter);
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

DataStormContract::FilterInfo
FilteredDataReaderI::getFilterInfo() const
{
    return { _filter->getId(), _filter->marshal(_parent->getTopicFactory()) };
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
    _parent->removeFiltered(_filter);
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

DataStormContract::FilterInfo
FilteredDataWriterI::getFilterInfo() const
{
    return { _filter->getId(), _filter->marshal(_parent->getTopicFactory()) };
}

void
FilteredDataWriterI::send(const shared_ptr<DataStormContract::DataSample>& sample) const
{
    _subscribers->f(_parent->getId(), _filter->getId(), sample);
}
