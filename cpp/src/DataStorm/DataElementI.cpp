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
    return { sample->id, sample->timestamp, sample->type, sample->encode(communicator) };
}

}

DataElementI::DataElementI(TopicI* parent) :
    _traceLevels(parent->getInstance()->getTraceLevels()),
    _forwarder(Ice::uncheckedCast<SessionPrx>(parent->getInstance()->getForwarderManager()->add(this))),
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
                        const shared_ptr<SessionPrx>& prx)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, prx->ice_getFacet() });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, prx->ice_getFacet() }, Listener(prx)).first;
    }
    if(p->second.keys.add(topic, id, key))
    {
        ++_listenerCount;
        session->subscribeToKey(topic, id, key, this, _facet);
        notifyListenerWaiters(session->getTopicLock());
        return true;
    }
    else
    {
        return false;
    }
}

void
DataElementI::detachKey(long long int topic, long long int id, SessionI* session, const string& facet, bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p != _listeners.end() && p->second.keys.remove(topic, id))
    {
        --_listenerCount;
        if(unsubscribe)
        {
            session->unsubscribeFromKey(topic, id, this);
        }
        notifyListenerWaiters(session->getTopicLock());
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
                           const shared_ptr<SessionPrx>& prx)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, prx->ice_getFacet() });
    if(p == _listeners.end())
    {
        p = _listeners.emplace(ListenerKey { session, prx->ice_getFacet() }, Listener(prx)).first;
    }
    if(p->second.filters.add(topic, id, filter))
    {
        ++_listenerCount;
        session->subscribeToFilter(topic, id, filter, this, _facet);
        notifyListenerWaiters(session->getTopicLock());
        return true;
    }
    else
    {
        return false;
    }
}

void
DataElementI::detachFilter(long long int topic, long long int id, SessionI* session, const string& facet, bool unsubscribe)
{
    // No locking necessary, called by the session with the mutex locked
    auto p = _listeners.find({ session, facet });
    if(p != _listeners.end() && p->second.filters.remove(topic, id))
    {
        --_listenerCount;
        if(unsubscribe)
        {
            session->unsubscribeFromFilter(topic, id, this);
        }
        notifyListenerWaiters(session->getTopicLock());
        if(p->second.empty())
        {
            _listeners.erase(p);
        }
    }
}

void
DataElementI::initSamples(const vector<shared_ptr<Sample>>&)
{
    assert(false);
}

void
DataElementI::queue(const shared_ptr<Sample>&, const string&)
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
DataReaderI::initSamples(const vector<shared_ptr<Sample>>& samples)
{
    if(_all.empty())
    {
        if(_traceLevels->data > 1)
        {
            Trace out(_traceLevels, _traceLevels->dataCat);
            out << this << ": initialized samples";
            if(!_facet.empty())
            {
                out << " (facet = " << _facet << ")";
            }
        }
        _unread.insert(_unread.end(), samples.begin(), samples.end());
        _parent->_cond.notify_all();
    }
}

void
DataReaderI::queue(const shared_ptr<Sample>& sample, const string&)
{
    if(_traceLevels->data > 1)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": queued sample " << sample->id;
        if(!_facet.empty())
        {
            out << " (facet = " << _facet << ")";
        }
    }
    _unread.push_back(sample);
    _parent->_cond.notify_all();
}

DataWriterI::DataWriterI(TopicWriterI* topic) :
    DataElementI(topic),
    _parent(topic),
    _subscribers(Ice::uncheckedCast<SubscriberSessionPrx>(_forwarder))
{
}

bool
DataWriterI::attachFilter(long long int topic,
                          long long int id,
                          const shared_ptr<Filter>& filter,
                          SessionI* session,
                          const shared_ptr<SessionPrx>& prx)
{
    //
    // If writer sample filtering is enabled, ensure the updates are sent using a session
    // facet specific to the reader.
    //
    auto p = prx;
    if(filter->hasWriterMatch())
    {
        ostringstream os;
        os << 'f' << id;
        p = Ice::uncheckedCast<SessionPrx>(prx->ice_facet(os.str()));
    }
    return DataElementI::attachFilter(topic, id, filter, session, p);
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
        out << this << ": publishing sample " << sample->id;
        if(!_facet.empty())
        {
            out << " (facet = " << _facet << ")";
        }
    }
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
        out << this << ": created key data reader";
    }
}

void
KeyDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed key data reader";
    }
    LongSeq ids;
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

string
KeyDataReaderI::toString() const
{
    ostringstream os;
    os << '[';
    for(auto q = _keys.begin(); q != _keys.end(); ++q)
    {
        if(q != _keys.begin())
        {
            os << ",";
        }
        os << (*q)->toString();
    }
    os << "]@" << _parent->getName();
    return os.str();
}

DataSampleSeq
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
        out << this << ": created key data writer";
    }
}

void
KeyDataWriterI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed key data writer";
    }
    LongSeq ids;
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

string
KeyDataWriterI::toString() const
{
    ostringstream os;
    os << '[';
    for(auto q = _keys.begin(); q != _keys.end(); ++q)
    {
        if(q != _keys.begin())
        {
            os << ",";
        }
        os << (*q)->toString();
    }
    os << "]@" << _parent->getName();
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
            if(!filter || filter->writerMatch(p))
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
    DataSample s = toSample(sample, getCommunicator());
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
        out << this << ": created filtered data reader";
    }

    //
    // If writer sample filtering is enabled, ensure the updates are received using a session
    // facet specific to this reader.
    //
    if(_filter->hasWriterMatch())
    {
        ostringstream os;
        os << 'f' << _filter->getId();
        _facet = os.str();
    }
}

void
FilteredDataReaderI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed filter data reader";
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

string
FilteredDataReaderI::toString() const
{
    ostringstream os;
    os << _filter->toString() << '@' << _parent->getName();
    return os.str();
}

void
FilteredDataReaderI::initSamples(const vector<shared_ptr<Sample>>& samples)
{
    if(_all.empty())
    {
        if(_filter->hasReaderMatch())
        {
            for(auto s : samples)
            {
                s->decode(getCommunicator());
                if(_filter->readerMatch(s))
                {
                    _unread.push_back(s);
                }
            }
            if(_traceLevels->data > 0)
            {
                Trace out(_traceLevels, _traceLevels->dataCat);
                out << this << ": initialized samples";
            }
            _parent->_cond.notify_all();
        }
        else
        {
            DataReaderI::initSamples(samples);
        }
    }
}

void
FilteredDataReaderI::queue(const shared_ptr<Sample>& sample, const string& facet)
{
    if(_filter->hasReaderMatch())
    {
        sample->decode(getCommunicator());
        if(_filter->readerMatch(sample))
        {
            DataReaderI::queue(sample);
        }
    }
    else
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
        out << this << ": created filtered data writer";
    }
}

void
FilteredDataWriterI::destroyImpl()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << this << ": destroyed filter data writer";
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

string
FilteredDataWriterI::toString() const
{
    ostringstream os;
    os << _filter->toString() << '@' << _parent->getName();
    return os.str();
}

void
FilteredDataWriterI::send(const shared_ptr<Sample>& sample) const
{
    _sample = sample;
    _subscribers->f(_parent->getId(), _filter->getId(), toSample(sample, getCommunicator()));
    _sample = nullptr;
}
