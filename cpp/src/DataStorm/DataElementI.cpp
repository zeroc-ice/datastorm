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
    _communicator(parent->getCommunicator())
{
}

DataReaderI::DataReaderI(TopicReaderI* topic) : DataElementI(topic), _parent(topic), _subscriber(topic->getSubscriber())
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
    while(_unread.empty())
    {
        _parent->_cond.wait(lock);
    }
    shared_ptr<Sample> sample = _unread.front();
    _unread.pop_front();
    return sample;
}

void
DataReaderI::queue(shared_ptr<Sample> sample)
{
    if(sample->type == DataStorm::SampleType::Add)
    {
        ++_instanceCount;
    }
    _unread.push_back(sample);
    _all.push_back(sample);
}

DataWriterI::DataWriterI(TopicWriterI* topic) : DataElementI(topic), _parent(topic), _publisher(topic->getPublisher())
{
}

void
DataWriterI::add(Value value)
{
    publish(make_shared<DataStormContract::DataSample>(0, 0, DataStormContract::SampleType::Add, value));
}

void
DataWriterI::update(Value value)
{
    publish(make_shared<DataStormContract::DataSample>(0, 0, DataStormContract::SampleType::Update, value));
}

void
DataWriterI::remove()
{
    publish(make_shared<DataStormContract::DataSample>(0, 0, DataStormContract::SampleType::Remove, Ice::ByteSeq()));
}

void
DataWriterI::publish(shared_ptr<DataStormContract::DataSample> sample)
{
    lock_guard<mutex> lock(_parent->_mutex);
    sample->id = ++_parent->_id;
    sample->timestamp = IceUtil::Time::now().toMilliSeconds();
    _all.push_back(sample);
    send(_all.back());
}

KeyDataReaderI::KeyDataReaderI(TopicReaderI* parent, const shared_ptr<Key>& key) :
    DataReaderI(parent), _key(key)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created key data reader `" << _parent << '/' << _key << "'";
    }
    _subscriber->subscribe(key);
}

void
KeyDataReaderI::destroy()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed key data reader `" << _parent << '/' << _key << "'";
    }
    _subscriber->unsubscribe(_key);
    _parent->remove(_key, shared_from_this());
}

void
KeyDataReaderI::waitForWriters(int count)
{
     _subscriber->waitForKeyListeners(_key, count);
}

bool
KeyDataReaderI::hasWriters()
{
    return _subscriber->hasKeyListeners(_key);
}

DataStormContract::KeyInfo
KeyDataReaderI::getKeyInfo() const
{
    return { _key->marshal(), 0 };
}

KeyDataWriterI::KeyDataWriterI(TopicWriterI* topic, const std::shared_ptr<Key>& key) :
    DataWriterI(topic),
    _key(key),
    _subscribers(Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_publisher->addForwarder(key)))
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created key data writer `" << _parent << '/' << _key << "'";
    }
    _publisher->subscribe(key);
}

void
KeyDataWriterI::destroy()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed key data writer `" << _parent << '/' << _key << "'";
    }
    _publisher->removeForwarder(_key);
    _publisher->unsubscribe(_key);
    _parent->remove(_key, shared_from_this());
}

void
KeyDataWriterI::waitForReaders(int count) const
{
    _publisher->waitForKeyListeners(_key, count);
}

bool
KeyDataWriterI::hasReaders() const
{
    return _publisher->hasKeyListeners(_key);
}

DataStormContract::KeyInfo
KeyDataWriterI::getKeyInfo() const
{
    return { _key->marshal(), 0 };
}

void
KeyDataWriterI::init(long long int lastId, DataStormContract::DataSamplesSeq& seq)
{
    auto data = make_shared<DataStormContract::DataSamples>();
    data->key = _key->marshal();
    if(lastId < 0)
    {
        data->samples.reserve(_all.size());
    }
    for(const auto& p : _all)
    {
        if(p->id > lastId)
        {
            data->samples.push_back(p);
        }
    }
    seq.emplace_back(data);
}

void
KeyDataWriterI::send(const std::shared_ptr<DataStormContract::DataSample>& sample) const
{
    _subscribers->s(_parent->getName(), _key->marshal(), sample);
}

FilteredDataReaderI::FilteredDataReaderI(TopicReaderI* topic, const string& filter) :
    DataReaderI(topic),
    _filter(filter)
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created filtered data reader `" << _parent << '/' << _filter << "'";
    }
    _subscriber->subscribe(_filter);
}

void
FilteredDataReaderI::destroy()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed filter data reader `" << _parent << '/' << _filter << "'";
    }
    _subscriber->unsubscribe(_filter);
    _parent->removeFiltered(_filter, shared_from_this());
}

void
FilteredDataReaderI::waitForWriters(int count)
{
     _subscriber->waitForFilteredListeners(_filter, count);
}

bool
FilteredDataReaderI::hasWriters()
{
     return _subscriber->hasFilteredListeners(_filter);
}

FilteredDataWriterI::FilteredDataWriterI(TopicWriterI* topic, const std::string& filter) :
    DataWriterI(topic),
    _filter(filter),
    _subscribers(Ice::uncheckedCast<DataStormContract::SubscriberSessionPrx>(_publisher->addForwarder(_filter)))
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "created filtered data writer `" << _parent << '/' << _filter << "'";
    }
    _publisher->subscribe(_filter);
}

void
FilteredDataWriterI::destroy()
{
    if(_traceLevels->data > 0)
    {
        Trace out(_traceLevels, _traceLevels->dataCat);
        out << "destroyed filter data writer `" << _parent << '/' << _filter << "'";
    }
    _publisher->removeForwarder(_filter);
    _publisher->unsubscribe(_filter);
    _parent->removeFiltered(_filter, shared_from_this());
}

void
FilteredDataWriterI::waitForReaders(int count) const
{
     _publisher->waitForFilteredListeners(_filter, count);
}

bool
FilteredDataWriterI::hasReaders() const
{
    return _publisher->hasFilteredListeners(_filter);
}

void
FilteredDataWriterI::send(const std::shared_ptr<DataStormContract::DataSample>& sample) const
{
    _subscribers->f(_parent->getName(), _filter, sample);
}
