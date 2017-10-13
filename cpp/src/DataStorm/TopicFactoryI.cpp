// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/TraceUtil.h>
#include <DataStorm/Instance.h>

using namespace std;
using namespace DataStormInternal;

TopicFactoryI::TopicFactoryI(const shared_ptr<Instance>& instance) : _nextReaderId(0), _nextWriterId(0)
{
    _instance = instance;
    _traceLevels = _instance->getTraceLevels();
}

shared_ptr<TopicReader>
TopicFactoryI::getTopicReader(const string& name,
                              function<shared_ptr<KeyFactory>()> createKeyFactory,
                              function<shared_ptr<FilterFactory>()> createFilterFactory,
                              typename Sample::FactoryType sampleFactory)
{
    shared_ptr<TopicReaderI> reader;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _readers.find(name);
        if(p != _readers.end())
        {
            return p->second;
        }

        reader = make_shared<TopicReaderI>(shared_from_this(),
                                           createKeyFactory(),
                                           createFilterFactory(),
                                           move(sampleFactory),
                                           name,
                                           _nextReaderId++);
        _readers.insert(make_pair(name, reader));
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << name << ": created topic reader";
        }
    }
    _instance->getNode()->getSubscriberForwarder()->announceTopics({ { reader->getId(), name } });
    _instance->getTopicLookup()->announceTopicReaderAsync(reader->getName(), _instance->getNode()->getProxy());
    return reader;
}

shared_ptr<TopicWriter>
TopicFactoryI::getTopicWriter(const string& name,
                              function<shared_ptr<KeyFactory>()> createKeyFactory,
                              function<shared_ptr<FilterFactory>()> createFilterFactory,
                              typename Sample::FactoryType sampleFactory)
{
    shared_ptr<TopicWriterI> writer;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _writers.find(name);
        if(p != _writers.end())
        {
            return p->second;
        }

        writer = make_shared<TopicWriterI>(shared_from_this(),
                                           createKeyFactory(),
                                           createFilterFactory(),
                                           move(sampleFactory),
                                           name,
                                           _nextWriterId++);
        _writers.insert(make_pair(name, writer));
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << name << ": created topic writer";
        }
    }
    _instance->getNode()->getPublisherForwarder()->announceTopics({ { writer->getId(), name } });
    _instance->getTopicLookup()->announceTopicWriterAsync(writer->getName(), _instance->getNode()->getProxy());
    return writer;
}

void
TopicFactoryI::removeTopicReader(const string& name)
{
    lock_guard<mutex> lock(_mutex);
    if(_traceLevels->topic > 0)
    {
        Trace out(_traceLevels, _traceLevels->topicCat);
        out << name << ": destroyed topic reader";
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
        out << name << ": destroyed topic writer";
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
TopicFactoryI::createPublisherSession(const string& topic, const shared_ptr<DataStormContract::NodePrx>& publisher)
{
    auto reader = getTopicReader(topic);
    if(reader)
    {
        _instance->getNode()->createPublisherSession(publisher);
    }
}

void
TopicFactoryI::createSubscriberSession(const string& topic, const shared_ptr<DataStormContract::NodePrx>& subscriber)
{
    auto writer = getTopicWriter(topic);
    if(writer)
    {
        _instance->getNode()->createSubscriberSession(subscriber);
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
