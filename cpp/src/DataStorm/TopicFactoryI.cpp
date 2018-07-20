// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
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

TopicFactoryI::TopicFactoryI(const shared_ptr<Instance>& instance) : _nextReaderId(0), _nextWriterId(10)
{
    _instance = instance;
    _traceLevels = _instance->getTraceLevels();
}

shared_ptr<TopicReader>
TopicFactoryI::createTopicReader(const string& name,
                                 const shared_ptr<KeyFactory>& keyFactory,
                                 const shared_ptr<TagFactory>& tagFactory,
                                 const shared_ptr<SampleFactory>& sampleFactory,
                                 const shared_ptr<FilterFactoryManager>& keyFilterFactories,
                                 const shared_ptr<FilterFactoryManager>& sampleFilterFactories)
{
    shared_ptr<TopicReaderI> reader;
    {
        lock_guard<mutex> lock(_mutex);
        reader = make_shared<TopicReaderI>(shared_from_this(),
                                           keyFactory,
                                           tagFactory,
                                           sampleFactory,
                                           keyFilterFactories,
                                           sampleFilterFactories,
                                           name,
                                           _nextReaderId++);
        reader->init();
        _readers[name].push_back(reader);
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << name << ": created topic reader";
        }
    }
    _instance->getNode()->getSubscriberForwarder()->announceTopics({ { name, { reader->getId() } } }, false);
    _instance->getTopicLookup()->announceTopicReaderAsync(name, _instance->getNode()->getProxy());
    return reader;
}

shared_ptr<TopicWriter>
TopicFactoryI::createTopicWriter(const string& name,
                                 const shared_ptr<KeyFactory>& keyFactory,
                                 const shared_ptr<TagFactory>& tagFactory,
                                 const shared_ptr<SampleFactory>& sampleFactory,
                                 const shared_ptr<FilterFactoryManager>& keyFilterFactories,
                                 const shared_ptr<FilterFactoryManager>& sampleFilterFactories)
{
    shared_ptr<TopicWriterI> writer;
    {
        lock_guard<mutex> lock(_mutex);
        writer = make_shared<TopicWriterI>(shared_from_this(),
                                           keyFactory,
                                           tagFactory,
                                           sampleFactory,
                                           keyFilterFactories,
                                           sampleFilterFactories,
                                           name,
                                           _nextWriterId++);
        writer->init();
        _writers[name].push_back(writer);
        if(_traceLevels->topic > 0)
        {
            Trace out(_traceLevels, _traceLevels->topicCat);
            out << name << ": created topic writer";
        }
    }
    _instance->getNode()->getPublisherForwarder()->announceTopics({ { name, { writer->getId() } } }, false);
    _instance->getTopicLookup()->announceTopicWriterAsync(name, _instance->getNode()->getProxy());
    return writer;
}

void
TopicFactoryI::removeTopicReader(const string& name, const shared_ptr<TopicI>& reader)
{
    lock_guard<mutex> lock(_mutex);
    if(_traceLevels->topic > 0)
    {
        Trace out(_traceLevels, _traceLevels->topicCat);
        out << name << ": destroyed topic reader";
    }
    auto& readers = _readers[name];
    readers.erase(find(readers.begin(), readers.end(), reader));
    if(readers.empty())
    {
        _readers.erase(name);
    }
}

void
TopicFactoryI::removeTopicWriter(const string& name, const shared_ptr<TopicI>& writer)
{
    lock_guard<mutex> lock(_mutex);
    if(_traceLevels->topic > 0)
    {
        Trace out(_traceLevels, _traceLevels->topicCat);
        out << name << ": destroyed topic writer";
    }
    auto& writers = _writers[name];
    writers.erase(find(writers.begin(), writers.end(), writer));
    if(writers.empty())
    {
        _writers.erase(name);
    }
}

vector<shared_ptr<TopicI>>
TopicFactoryI::getTopicReaders(const string& name) const
{
    lock_guard<mutex> lock(_mutex);
    auto p = _readers.find(name);
    if(p == _readers.end())
    {
        return vector<shared_ptr<TopicI>>();
    }
    return p->second;
}

vector<shared_ptr<TopicI>>
TopicFactoryI::getTopicWriters(const string& name) const
{
    lock_guard<mutex> lock(_mutex);
    auto p = _writers.find(name);
    if(p == _writers.end())
    {
        return vector<shared_ptr<TopicI>>();
    }
    return p->second;
}

void
TopicFactoryI::createPublisherSession(const string& topic, const shared_ptr<DataStormContract::NodePrx>& publisher)
{
    auto readers = getTopicReaders(topic);
    if(!readers.empty())
    {
        _instance->getNode()->createPublisherSession(publisher);
    }
}

void
TopicFactoryI::createSubscriberSession(const string& topic, const shared_ptr<DataStormContract::NodePrx>& subscriber)
{
    auto writers = getTopicWriters(topic);
    if(!writers.empty())
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
        DataStormContract::TopicInfo info;
        info.name = p.first;
        info.ids.reserve(p.second.size());
        for(const auto& q : p.second)
        {
            info.ids.push_back(q->getId());
        }
        readers.push_back(move(info));
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
        DataStormContract::TopicInfo info;
        info.name = p.first;
        info.ids.reserve(p.second.size());
        for(const auto& q : p.second)
        {
            info.ids.push_back(q->getId());
        }
        writers.push_back(info);
    }
    return writers;
}

shared_ptr<Ice::Communicator>
TopicFactoryI::getCommunicator() const
{
    return _instance->getCommunicator();
}
