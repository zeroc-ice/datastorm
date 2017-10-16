// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/InternalI.h>
#include <DataStorm/Contract.h>

namespace DataStormInternal
{

class Instance;
class TraceLevels;

class TopicI;
class TopicReaderI;
class TopicWriterI;

class TopicFactoryI : public TopicFactory, public std::enable_shared_from_this<TopicFactoryI>
{
public:

    TopicFactoryI(const std::shared_ptr<Instance>&);

    virtual std::shared_ptr<TopicReader> createTopicReader(const std::string&,
                                                           const std::shared_ptr<KeyFactory>&,
                                                           const std::shared_ptr<FilterFactory>&,
                                                           typename Sample::FactoryType) override;

    virtual std::shared_ptr<TopicWriter> createTopicWriter(const std::string&,
                                                           const std::shared_ptr<KeyFactory>&,
                                                           const std::shared_ptr<FilterFactory>&,
                                                           typename Sample::FactoryType) override;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

    void removeTopicReader(const std::string&, const std::shared_ptr<TopicI>&);
    void removeTopicWriter(const std::string&, const std::shared_ptr<TopicI>&);

    std::vector<std::shared_ptr<TopicI>> getTopicReaders(const std::string&) const;
    std::vector<std::shared_ptr<TopicI>> getTopicWriters(const std::string&) const;

    void createSubscriberSession(const std::string&, const std::shared_ptr<DataStormContract::NodePrx>&);
    void createPublisherSession(const std::string&, const std::shared_ptr<DataStormContract::NodePrx>&);

    std::shared_ptr<Instance> getInstance() const
    {
        return _instance;
    }

    DataStormContract::TopicInfoSeq getTopicReaders() const;
    DataStormContract::TopicInfoSeq getTopicWriters() const;

private:

    mutable std::mutex _mutex;
    std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    std::map<std::string, std::vector<std::shared_ptr<TopicI>>> _readers;
    std::map<std::string, std::vector<std::shared_ptr<TopicI>>> _writers;
    long long int _nextReaderId;
    long long int _nextWriterId;
};

}