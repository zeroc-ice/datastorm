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

class TopicReaderI;
class TopicWriterI;

class TopicFactoryI : public TopicFactory, public std::enable_shared_from_this<TopicFactoryI>
{
public:

    TopicFactoryI(const std::shared_ptr<Instance>&);

    virtual std::shared_ptr<TopicReader> getTopicReader(const std::string&,
                                                        std::function<std::shared_ptr<KeyFactory>()>,
                                                        std::function<std::shared_ptr<FilterFactory>()>,
                                                        typename Sample::FactoryType) override;

    virtual std::shared_ptr<TopicWriter> getTopicWriter(const std::string&,
                                                        std::function<std::shared_ptr<KeyFactory>()>,
                                                        std::function<std::shared_ptr<FilterFactory>()>,
                                                        typename Sample::FactoryType) override;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

    void removeTopicReader(const std::string&);
    void removeTopicWriter(const std::string&);

    std::shared_ptr<TopicReaderI> getTopicReader(const std::string&) const;
    std::shared_ptr<TopicWriterI> getTopicWriter(const std::string&) const;

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
    std::map<std::string, std::shared_ptr<TopicReaderI>> _readers;
    std::map<std::string, std::shared_ptr<TopicWriterI>> _writers;
    long long int _nextReaderId;
    long long int _nextWriterId;
};

}