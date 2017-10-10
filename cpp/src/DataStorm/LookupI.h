// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/Contract.h>

namespace DataStormInternal
{

class TopicFactoryI;
class Instance;

class TopicLookupI : public DataStormContract::TopicLookup
{
public:

    TopicLookupI(const std::shared_ptr<TopicFactoryI>&);

    virtual void announceTopicPublisher(std::string, std::shared_ptr<DataStormContract::PublisherPrx>,
                                        const Ice::Current&);
    virtual void announceTopicSubscriber(std::string, std::shared_ptr<DataStormContract::SubscriberPrx>,
                                         const Ice::Current&);

private:

    std::shared_ptr<TopicFactoryI> _factory;
};

}
