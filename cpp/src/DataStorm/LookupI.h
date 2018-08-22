// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Contract.h>

namespace DataStormI
{

class TopicFactoryI;
class Instance;

class TopicLookupI : public DataStormContract::TopicLookup
{
public:

    TopicLookupI(const std::shared_ptr<TopicFactoryI>&);

    virtual void announceTopicReader(std::string, std::shared_ptr<DataStormContract::NodePrx>, const Ice::Current&);
    virtual void announceTopicWriter(std::string, std::shared_ptr<DataStormContract::NodePrx>, const Ice::Current&);

private:

    std::shared_ptr<TopicFactoryI> _factory;
};

}
