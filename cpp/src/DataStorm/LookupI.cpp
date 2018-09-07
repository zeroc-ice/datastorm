// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/LookupI.h>
#include <DataStorm/TopicFactoryI.h>

using namespace std;
using namespace DataStormI;

TopicLookupI::TopicLookupI(const shared_ptr<TopicFactoryI>& factory) : _factory(factory)
{
}

void
TopicLookupI::announceTopicReader(string name, shared_ptr<DataStormContract::NodePrx> proxy, const Ice::Current&)
{
    _factory->createSubscriberSession(name, proxy);
}

void
TopicLookupI::announceTopicWriter(string name, shared_ptr<DataStormContract::NodePrx> proxy, const Ice::Current&)
{
    _factory->createPublisherSession(name, proxy);
}
