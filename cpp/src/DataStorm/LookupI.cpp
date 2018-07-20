// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/LookupI.h>
#include <DataStorm/TopicFactoryI.h>

using namespace std;
using namespace DataStormInternal;

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
