// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/LookupI.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TopicI.h>
#include <DataStorm/PeerI.h>

using namespace std;
using namespace DataStormInternal;

TopicLookupI::TopicLookupI(shared_ptr<TopicFactoryI> factory) : _factory(factory)
{
}

void
TopicLookupI::announceTopicSubscriber(string name, shared_ptr<DataStormContract::SubscriberPrx> proxy, const Ice::Current&)
{
    _factory->createSession(name, proxy);
}

void
TopicLookupI::announceTopicPublisher(string name, shared_ptr<DataStormContract::PublisherPrx> proxy, const Ice::Current&)
{
    _factory->createSession(name, proxy);
}
