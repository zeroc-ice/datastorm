// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/Instance.h>
#include <DataStorm/LookupI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/NodeSessionManager.h>
#include <DataStorm/NodeI.h>

using namespace std;
using namespace DataStormContract;
using namespace DataStormI;

LookupI::LookupI(const shared_ptr<Instance>& instance) :
    _nodeSessionManager(instance->getNodeSessionManager()),
    _topicFactory(instance->getTopicFactory()),
    _nodePrx(instance->getNode()->getProxy())
{
}

void
LookupI::announceTopicReader(Ice::Identity from, string name, shared_ptr<NodePrx> proxy, const Ice::Current& current)
{
    _nodeSessionManager->announceTopicReader(from, name, proxy, current.con);
    _topicFactory->createSubscriberSession(name, proxy, current.con);
}

void
LookupI::announceTopicWriter(Ice::Identity from, string name, shared_ptr<NodePrx> proxy, const Ice::Current& current)
{
    _nodeSessionManager->announceTopicWriter(from, name, proxy, current.con);
    _topicFactory->createPublisherSession(name, proxy, current.con);
}

void
LookupI::announceTopics(Ice::Identity from, StringSeq readers, StringSeq writers, shared_ptr<NodePrx> proxy,
                        const Ice::Current& current)
{
    _nodeSessionManager->announceTopics(from, readers, writers, proxy, current.con);
    for(auto name : readers)
    {
        _topicFactory->createSubscriberSession(name, proxy, current.con);
    }
    for(auto name : writers)
    {
        _topicFactory->createPublisherSession(name, proxy, current.con);
    }
}

shared_ptr<NodePrx>
LookupI::createSession(shared_ptr<NodePrx> node, const Ice::Current& current)
{
    _nodeSessionManager->createOrGet(move(node), current.con, true);
    return _nodePrx;
}
