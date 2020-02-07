//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#include <DataStorm/Instance.h>
#include <DataStorm/LookupI.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/NodeSessionManager.h>
#include <DataStorm/NodeI.h>

using namespace std;
using namespace DataStormContract;
using namespace DataStormI;

LookupI::LookupI(shared_ptr<NodeSessionManager> nodeSessionManager,
                 shared_ptr<TopicFactoryI> topicFactory,
                 shared_ptr<NodePrx> nodePrx) :
    _nodeSessionManager(move(nodeSessionManager)),
    _topicFactory(move(topicFactory)),
    _nodePrx(move(nodePrx))
{
}

void
LookupI::announceTopicReader(string name, shared_ptr<NodePrx> proxy, const Ice::Current& current)
{
    _nodeSessionManager->announceTopicReader(name,proxy, current.con);
    _topicFactory->createSubscriberSession(name, proxy, current.con);
}

void
LookupI::announceTopicWriter(string name, shared_ptr<NodePrx> proxy, const Ice::Current& current)
{
    _nodeSessionManager->announceTopicWriter(name, proxy, current.con);
    _topicFactory->createPublisherSession(name, proxy, current.con);
}

void
LookupI::announceTopics(StringSeq readers, StringSeq writers, shared_ptr<NodePrx> proxy, const Ice::Current& current)
{
    _nodeSessionManager->announceTopics(readers, writers, proxy, current.con);
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
