// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/Instance.h>
#include <DataStorm/SessionManager.h>
#include <DataStorm/LookupI.h>
#include <DataStorm/TraceUtil.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/NodeI.h>
#include <DataStorm/CallbackExecutor.h>

#include <IceUtil/UUID.h>

using namespace std;
using namespace DataStormI;

Instance::Instance(const shared_ptr<Ice::Communicator>& communicator) : _communicator(communicator)
{
    shared_ptr<Ice::Properties> properties = _communicator->getProperties();
    if(properties->getProperty("DataStorm.Server.Endpoints").empty())
    {
        properties->setProperty("DataStorm.Server.Endpoints", "tcp");
    }
    properties->setProperty("DataStorm.Server.ThreadPool.SizeMax", "1");
    properties->setProperty("DataStorm.Collocated.AdapterId", IceUtil::generateUUID());
    properties->setProperty("DataStorm.Multicast.Endpoints", "udp -h 239.255.0.1 -p 10000");
    properties->setProperty("DataStorm.Multicast.ProxyOptions", "-d");
    properties->setProperty("DataStorm.Multicast.ThreadPool.SizeMax", "1");

    _adapter = _communicator->createObjectAdapter("DataStorm.Server");
    _collocatedAdapter = _communicator->createObjectAdapter("DataStorm.Collocated");
    _multicastAdapter = _communicator->createObjectAdapter("DataStorm.Multicast");

    _executor = make_shared<CallbackExecutor>();
    _sessionManager = make_shared<SessionManager>(_executor);

    _forwarderManager = make_shared<ForwarderManager>(_collocatedAdapter);
    _collocatedAdapter->addDefaultServant(_forwarderManager, "forwarders");

    _traceLevels = make_shared<TraceLevels>(_communicator);
}

void
Instance::init()
{
    _node = make_shared<NodeI>(shared_from_this());
    _node->init();

    _topicFactory = make_shared<TopicFactoryI>(shared_from_this());

    auto lookup = _multicastAdapter->add(make_shared<TopicLookupI>(_topicFactory), {"DataStorm", "Lookup"});
    _lookup = Ice::uncheckedCast<DataStormContract::TopicLookupPrx>(lookup->ice_collocationOptimized(false));

    _adapter->activate();
    _collocatedAdapter->activate();
    _multicastAdapter->activate();
}

void
Instance::destroy(bool ownsCommunicator)
{
    if(ownsCommunicator)
    {
        _communicator->destroy();
    }
    else
    {
        _adapter->destroy();
        _collocatedAdapter->destroy();
        _multicastAdapter->destroy();
    }
    _node->destroy();
}
