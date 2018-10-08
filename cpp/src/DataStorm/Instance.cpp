// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
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

Instance::Instance(const shared_ptr<Ice::Communicator>& communicator) : _communicator(communicator), _shutdown(false)
{
    shared_ptr<Ice::Properties> properties = _communicator->getProperties();
    if(properties->getProperty("DataStorm.Node.Server.Endpoints").empty())
    {
        properties->setProperty("DataStorm.Node.Server.Endpoints", "tcp");
    }
    properties->setProperty("DataStorm.Node.Server.ThreadPool.SizeMax", "1");
    properties->setProperty("DataStorm.Node.Multicast.Endpoints", "udp -h 239.255.0.1 -p 10000");
    properties->setProperty("DataStorm.Node.Multicast.ProxyOptions", "-d");
    properties->setProperty("DataStorm.Node.Multicast.ThreadPool.SizeMax", "1");

    _adapter = _communicator->createObjectAdapter("DataStorm.Node.Server");
    _multicastAdapter = _communicator->createObjectAdapter("DataStorm.Node.Multicast");

    //
    // Create a collocated object adapter with a random name to prevent user configuration
    // of the adapter.
    //
    string collocated = IceUtil::generateUUID();
    properties->setProperty(collocated + ".AdapterId", collocated);
    _collocatedAdapter = _communicator->createObjectAdapter(collocated);

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
Instance::shutdown()
{
    unique_lock<mutex> lock(_mutex);
    _shutdown = true;
    _cond.notify_all();
    _topicFactory->shutdown();
}

bool
Instance::isShutdown() const
{
    unique_lock<mutex> lock(_mutex);
    return _shutdown;
}

void
Instance::waitForShutdown() const
{
    unique_lock<mutex> lock(_mutex);
    _cond.wait(lock, [&]() { return _shutdown; }); // Wait until shutdown is called
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
    _node->destroy(ownsCommunicator);
}
