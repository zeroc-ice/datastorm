// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/Instance.h>
#include <DataStorm/SessionManager.h>
#include <DataStorm/LookupI.h>
#include <DataStorm/TraceUtil.h>

#include <IceUtil/UUID.h>

using namespace std;
using namespace DataStormInternal;

Instance::Instance(const shared_ptr<Ice::Communicator>& communicator) : _communicator(communicator)
{
    if(!_communicator)
    {
        _communicator = Ice::initialize();
    }

    shared_ptr<Ice::Properties> properties = _communicator->getProperties();
    properties->setProperty("DataStorm.Endpoints", "tcp");
    properties->setProperty("DataStorm.ThreadPool.SizeMax", "1");
    properties->setProperty("DataStormCollocated.AdapterId", IceUtil::generateUUID());
    properties->setProperty("DataStormMulticast.Endpoints", "udp -h 239.255.0.1 -p 12345");
    properties->setProperty("DataStormMulticast.ProxyOptions", "-d");
    properties->setProperty("DataStormMulticast.ThreadPool.SizeMax", "1");

    _adapter = _communicator->createObjectAdapter("DataStorm");
    _collocatedAdapter = _communicator->createObjectAdapter("DataStormCollocated");
    _multicastAdapter = _communicator->createObjectAdapter("DataStormMulticast");

    _sessionManager = make_shared<SessionManager>();

    _forwarderManager = make_shared<ForwarderManager>(_collocatedAdapter);
    _collocatedAdapter->addDefaultServant(_forwarderManager, "forwarders");

    _traceLevels = make_shared<TraceLevels>(_communicator);
}

void
Instance::init(const shared_ptr<TopicFactoryI>& factory)
{
    _topicFactory = factory;
    auto lookup = _multicastAdapter->add(make_shared<TopicLookupI>(factory), {"DataStorm", "Lookup"});
    _lookup = Ice::uncheckedCast<DataStormContract::TopicLookupPrx>(lookup->ice_collocationOptimized(false));
    _adapter->activate();
    _collocatedAdapter->activate();
    _multicastAdapter->activate();
}
