// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/ForwarderManager.h>

using namespace std;
using namespace DataStormInternal;

ForwarderManager::ForwarderManager(const shared_ptr<Ice::ObjectAdapter>& adapter) :
    _adapter(adapter), _nextId(0)
{
}

shared_ptr<Ice::ObjectPrx>
ForwarderManager::add(Forwarder* forwarder)
{
    lock_guard<mutex> lock(_mutex);
    ostringstream os;
    os << _nextId++;
    _forwarders.emplace(os.str(), forwarder);
    try
    {
        return _adapter->createProxy({ os.str(), "forwarders"});
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
        return nullptr;
    }
}

void
ForwarderManager::remove(const Ice::Identity& id)
{
    lock_guard<mutex> lock(_mutex);
    _forwarders.erase(id.name);
}

bool
ForwarderManager::ice_invoke(Ice::ByteSeq inEncaps, Ice::ByteSeq&, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    auto p = _forwarders.find(current.id.name);
    if(p == _forwarders.end())
    {
        throw Ice::ObjectNotExistException(__FILE__, __LINE__, current.id, current.facet, current.operation);
    }
    p->second->forward(inEncaps, current);
    return true;
}
