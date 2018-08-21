// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Ice.h>

#include <DataStorm/Contract.h>

namespace DataStorm
{

class TopicFactory;

}

namespace DataStormI
{

class TopicFactoryI;
class SessionManager;
class TraceLevels;
class ForwarderManager;
class NodeI;
class CallbackExecutor;

class Instance : public std::enable_shared_from_this<Instance>
{
public:

    Instance(const std::shared_ptr<Ice::Communicator>&);

    void init();

    std::shared_ptr<SessionManager>
    getSessionManager() const
    {
        return _sessionManager;
    }

    std::shared_ptr<Ice::Communicator>
    getCommunicator() const
    {
        return _communicator;
    }

    std::shared_ptr<Ice::ObjectAdapter>
    getObjectAdapter() const
    {
        return _adapter;
    }

    std::shared_ptr<ForwarderManager>
    getForwarderManager() const
    {
        return _forwarderManager;
    }

    std::shared_ptr<Ice::ObjectAdapter>
    getMulticastObjectAdapter() const
    {
        return _multicastAdapter;
    }

    std::shared_ptr<DataStormContract::TopicLookupPrx>
    getTopicLookup() const
    {
        return _lookup;
    }

    std::shared_ptr<TopicFactoryI>
    getTopicFactory() const
    {
        return _topicFactory;
    }

    std::shared_ptr<TraceLevels>
    getTraceLevels() const
    {
        return _traceLevels;
    }

    std::shared_ptr<NodeI>
    getNode() const
    {
        return _node;
    }

    std::shared_ptr<CallbackExecutor>
    getCallbackExecutor()
    {
        return _executor;
    }

    void destroy(bool);

private:

    std::shared_ptr<TopicFactoryI> _topicFactory;
    std::shared_ptr<SessionManager> _sessionManager;
    std::shared_ptr<ForwarderManager> _forwarderManager;
    std::shared_ptr<NodeI> _node;
    std::shared_ptr<Ice::Communicator> _communicator;
    std::shared_ptr<Ice::ObjectAdapter> _adapter;
    std::shared_ptr<Ice::ObjectAdapter> _collocatedAdapter;
    std::shared_ptr<Ice::ObjectAdapter> _multicastAdapter;
    std::shared_ptr<DataStormContract::TopicLookupPrx> _lookup;
    std::shared_ptr<TraceLevels> _traceLevels;
    std::shared_ptr<CallbackExecutor> _executor;
};

}
