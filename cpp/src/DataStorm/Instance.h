// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Ice.h>

#include <Internal.h>

namespace DataStorm
{

class TopicFactory;

}

namespace DataStormInternal
{

class TopicFactoryI;
class SessionManager;
class TraceLevels;
class ForwarderManager;

class Instance
{
public:

    Instance(const std::shared_ptr<Ice::Communicator>&);

    void init(const std::weak_ptr<DataStorm::TopicFactory>&, const std::shared_ptr<TopicFactoryI>&);

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
    getTopicFactoryI() const
    {
        return _topicFactoryI;
    }

    std::shared_ptr<DataStorm::TopicFactory>
    getTopicFactory() const
    {
        return _topicFactory.lock();
    }

    std::shared_ptr<TraceLevels>
    getTraceLevels() const
    {
        return _traceLevels;
    }

private:

    std::weak_ptr<DataStorm::TopicFactory> _topicFactory;
    std::shared_ptr<TopicFactoryI> _topicFactoryI;
    std::shared_ptr<SessionManager> _sessionManager;
    std::shared_ptr<ForwarderManager> _forwarderManager;
    std::shared_ptr<Ice::Communicator> _communicator;
    std::shared_ptr<Ice::ObjectAdapter> _adapter;
    std::shared_ptr<Ice::ObjectAdapter> _collocatedAdapter;
    std::shared_ptr<Ice::ObjectAdapter> _multicastAdapter;
    std::shared_ptr<DataStormContract::TopicLookupPrx> _lookup;
    std::shared_ptr<TraceLevels> _traceLevels;
};

}