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

namespace DataStormInternal
{

class TopicFactoryI;
class SessionManager;
class TraceLevels;

class Instance
{
public:

    Instance(std::shared_ptr<Ice::Communicator>);

    void init(std::shared_ptr<TopicFactoryI>);

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

    std::shared_ptr<Ice::ObjectAdapter>
    getCollocatedObjectAdapter() const
    {
        return _collocatedAdapter;
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

private:

    std::shared_ptr<TopicFactoryI> _topicFactory;
    std::shared_ptr<SessionManager> _sessionManager;
    std::shared_ptr<Ice::Communicator> _communicator;
    std::shared_ptr<Ice::ObjectAdapter> _adapter;
    std::shared_ptr<Ice::ObjectAdapter> _collocatedAdapter;
    std::shared_ptr<Ice::ObjectAdapter> _multicastAdapter;
    std::shared_ptr<DataStormContract::TopicLookupPrx> _lookup;
    std::shared_ptr<TraceLevels> _traceLevels;
};

}