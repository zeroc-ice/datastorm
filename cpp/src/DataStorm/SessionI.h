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
#include <DataStorm/PeerI.h>

#include <Internal.h>

namespace DataStormInternal
{

class TopicI;
class TopicReaderI;
class TopicWriterI;
class Instance;
class TraceLevels;

class SessionI : virtual public DataStormContract::Session, public std::enable_shared_from_this<SessionI>
{
public:

    SessionI(PeerI*, std::shared_ptr<DataStormContract::PeerPrx>);
    void init();

    virtual void initTopics(DataStormContract::StringSeq, const Ice::Current&);
    virtual void addTopic(std::string, const Ice::Current&);
    virtual void removeTopic(std::string, const Ice::Current&);

    virtual void initKeysAndFilters(std::string,
                                    long long int,
                                    DataStormContract::KeyInfoSeq,
                                    DataStormContract::StringSeq,
                                    const Ice::Current&);

    virtual void attachKey(std::string, long long int, DataStormContract::KeyInfo, const Ice::Current&);
    virtual void detachKey(std::string, DataStormContract::Key, const Ice::Current&);

    virtual void attachFilter(std::string, long long int, std::string, const Ice::Current&);
    virtual void detachFilter(std::string, std::string, const Ice::Current&);

    virtual void destroy(const Ice::Current&);

    virtual void connected(std::shared_ptr<DataStormContract::SessionPrx>, std::shared_ptr<Ice::Connection>,
                           const DataStormContract::StringSeq&);
    virtual void disconnected(std::exception_ptr);

    void checkTopic(std::shared_ptr<TopicI>);

    std::shared_ptr<DataStormContract::SessionPrx> getSession() const;
    virtual long long int getLastId(const std::string&);

    std::shared_ptr<DataStormContract::SessionPrx> getProxy() const
    {
        return _proxy;
    }

    std::shared_ptr<DataStormContract::PeerPrx> getPeer() const
    {
        return _peer;
    }

protected:

    friend class TopicPeer;

    virtual std::shared_ptr<TopicI> getTopic(const std::string&) const = 0;

    const std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    mutable std::mutex _mutex;
    PeerI* _parent;
    std::shared_ptr<DataStormContract::SessionPrx> _proxy;
    const std::shared_ptr<DataStormContract::PeerPrx> _peer;
    std::set<std::string> _topics;

    std::map<TopicPeer*, std::set<TopicPeer::Filter*>> _filters;
    std::map<TopicPeer*, std::set<TopicPeer::Listener*>> _listeners;

    std::shared_ptr<DataStormContract::SessionPrx> _session;
    std::shared_ptr<Ice::Connection> _connection;
};

class SubscriberSessionI : public SessionI, public DataStormContract::SubscriberSession
{
public:

    SubscriberSessionI(SubscriberI*, std::shared_ptr<DataStormContract::PeerPrx>);

    virtual void i(std::string, DataStormContract::DataSamplesSeq, const Ice::Current&);
    virtual void s(std::string, DataStormContract::Key, std::shared_ptr<DataStormContract::DataSample>, const Ice::Current&);
    virtual void f(std::string, std::string, std::shared_ptr<DataStormContract::DataSample>, const Ice::Current&);

    virtual long long int getLastId(const std::string&);
    bool setLastId(const std::string&, long long int);

private:

    virtual std::shared_ptr<TopicI> getTopic(const std::string&) const;

    std::map<std::string, long long int> _lastIds;
};

class PublisherSessionI : public SessionI, public DataStormContract::PublisherSession
{
public:

    PublisherSessionI(PublisherI*, std::shared_ptr<DataStormContract::PeerPrx>);

private:

    virtual std::shared_ptr<TopicI> getTopic(const std::string&) const;
};

}