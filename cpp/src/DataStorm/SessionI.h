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
class DataElementI;
class Instance;
class TraceLevels;

class SessionI : virtual public DataStormContract::Session, public std::enable_shared_from_this<SessionI>
{
    template<typename K> class Subscribers
    {
    public:

        Subscribers(const std::shared_ptr<K>& key) : _key(key)
        {
        }

        void add(DataElementI* element)
        {
            _subscribers.insert(element);
        }

        void remove(DataElementI* element)
        {
            _subscribers.erase(element);
        }

        std::shared_ptr<K>
        get() const
        {
            return _key;
        }

        const std::set<DataElementI*>&
        getSubscribers() const
        {
            return _subscribers;
        }

    private:

        const std::shared_ptr<K> _key;
        std::set<DataElementI*> _subscribers;
    };

    class TopicSubscribers
    {
    public:

        TopicSubscribers(TopicI* topic) : _topic(topic)
        {
        }

        Subscribers<Key>* getKey(long long int id, const std::shared_ptr<Key>& key = nullptr)
        {
            auto p = _keys.find(id);
            if(p != _keys.end())
            {
                return &p->second;
            }
            else if(key)
            {
                return &_keys.emplace(id, Subscribers<Key>(key)).first->second;
            }
            else
            {
                return 0;
            }
        }

        Subscribers<Key> removeKey(long long id)
        {
            auto p = _keys.find(id);
            if(p != _keys.end())
            {
                Subscribers<Key> tmp(std::move(p->second));
                _keys.erase(p);
                return tmp;
            }
            return Subscribers<Key>(nullptr);
        }

        Subscribers<Filter>* getFilter(long long int id, const std::shared_ptr<Filter>& filter = nullptr)
        {
            auto p = _filters.find(id);
            if(p != _filters.end())
            {
                return &p->second;
            }
            else if(filter)
            {
                return &_filters.emplace(id, Subscribers<Filter>(filter)).first->second;
            }
            else
            {
                return 0;
            }
        }

        Subscribers<Filter> removeFilter(long long id)
        {
            auto p = _filters.find(id);
            if(p != _filters.end())
            {
                Subscribers<Filter> tmp(std::move(p->second));
                _filters.erase(p);
                return tmp;
            }
            return Subscribers<Filter>(nullptr);
        }

        const std::map<long long int, Subscribers<Key>>&
        getKeys() const
        {
            return _keys;
        }

        const std::map<long long int, Subscribers<Filter>>&
        getFilters() const
        {
            return _filters;
        }

        TopicI*
        get() const
        {
            return _topic;
        }

    private:

        TopicI* _topic;
        std::map<long long int, Subscribers<Key>> _keys;
        std::map<long long int, Subscribers<Filter>> _filters;
    };

public:

    SessionI(PeerI*, const std::shared_ptr<DataStormContract::PeerPrx>&);
    void init();

    virtual void announceTopics(DataStormContract::TopicInfoSeq, const Ice::Current&);
    virtual void attachTopics(DataStormContract::TopicInfoAndContentSeq, const Ice::Current&);
    virtual void detachTopic(long long int, const Ice::Current&);

    virtual void announceKey(long long int, DataStormContract::KeyInfo, const Ice::Current&);
    virtual void announceFilter(long long int, DataStormContract::FilterInfo, const Ice::Current&);
    virtual void attachKeysAndFilters(long long int,
                                      long long int,
                                      DataStormContract::KeyInfoAndSamplesSeq,
                                      DataStormContract::FilterInfoSeq,
                                      const Ice::Current&);
    virtual void detachKey(long long int, long long int, const Ice::Current&);
    virtual void detachFilter(long long int, long long int, const Ice::Current&);

    virtual void destroy(const Ice::Current&);

    virtual void connected(const std::shared_ptr<DataStormContract::SessionPrx>&,
                           const std::shared_ptr<Ice::Connection>&,
                           const DataStormContract::TopicInfoSeq&);

    virtual void disconnected(std::exception_ptr);

    std::shared_ptr<DataStormContract::SessionPrx> getSession() const;
    std::shared_ptr<DataStormContract::SessionPrx> getSessionNoLock() const;

    virtual long long int getLastId(long long int) const;

    std::shared_ptr<DataStormContract::SessionPrx> getProxy() const
    {
        return _proxy;
    }

    std::shared_ptr<DataStormContract::PeerPrx> getPeer() const
    {
        return _peer;
    }

    std::unique_lock<std::mutex>& getLock()
    {
        return *_lock;
    }

    void subscribe(long long int, TopicI*);
    void unsubscribe(long long int, bool);
    void disconnect(long long int);

    void subscribeToKey(long long int, long long int, const std::shared_ptr<Key>&, DataElementI*);
    void unsubscribeFromKey(long long int, long long int, DataElementI*);
    void disconnectFromKey(long long int, long long int, DataElementI*);

    void subscribeToFilter(long long int, long long int, const std::shared_ptr<Filter>&, DataElementI*);
    void unsubscribeFromFilter(long long int, long long int, DataElementI*);
    void disconnectFromFilter(long long int, long long int, DataElementI*);

protected:

    void runWithTopic(const std::string&, std::function<void (const std::shared_ptr<TopicI>&)>);
    void runWithTopic(long long int id, std::function<void (TopicSubscribers&)>);

    virtual std::shared_ptr<TopicI> getTopic(const std::string&) const = 0;

    const std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    mutable std::mutex _mutex;
    PeerI* _parent;
    std::shared_ptr<DataStormContract::SessionPrx> _proxy;
    const std::shared_ptr<DataStormContract::PeerPrx> _peer;

    std::map<long long int, TopicSubscribers> _topics;
    std::unique_lock<std::mutex>* _lock;

    std::shared_ptr<DataStormContract::SessionPrx> _session;
    std::shared_ptr<Ice::Connection> _connection;
};

class SubscriberSessionI : public SessionI, public DataStormContract::SubscriberSession
{
public:

    SubscriberSessionI(SubscriberI*, const std::shared_ptr<DataStormContract::PeerPrx>&);

    virtual void i(long long int, DataStormContract::DataSamplesSeq, const Ice::Current&);
    virtual void s(long long int, long long int, std::shared_ptr<DataStormContract::DataSample>, const Ice::Current&);
    virtual void f(long long int, long long int, std::shared_ptr<DataStormContract::DataSample>, const Ice::Current&);

    virtual long long int getLastId(long long int) const;
    bool setLastId(long long int, long long int);

private:

    virtual std::shared_ptr<TopicI> getTopic(const std::string&) const;

    std::map<long long int, long long int> _lastIds;
};

class PublisherSessionI : public SessionI, public DataStormContract::PublisherSession
{
public:

    PublisherSessionI(PublisherI*, const std::shared_ptr<DataStormContract::PeerPrx>&);

private:

    virtual std::shared_ptr<TopicI> getTopic(const std::string&) const;
};

}