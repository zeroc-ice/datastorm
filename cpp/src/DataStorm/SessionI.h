// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Ice.h>
#include <DataStorm/NodeI.h>

#include <DataStorm/Contract.h>

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

        void add(DataElementI* element, const std::string& facet)
        {
            _subscribers.emplace(element, facet);
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

        const std::map<DataElementI*, std::string>&
        getSubscribers() const
        {
            return _subscribers;
        }

    private:

        const std::shared_ptr<K> _key;
        std::map<DataElementI*, std::string> _subscribers;
    };

    class TopicSubscribers
    {
    public:

        TopicSubscribers() : _lastId(-1)
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

        long long int
        getLastId() const
        {
            return _lastId;
        }

        bool
        setLastId(long long int lastId)
        {
            if(_lastId >= lastId)
            {
                return false;
            }
            _lastId = lastId;
            return true;
        }

    private:

        long long int _lastId;
        std::map<long long int, Subscribers<Key>> _keys;
        std::map<long long int, Subscribers<Filter>> _filters;
    };

public:

    SessionI(NodeI*, const std::shared_ptr<DataStormContract::NodePrx>&);
    void init(const std::shared_ptr<DataStormContract::SessionPrx>&);

    virtual void announceTopics(DataStormContract::TopicInfoSeq, const Ice::Current&);
    virtual void attachTopics(DataStormContract::TopicInfoAndContentSeq, const Ice::Current&);
    virtual void detachTopic(long long int, const Ice::Current&);

    virtual void announceKeys(long long int, DataStormContract::KeyInfoSeq, const Ice::Current&);
    virtual void announceFilter(long long int, DataStormContract::FilterInfo, const Ice::Current&);
    virtual void attachKeysAndFilters(long long int,
                                      long long int,
                                      DataStormContract::KeyInfoAndSamplesSeq,
                                      DataStormContract::FilterInfoSeq,
                                      const Ice::Current&);
    virtual void detachKeys(long long int, DataStormContract::LongSeq, const Ice::Current&);
    virtual void detachFilter(long long int, long long int, const Ice::Current&);


    void connected(const std::shared_ptr<DataStormContract::SessionPrx>&,
                   const std::shared_ptr<Ice::Connection>&,
                   const DataStormContract::TopicInfoSeq&);
    void disconnected(std::exception_ptr);
    void destroyImpl();

    void addConnectedCallback(std::function<void(std::shared_ptr<DataStormContract::SessionPrx>)>);

    std::shared_ptr<DataStormContract::SessionPrx> getSession() const;
    std::shared_ptr<DataStormContract::SessionPrx> getSessionNoLock() const;

    virtual long long int getLastId(long long int) const;

    std::shared_ptr<DataStormContract::SessionPrx> getProxy() const
    {
        return _proxy;
    }

    std::shared_ptr<DataStormContract::NodePrx> getNode() const
    {
        return _node;
    }

    std::unique_lock<std::mutex>& getTopicLock()
    {
        return *_topicLock;
    }

    void subscribe(long long int, TopicI*);
    void unsubscribe(long long int, TopicI*, bool);
    void disconnect(long long int, TopicI*);

    void subscribeToKey(long long int, long long int, const std::shared_ptr<Key>&, DataElementI*);
    void unsubscribeFromKey(long long int, long long int, DataElementI*);
    void disconnectFromKey(long long int, long long int, DataElementI*);

    void subscribeToFilter(long long int, long long int, const std::shared_ptr<Filter>&, DataElementI*);
    void unsubscribeFromFilter(long long int, long long int, DataElementI*);
    void disconnectFromFilter(long long int, long long int, DataElementI*);

protected:

    void runWithTopics(const std::string&, std::function<void (const std::shared_ptr<TopicI>&)>);
    void runWithTopics(long long int, std::function<void (TopicI*, TopicSubscribers&)>);
    void runWithTopic(long long int, TopicI*, std::function<void (TopicSubscribers&)>);

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const = 0;
    virtual bool reconnect() const = 0;

    const std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    mutable std::mutex _mutex;
    NodeI* _parent;
    std::string _id;
    std::shared_ptr<DataStormContract::SessionPrx> _proxy;
    const std::shared_ptr<DataStormContract::NodePrx> _node;
    bool _destroyed;

    std::map<long long int, std::map<TopicI*, TopicSubscribers>> _topics;
    std::unique_lock<std::mutex>* _topicLock;

    std::shared_ptr<DataStormContract::SessionPrx> _session;
    std::shared_ptr<Ice::Connection> _connection;
    std::vector<std::function<void(std::shared_ptr<DataStormContract::SessionPrx>)>> _connectedCallbacks;
};

class SubscriberSessionI : public SessionI, public DataStormContract::SubscriberSession
{
public:

    SubscriberSessionI(NodeI*, const std::shared_ptr<DataStormContract::NodePrx>&);
    virtual void destroy(const Ice::Current&) override;

    virtual void i(long long int, DataStormContract::DataSamplesSeq, const Ice::Current&) override;
    virtual void s(long long int, long long int, DataStormContract::DataSample, const Ice::Current&) override;
    virtual void f(long long int, long long int, DataStormContract::DataSample, const Ice::Current&) override;

private:

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const override;
    virtual bool reconnect() const override;
};

class PublisherSessionI : public SessionI, public DataStormContract::PublisherSession
{
public:

    PublisherSessionI(NodeI*, const std::shared_ptr<DataStormContract::NodePrx>&);
    virtual void destroy(const Ice::Current&) override;

private:

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const override;
    virtual bool reconnect() const override;
};

}