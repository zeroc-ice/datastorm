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
protected:

    struct Subscriber
    {
        Subscriber(const std::string& facet) : facet(facet), initialized(false)
        {
        }

        const std::string facet;
        bool initialized;
    };

    template<typename K> class Subscribers
    {
    public:

        Subscribers(const std::shared_ptr<K>& key) : _key(key)
        {
        }

        void add(DataElementI* element, const std::string& facet)
        {
            _subscribers.emplace(element, Subscriber { facet });
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

        std::map<DataElementI*, Subscriber>&
        getSubscribers()
        {
            return _subscribers;
        }

    private:

        const std::shared_ptr<K> _key;
        std::map<DataElementI*, Subscriber> _subscribers;
    };

    template<typename K> class SubscribersFactory
    {
    public:

        Subscribers<K>* get(long long int id, const std::shared_ptr<K>& k = nullptr)
        {
            auto p = _elements.find(id);
            if(p != _elements.end())
            {
                //assert(!k || k == p->second.get());
                return &p->second;
            }
            else if(k)
            {
                return &_elements.emplace(id, Subscribers<K>(k)).first->second;
            }
            else
            {
                return 0;
            }
        }

        Subscribers<K> remove(long long id)
        {
            auto p = _elements.find(id);
            if(p != _elements.end())
            {
                Subscribers<K> tmp(std::move(p->second));
                _elements.erase(p);
                return tmp;
            }
            return Subscribers<K>(nullptr);
        }

        std::map<long long int, Subscribers<K>>&
        getAll()
        {
            return _elements;
        }

    private:

        std::map<long long int, Subscribers<K>> _elements;
    };

    struct TopicSubscriber
    {
        SubscribersFactory<Key> keys;
        SubscribersFactory<Filter> filters;
    };

    class TopicSubscribers
    {
    public:

        TopicSubscribers() : _lastId(-1)
        {
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

        void
        addSubscriber(TopicI* topic)
        {
            _subscribers[topic] = TopicSubscriber();
        }

        TopicSubscriber&
        getSubscriber(TopicI* topic)
        {
            assert(_subscribers.find(topic) != _subscribers.end());
            return _subscribers.at(topic);
        }

        void
        removeSubscriber(TopicI* topic)
        {
            _subscribers.erase(topic);
        }

        std::map<TopicI*, TopicSubscriber>&
        getSubscribers()
        {
            return _subscribers;
        }

        void
        clearSubscribers()
        {
            _subscribers.clear();
        }

    private:

        long long int _lastId;
        std::map<TopicI*, TopicSubscriber> _subscribers;
    };

public:

    SessionI(NodeI*, const std::shared_ptr<DataStormContract::NodePrx>&);
    void init(const std::shared_ptr<DataStormContract::SessionPrx>&);

    virtual void announceTopics(DataStormContract::TopicInfoSeq, const Ice::Current&) override;
    virtual void attachTopic(DataStormContract::TopicSpec, const Ice::Current&) override;
    virtual void detachTopic(long long int, const Ice::Current&) override;

    virtual void announceElements(long long int, DataStormContract::ElementInfoSeq, const Ice::Current&) override;
    virtual void attachElements(long long int, long long int, DataStormContract::ElementSpecSeq, const Ice::Current&) override;
    virtual void attachElementsAck(long long int, long long int, DataStormContract::ElementSpecAckSeq, const Ice::Current&) override;
    virtual void detachElements(long long int, DataStormContract::LongSeq, const Ice::Current&) override;

    virtual void initSamples(long long int, DataStormContract::DataSamplesSeq, const Ice::Current&) override;

    void connected(const std::shared_ptr<DataStormContract::SessionPrx>&,
                   const std::shared_ptr<Ice::Connection>&,
                   const DataStormContract::TopicInfoSeq&);
    void disconnected(std::exception_ptr);
    void destroyImpl();

    const std::string& getId() const
    {
        return _id;
    }

    std::shared_ptr<Ice::Connection> getConnection() const
    {
        return _connection;
    }

    void addConnectedCallback(std::function<void(std::shared_ptr<DataStormContract::SessionPrx>)>);

    std::shared_ptr<DataStormContract::SessionPrx> getSession() const;
    std::shared_ptr<DataStormContract::SessionPrx> getSessionNoLock() const;

    long long int getLastId(long long int) const;

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
    void unsubscribe(long long int, TopicI*);
    void disconnect(long long int, TopicI*);

    void subscribeToKey(long long int, long long int, const std::shared_ptr<Key>&, DataElementI*, const std::string&);
    void unsubscribeFromKey(long long int, long long int, DataElementI*);
    void disconnectFromKey(long long int, long long int, DataElementI*);

    void subscribeToFilter(long long int, long long int, const std::shared_ptr<Filter>&, DataElementI*, const std::string&);
    void unsubscribeFromFilter(long long int, long long int, DataElementI*);
    void disconnectFromFilter(long long int, long long int, DataElementI*);

    void subscriberInitialized(long long int, long long int, DataElementI*);

protected:

    void runWithTopics(const std::string&, std::function<void (const std::shared_ptr<TopicI>&)>);
    void runWithTopics(long long int, std::function<void (TopicI*, TopicSubscriber&)>);
    void runWithTopics(long long int, std::function<void (TopicI*, TopicSubscriber&, TopicSubscribers&)>);
    void runWithTopic(long long int, TopicI*, std::function<void (TopicSubscriber&)>);

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

    std::map<long long int, TopicSubscribers> _topics;
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

    virtual void s(long long int, long long int, DataStormContract::DataSample, const Ice::Current&) override;

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