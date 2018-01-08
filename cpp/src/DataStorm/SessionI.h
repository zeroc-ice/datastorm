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
        Subscriber(const std::string& facet, bool visited) :
            facet(facet), initialized(false), lastId(0), visited(visited)
        {
        }

        const std::string facet;
        bool initialized;
        long long int lastId;
        bool visited;
    };

    template<typename K> class Subscribers
    {
    public:

        Subscribers(const std::shared_ptr<K>& key) : _key(key), _visited(false)
        {
        }

        void add(DataElementI* element, const std::string& facet, bool trackVisits)
        {
            _visited = trackVisits;
            auto p = _subscribers.find(element);
            if(p != _subscribers.end())
            {
                p->second.visited = trackVisits;
            }
            else
            {
                _subscribers.emplace(element, Subscriber { facet, trackVisits });
            }
        }

        void remove(DataElementI* element)
        {
            _subscribers.erase(element);
        }

        const std::shared_ptr<K>&
        get() const
        {
            return _key;
        }

        std::map<DataElementI*, Subscriber>&
        getSubscribers()
        {
            return _subscribers;
        }

        bool
        reap()
        {
            if(!_visited)
            {
                return true;
            }
            _visited = false;
            auto p = _subscribers.begin();
            while(p != _subscribers.end())
            {
                if(!p->second.visited)
                {
                    _subscribers.erase(p++);
                }
                else
                {
                    p->second.visited = false;
                    ++p;
                }
            }
            return _subscribers.empty();
        }

    private:

        const std::shared_ptr<K> _key;
        std::map<DataElementI*, Subscriber> _subscribers;
        bool _visited;
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

        DataStormContract::LongLongDict
        getLastIds(const std::shared_ptr<K>& k, DataElementI* element)
        {
            DataStormContract::LongLongDict lastIds;
            for(auto& e : _elements)
            {
                if(e.second.get() == k)
                {
                    auto p = e.second.getSubscribers().find(element);
                    if(p != e.second.getSubscribers().end())
                    {
                        lastIds.emplace(e.first, p->second.lastId);
                    }
                }
            }
            return lastIds;
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

        void
        reap()
        {
            auto p = _elements.begin();
            while(p != _elements.end())
            {
                if(p->second.reap())
                {
                    _elements.erase(p++);
                }
                else
                {
                    ++p;
                }
            }
        }

    private:

        std::map<long long int, Subscribers<K>> _elements;
    };

    struct TopicSubscriber
    {
        TopicSubscriber(bool visited) : visited(visited)
        {
        }

        SubscribersFactory<Key> keys;
        SubscribersFactory<Filter> filters;
        std::map<long long int, std::shared_ptr<Tag>> tags;
        bool visited;
    };

    class TopicSubscribers
    {
    public:

        TopicSubscribers()
        {
        }

        void
        addSubscriber(TopicI* topic, bool trackVisits)
        {
            _visited = trackVisits;
            auto p = _subscribers.find(topic);
            if(p != _subscribers.end())
            {
                p->second.visited = trackVisits;
            }
            else
            {
                _subscribers.emplace(topic, TopicSubscriber(trackVisits));
            }
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

        bool
        reap()
        {
            if(!_visited)
            {
                return true;
            }
            _visited = false;
            auto p = _subscribers.begin();
            while(p != _subscribers.end())
            {
                if(!p->second.visited)
                {
                    _subscribers.erase(p++);
                }
                else
                {
                    p->second.visited = false;
                    ++p;
                }
            }
            return _subscribers.empty();
        }

    private:

        std::map<TopicI*, TopicSubscriber> _subscribers;
        bool _visited;
    };

public:

    SessionI(const std::shared_ptr<NodeI>&, const std::shared_ptr<DataStormContract::NodePrx>&);
    void init(const std::shared_ptr<DataStormContract::SessionPrx>&);

    virtual void announceTopics(DataStormContract::TopicInfoSeq, bool, const Ice::Current&) override;
    virtual void attachTopic(DataStormContract::TopicSpec, const Ice::Current&) override;
    virtual void detachTopic(long long int, const Ice::Current&) override;

    virtual void attachTags(long long int, DataStormContract::ElementInfoSeq, bool, const Ice::Current&) override;
    virtual void detachTags(long long int, DataStormContract::LongSeq, const Ice::Current&) override;

    virtual void announceElements(long long int, DataStormContract::ElementInfoSeq, const Ice::Current&) override;
    virtual void attachElements(long long int, DataStormContract::ElementSpecSeq, bool, const Ice::Current&) override;
    virtual void attachElementsAck(long long int, DataStormContract::ElementSpecAckSeq, const Ice::Current&) override;
    virtual void detachElements(long long int, DataStormContract::LongSeq, const Ice::Current&) override;

    virtual void initSamples(long long int, DataStormContract::DataSamplesSeq, const Ice::Current&) override;

    void connected(const std::shared_ptr<DataStormContract::SessionPrx>&,
                   const std::shared_ptr<Ice::Connection>&,
                   const DataStormContract::TopicInfoSeq&);
    void disconnected(const std::shared_ptr<Ice::Connection>&, std::exception_ptr);
    void destroyImpl(const std::exception_ptr&);

    const std::string& getId() const
    {
        return _id;
    }

    std::shared_ptr<Ice::Connection> getConnection() const
    {
        return _connection;
    }

    void addConnectedCallback(std::function<void(std::shared_ptr<DataStormContract::SessionPrx>)>,
                              const std::shared_ptr<Ice::Connection>&);

    std::shared_ptr<DataStormContract::SessionPrx> getSession() const;

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

    DataStormContract::LongLongDict getLastIds(long long int, const std::shared_ptr<Key>&, DataElementI*);
    std::vector<std::shared_ptr<Sample>> subscriberInitialized(long long int,
                                                               long long int,
                                                               const DataStormContract::DataSampleSeq&,
                                                               const std::shared_ptr<Key>&,
                                                               DataElementI*);

protected:

    void runWithTopics(const std::string&, std::vector<std::shared_ptr<TopicI>>&,
                       std::function<void (const std::shared_ptr<TopicI>&)>);
    void runWithTopics(long long int, std::function<void (TopicI*, TopicSubscriber&)>);
    void runWithTopics(long long int, std::function<void (TopicI*, TopicSubscriber&, TopicSubscribers&)>);
    void runWithTopic(long long int, TopicI*, std::function<void (TopicSubscriber&)>);

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const = 0;
    virtual bool reconnect() const = 0;

    const std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    mutable std::mutex _mutex;
    std::shared_ptr<NodeI> _parent;
    std::string _id;
    std::shared_ptr<DataStormContract::SessionPrx> _proxy;
    const std::shared_ptr<DataStormContract::NodePrx> _node;
    bool _destroyed;
    bool _trackVisits;

    std::map<long long int, TopicSubscribers> _topics;
    std::unique_lock<std::mutex>* _topicLock;

    std::shared_ptr<DataStormContract::SessionPrx> _session;
    std::shared_ptr<Ice::Connection> _connection;
    std::vector<std::function<void(std::shared_ptr<DataStormContract::SessionPrx>)>> _connectedCallbacks;
};

class SubscriberSessionI : public SessionI, public DataStormContract::SubscriberSession
{
public:

    SubscriberSessionI(const std::shared_ptr<NodeI>&, const std::shared_ptr<DataStormContract::NodePrx>&);
    virtual void destroy(const Ice::Current&) override;

    virtual void s(long long int, long long int, DataStormContract::DataSample, const Ice::Current&) override;

private:

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const override;
    virtual bool reconnect() const override;
};

class PublisherSessionI : public SessionI, public DataStormContract::PublisherSession
{
public:

    PublisherSessionI(const std::shared_ptr<NodeI>&, const std::shared_ptr<DataStormContract::NodePrx>&);
    virtual void destroy(const Ice::Current&) override;

private:

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const override;
    virtual bool reconnect() const override;
};

}
