// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/NodeI.h>
#include <DataStorm/Contract.h>

#include <Ice/Ice.h>

namespace DataStormI
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

    struct ElementSubscriber
    {
        ElementSubscriber(const std::string& facet, const std::shared_ptr<Key>& key, int sessionInstanceId) :
            facet(facet), initialized(false), lastId(0), sessionInstanceId(sessionInstanceId)
        {
            keys.insert(key);
        }

        const std::string facet;
        bool initialized;
        long long int lastId;
        std::set<std::shared_ptr<Key>> keys;
        int sessionInstanceId;
    };

    class ElementSubscribers
    {
    public:

        ElementSubscribers(const std::string& name, int priority) :
            name(name), priority(priority), _sessionInstanceId(0)
        {
        }

        void addSubscriber(const std::shared_ptr<DataElementI>& element,
                           const std::shared_ptr<Key>& key,
                           const std::string& facet,
                           int sessionInstanceId)
        {
            _sessionInstanceId = sessionInstanceId;
            auto p = _subscribers.find(element);
            if(p != _subscribers.end())
            {
                p->second.keys.insert(key);
                p->second.sessionInstanceId = sessionInstanceId;
                p->second.initialized = false;
            }
            else
            {
                _subscribers.emplace(element, ElementSubscriber(facet, key, sessionInstanceId));
            }
        }

        void removeSubscriber(const std::shared_ptr<DataElementI>& element)
        {
            _subscribers.erase(element);
        }

        std::map<std::shared_ptr<DataElementI>, ElementSubscriber>&
        getSubscribers()
        {
            return _subscribers;
        }

        ElementSubscriber*
        getSubscriber(const std::shared_ptr<DataElementI>& element)
        {
            auto p = _subscribers.find(element);
            if(p != _subscribers.end())
            {
                return &p->second;
            }
            return 0;
        }

        bool
        reap(int sessionInstanceId)
        {
            if(_sessionInstanceId != sessionInstanceId)
            {
                return true;
            }

            auto p = _subscribers.begin();
            while(p != _subscribers.end())
            {
                if(p->second.sessionInstanceId != sessionInstanceId)
                {
                    _subscribers.erase(p++);
                }
                else
                {
                    ++p;
                }
            }
            return _subscribers.empty();
        }

        std::string name;
        int priority;

    private:

        std::map<std::shared_ptr<DataElementI>, ElementSubscriber> _subscribers;
        int _sessionInstanceId;
    };

    class TopicSubscriber
    {
    public:

        TopicSubscriber(int sessionInstanceId) : sessionInstanceId(sessionInstanceId)
        {
        }

        ElementSubscribers* add(long long int id, const std::string& name, int priority)
        {
            auto p = _elements.find(id);
            if(p == _elements.end())
            {
                p = _elements.emplace(id, ElementSubscribers(name, priority)).first;
            }
            return &p->second;
        }

        ElementSubscribers* get(long long int id)
        {
            auto p = _elements.find(id);
            if(p == _elements.end())
            {
                return 0;
            }
            return &p->second;
        }

        ElementSubscribers remove(long long id)
        {
            auto p = _elements.find(id);
            if(p != _elements.end())
            {
                ElementSubscribers tmp(std::move(p->second));
                _elements.erase(p);
                return tmp;
            }
            return ElementSubscribers("", 0);
        }

        std::map<long long int, ElementSubscribers>&
        getAll()
        {
            return _elements;
        }

        void
        reap(int sessionInstanceId)
        {
            auto p = _elements.begin();
            while(p != _elements.end())
            {
                if(p->second.reap(sessionInstanceId))
                {
                    _elements.erase(p++);
                }
                else
                {
                    ++p;
                }
            }
        }

        std::map<long long int, std::pair<std::shared_ptr<Key>, std::map<long long int, int>>> keys;
        std::map<long long int, std::shared_ptr<Tag>> tags;
        int sessionInstanceId;

    private:

        std::map<long long int, ElementSubscribers> _elements;
    };

    class TopicSubscribers
    {
    public:

        TopicSubscribers()
        {
        }

        void
        addSubscriber(TopicI* topic, int sessionInstanceId)
        {
            _sessionInstanceId = sessionInstanceId;
            auto p = _subscribers.find(topic);
            if(p != _subscribers.end())
            {
                p->second.sessionInstanceId = sessionInstanceId;
            }
            else
            {
                _subscribers.emplace(topic, TopicSubscriber(sessionInstanceId));
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
        reap(int sessionInstanceId)
        {
            if(sessionInstanceId != _sessionInstanceId)
            {
                return true;
            }

            auto p = _subscribers.begin();
            while(p != _subscribers.end())
            {
                if(p->second.sessionInstanceId != sessionInstanceId)
                {
                    _subscribers.erase(p++);
                }
                else
                {
                    ++p;
                }
            }
            return _subscribers.empty();
        }

    private:

        std::map<TopicI*, TopicSubscriber> _subscribers;
        int _sessionInstanceId;
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

    virtual void disconnected(const Ice::Current&) override;

    void connected(const std::shared_ptr<DataStormContract::SessionPrx>&,
                   const std::shared_ptr<Ice::Connection>&,
                   const DataStormContract::TopicInfoSeq&);
    bool disconnected(const std::shared_ptr<Ice::Connection>&, std::exception_ptr);
    void reconnectOrRemove();
    bool retry(const std::shared_ptr<DataStormContract::NodePrx>&, std::exception_ptr);
    void destroyImpl(const std::exception_ptr&);

    const std::string& getId() const
    {
        return _id;
    }

    std::shared_ptr<Ice::Connection> getConnection() const;
    std::shared_ptr<DataStormContract::SessionPrx> getSession() const;
    bool checkSession();

    template<typename T = DataStormContract::SessionPrx> std::shared_ptr<T> getProxy() const
    {
        return Ice::uncheckedCast<T>(_proxy);
    }

    std::shared_ptr<DataStormContract::NodePrx> getNode() const;
    void setNode(std::shared_ptr<DataStormContract::NodePrx>);

    std::unique_lock<std::mutex>& getTopicLock()
    {
        return *_topicLock;
    }

    void subscribe(long long int, TopicI*);
    void unsubscribe(long long int, TopicI*);
    void disconnect(long long int, TopicI*);

    void subscribeToKey(long long int, long long int, const std::shared_ptr<DataElementI>&, const std::string&,
                        const std::shared_ptr<Key>&, long long int, const std::string&, int);
    void unsubscribeFromKey(long long int, long long int, const std::shared_ptr<DataElementI>&, long long int);
    void disconnectFromKey(long long int, long long int, const std::shared_ptr<DataElementI>&, long long int);

    void subscribeToFilter(long long int, long long int, const std::shared_ptr<DataElementI>&, const std::string&,
                           const std::shared_ptr<Key>&, const std::string&, int);
    void unsubscribeFromFilter(long long int, long long int, const std::shared_ptr<DataElementI>&, long long int);
    void disconnectFromFilter(long long int, long long int, const std::shared_ptr<DataElementI>&, long long int);

    DataStormContract::LongLongDict getLastIds(long long int, long long int, const std::shared_ptr<DataElementI>&);
    std::vector<std::shared_ptr<Sample>> subscriberInitialized(long long int,
                                                               long long int,
                                                               const DataStormContract::DataSampleSeq&,
                                                               const std::shared_ptr<Key>&,
                                                               const std::shared_ptr<DataElementI>&);

protected:

    void runWithTopics(const std::string&, std::vector<std::shared_ptr<TopicI>>&,
                       std::function<void (const std::shared_ptr<TopicI>&)>);
    void runWithTopics(long long int, std::function<void (TopicI*, TopicSubscriber&)>);
    void runWithTopics(long long int, std::function<void (TopicI*, TopicSubscriber&, TopicSubscribers&)>);
    void runWithTopic(long long int, TopicI*, std::function<void (TopicSubscriber&)>);

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const = 0;
    virtual bool reconnect(const std::shared_ptr<DataStormContract::NodePrx>&) = 0;
    virtual void remove() = 0;

    const std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    mutable std::mutex _mutex;
    std::shared_ptr<NodeI> _parent;
    std::string _id;
    std::shared_ptr<DataStormContract::SessionPrx> _proxy;
    std::shared_ptr<DataStormContract::NodePrx> _node;
    bool _destroyed;
    int _sessionInstanceId;
    int _retryCount;
    std::function<void()> _retryCanceller;

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

    virtual void s(long long int, long long int, DataStormContract::DataSample, const Ice::Current&) override;

private:

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const override;
    virtual bool reconnect(const std::shared_ptr<DataStormContract::NodePrx>&) override;
    virtual void remove() override;
};

class PublisherSessionI : public SessionI, public DataStormContract::PublisherSession
{
public:

    PublisherSessionI(const std::shared_ptr<NodeI>&, const std::shared_ptr<DataStormContract::NodePrx>&);

private:

    virtual std::vector<std::shared_ptr<TopicI>> getTopics(const std::string&) const override;
    virtual bool reconnect(const std::shared_ptr<DataStormContract::NodePrx>&) override;
    virtual void remove() override;
};

}
