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
        Subscriber(const std::string& facet, long long int keyId, int sessionInstanceId) :
            facet(facet), initialized(false), lastId(0), sessionInstanceId(sessionInstanceId)
        {
            keys.insert(keyId);
        }

        const std::string facet;
        bool initialized;
        long long int lastId;
        std::set<long long int> keys;
        int sessionInstanceId;
    };

    class Subscribers
    {
    public:

        Subscribers() : _sessionInstanceId(0), _lastId(0)
        {
        }

        void add(const std::shared_ptr<DataElementI>& element, long long keyId, const std::string& facet, int sessionInstanceId)
        {
            _sessionInstanceId = sessionInstanceId;
            auto p = _subscribers.find(element);
            if(p != _subscribers.end())
            {
                p->second.keys.insert(keyId);
                p->second.sessionInstanceId = sessionInstanceId;
            }
            else
            {
                _subscribers.emplace(element, Subscriber { facet, keyId, sessionInstanceId });
            }
        }

        std::set<long long int> remove(const std::shared_ptr<DataElementI>& element)
        {
            auto p = _subscribers.find(element);
            if(p == _subscribers.end())
            {
                return {};
            }
            auto keys = move(p->second.keys);
            _subscribers.erase(p);
            return keys;
        }

        std::map<std::shared_ptr<DataElementI>, Subscriber>&
        getSubscribers()
        {
            return _subscribers;
        }

        Subscriber*
        get(const std::shared_ptr<DataElementI>& element)
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

    private:

        std::map<std::shared_ptr<DataElementI>, Subscriber> _subscribers;
        int _sessionInstanceId;
        long long int _lastId;
    };

    class SubscribersManager
    {
    public:

        Subscribers* get(long long int id, bool create = false)
        {
            auto p = _elements.find(id);
            if(p == _elements.end())
            {
                if(!create)
                {
                    return 0;
                }
                p = _elements.emplace(id, Subscribers()).first;
            }
            return &p->second;
        }

        Subscribers remove(long long id)
        {
            auto p = _elements.find(id);
            if(p != _elements.end())
            {
                Subscribers tmp(std::move(p->second));
                _elements.erase(p);
                return tmp;
            }
            return Subscribers();
        }

        std::map<long long int, Subscribers>&
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

    private:

        std::map<long long int, Subscribers> _elements;
    };

    template<typename T> class Element
    {
    public:

        Element(const std::shared_ptr<T>& element) : _element(element)
        {
        }

        void add(const std::shared_ptr<DataElementI>& subscriber, long long int elementId)
        {
            auto p = _subscribers.find(subscriber);
            if(p == _subscribers.end())
            {
                p = _subscribers.emplace(subscriber, std::set<long long int> {}).first;
            }
            p->second.insert(elementId);
        }

        bool remove(const std::shared_ptr<DataElementI>& subscriber, long long int elementId)
        {
            auto p = _subscribers.find(subscriber);
            assert(p != _subscribers.end());
            p->second.erase(elementId);
            if(p->second.empty())
            {
                _subscribers.erase(p);
            }
            return _subscribers.empty();
        }

        const std::shared_ptr<T>& get() const
        {
            return _element;
        }

        std::set<long long int> getElementIds(const std::shared_ptr<DataElementI>& element) const
        {
            auto p = _subscribers.find(element);
            if(p != _subscribers.end())
            {
                return p->second;
            }
            return {};
        }

    private:

        const std::shared_ptr<T> _element;
        std::map<std::shared_ptr<DataElementI>, std::set<long long int>> _subscribers;
    };

    template<typename T> class ElementManager
    {
    public:

        void add(long long int id, const std::shared_ptr<T>& element, const std::shared_ptr<DataElementI>& subscriber, long long int elementId)
        {
            auto p = _elements.find(id);
            if(p == _elements.end())
            {
                p = _elements.emplace(id, Element<T>(element)).first;
            }
            p->second.add(subscriber, elementId);
        }

        std::shared_ptr<T> remove(long long int id, const std::shared_ptr<DataElementI>& subscriber, long long int elementId)
        {
            auto p = _elements.find(id);
            assert(p != _elements.end());
            auto k = p->second.get();
            if(p->second.remove(subscriber, elementId))
            {
                _elements.erase(p);
            }
            return k;
        }

        std::shared_ptr<T> get(long long int id) const
        {
            auto p = _elements.find(id);
            if(p != _elements.end())
            {
                return p->second.get();
            }
            return nullptr;
        }

        bool empty()
        {
            return _elements.empty();
        }

        std::set<long long int>
        getSubscriberElementIds(long long int id, const std::shared_ptr<DataElementI>& element) const
        {
            auto p = _elements.find(id);
            if(p != _elements.end())
            {
                return p->second.getElementIds(element);
            }
            return {};
        }

    private:

        std::map<long long int, Element<T>> _elements;
    };

    struct TopicSubscriber
    {
        TopicSubscriber(int sessionInstanceId) : sessionInstanceId(sessionInstanceId)
        {
        }

        SubscribersManager subscribers;
        ElementManager<Key> keys;
        ElementManager<Filter> filters;
        std::map<long long int, std::shared_ptr<Tag>> tags;
        int sessionInstanceId;
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

    void subscribeToKey(long long int, long long int, long long int, const std::shared_ptr<Key>&, const std::shared_ptr<DataElementI>&,
                        const std::string&);
    void unsubscribeFromKey(long long int, long long int, const std::shared_ptr<DataElementI>&);
    void disconnectFromKey(long long int, long long int, const std::shared_ptr<DataElementI>&);

    void subscribeToFilter(long long int, long long int, long long int, const std::shared_ptr<Filter>&, const std::shared_ptr<DataElementI>&,
                           const std::string&);
    void unsubscribeFromFilter(long long int, long long int, const std::shared_ptr<DataElementI>&);
    void disconnectFromFilter(long long int, long long int, const std::shared_ptr<DataElementI>&);

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
    virtual bool reconnect() const = 0;

    const std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    mutable std::mutex _mutex;
    std::shared_ptr<NodeI> _parent;
    std::string _id;
    std::shared_ptr<DataStormContract::SessionPrx> _proxy;
    const std::shared_ptr<DataStormContract::NodePrx> _node;
    bool _destroyed;
    int _sessionInstanceId;

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
