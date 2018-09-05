// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/InternalI.h>
#include <DataStorm/ForwarderManager.h>
#include <DataStorm/Contract.h>

#include <deque>

namespace DataStormI
{

class SessionI;
class TopicI;
class TopicReaderI;
class TopicWriterI;
class CallbackExecutor;

class TraceLevels;

class DataElementI : virtual public DataElement, public Forwarder, public std::enable_shared_from_this<DataElementI>
{
protected:

    struct Subscriber
    {
        Subscriber(long long int id,
                   const std::shared_ptr<Filter>& filter,
                   const std::shared_ptr<Filter>& sampleFilter,
                   int priority) :
            id(id), filter(filter), sampleFilter(sampleFilter), priority(priority)
        {
        }

        long long int topicId;
        long long int elementId;
        long long int id;
        std::set<std::shared_ptr<Key>> keys;
        std::shared_ptr<Filter> filter;
        std::shared_ptr<Filter> sampleFilter;
        int priority;
    };

private:

    struct ListenerKey
    {
        std::shared_ptr<SessionI> session;
        std::string facet;

        bool operator<(const ListenerKey& other) const
        {
            if(session < other.session)
            {
                return true;
            }
            else if(other.session < session)
            {
                return false;
            }
            return facet < other.facet;
        }
    };

    struct Listener
    {
        Listener(const std::shared_ptr<DataStormContract::SessionPrx>& proxy, const std::string& facet) :
            proxy(facet.empty() ? proxy : Ice::uncheckedCast<DataStormContract::SessionPrx>(proxy->ice_facet(facet)))
        {
        }

        bool matchOne(const std::shared_ptr<Sample>& sample, bool matchKey) const
        {
            for(const auto& s : subscribers)
            {
                if((!matchKey || s.second->keys.empty() || s.second->keys.find(sample->key) != s.second->keys.end()) &&
                   (!s.second->filter || s.second->filter->match(sample->key)) &&
                   (!s.second->sampleFilter || s.second->sampleFilter->match(sample)))
                {
                    return true;
                }
            }
            return false;
        }

        std::shared_ptr<Subscriber> addOrGet(long long int topicId,
                                             long long int elementId,
                                             long long int id,
                                             const std::shared_ptr<Filter>& filter,
                                             const std::shared_ptr<Filter>& sampleFilter,
                                             int priority)
        {
            auto k = std::make_pair(topicId, elementId);
            auto p = subscribers.find(k);
            if(p == subscribers.end())
            {
                p = subscribers.emplace(k, std::make_shared<Subscriber>(id, filter, sampleFilter, priority)).first;
            }
            return p->second;
        }

        std::shared_ptr<Subscriber> get(long long int topicId, long long int elementId)
        {
            return subscribers.find(std::make_pair(topicId, elementId))->second;
        }

        bool remove(long long int topicId, long long int elementId)
        {
            subscribers.erase(std::make_pair(topicId, elementId));
            return subscribers.empty();
        }

        std::shared_ptr<DataStormContract::SessionPrx> proxy;
        std::map<std::pair<long long int, long long int>, std::shared_ptr<Subscriber>> subscribers;
    };

public:

    DataElementI(TopicI*, long long int, const DataStorm::Config&);
    virtual ~DataElementI();

    void init();

    virtual void destroy() override;

    void attach(long long int,
                long long int,
                const std::shared_ptr<Key>&,
                const std::shared_ptr<Filter>&,
                const std::shared_ptr<SessionI>&,
                const std::shared_ptr<DataStormContract::SessionPrx>&,
                const DataStormContract::ElementData&,
                const std::chrono::time_point<std::chrono::system_clock>&,
                DataStormContract::ElementDataAckSeq&);

    std::function<void()> attach(long long int,
                                 long long int,
                                 const std::shared_ptr<Key>&,
                                 const std::shared_ptr<Filter>&,
                                 const std::shared_ptr<SessionI>&,
                                 const std::shared_ptr<DataStormContract::SessionPrx>&,
                                 const DataStormContract::ElementDataAck&,
                                 const std::chrono::time_point<std::chrono::system_clock>&,
                                 DataStormContract::DataSamplesSeq&);

    bool attachKey(long long int,
                   long long int,
                   const std::shared_ptr<Key>&,
                   const std::shared_ptr<Filter>&,
                   const std::shared_ptr<SessionI>&,
                   const std::shared_ptr<DataStormContract::SessionPrx>&,
                   const std::string&,
                   long long int,
                   int);

    void detachKey(long long int,
                   long long int,
                   const std::shared_ptr<Key>&,
                   const std::shared_ptr<SessionI>&,
                   const std::string&,
                   bool);

    bool attachFilter(long long int,
                      long long int,
                      const std::shared_ptr<Key>&,
                      const std::shared_ptr<Filter>&,
                      const std::shared_ptr<SessionI>&,
                      const std::shared_ptr<DataStormContract::SessionPrx>&,
                      const std::string&,
                      long long int,
                      const std::shared_ptr<Filter>&,
                      int);

    void detachFilter(long long int,
                      long long int,
                      const std::shared_ptr<Key>&,
                      const std::shared_ptr<SessionI>&,
                      const std::string&,
                      bool);

    virtual void onKeyConnect(std::function<void(Id, std::shared_ptr<Key>)>) override;
    virtual void onKeyDisconnect(std::function<void(Id, std::shared_ptr<Key>)>) override;
    virtual std::vector<std::shared_ptr<Key>> getConnectedKeys() const override;

    virtual void onFilterConnect(std::function<void(Id, std::shared_ptr<Filter>)>) override;
    virtual void onFilterDisconnect(std::function<void(Id, std::shared_ptr<Filter>)>) override;

    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&, long long int, long long int, int,
                             const std::chrono::time_point<std::chrono::system_clock>&, bool);
    virtual DataStormContract::DataSamples getSamples(const std::shared_ptr<Key>&,
                                                      const std::shared_ptr<Filter>&,
                                                      const std::shared_ptr<DataStormContract::ElementConfig>&,
                                                      long long int,
                                                      const std::chrono::time_point<std::chrono::system_clock>&);

    virtual void queue(const std::shared_ptr<Sample>&, int, const std::shared_ptr<SessionI>&, const std::string&,
                       const std::chrono::time_point<std::chrono::system_clock>&, bool);

    virtual std::string toString() const = 0;
    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

    long long int getId() const
    {
        return _id;
    }

    std::shared_ptr<DataStormContract::ElementConfig> getConfig() const;

    void waitForListeners(int count) const;
    bool hasListeners() const;

    TopicI* getTopic() const
    {
        return _parent.get();
    }

protected:

    virtual bool addConnectedKey(const std::shared_ptr<Key>&, const std::shared_ptr<Subscriber>&);
    virtual bool removeConnectedKey(const std::shared_ptr<Key>&, const std::shared_ptr<Subscriber>&);

    void notifyListenerWaiters(std::unique_lock<std::mutex>&) const;
    void disconnect();
    virtual void destroyImpl() = 0;

    std::shared_ptr<TraceLevels> _traceLevels;
    std::shared_ptr<DataStormContract::SessionPrx> _forwarder;
    mutable std::shared_ptr<Sample> _sample;
    long long int _id;
    size_t _listenerCount;
    std::shared_ptr<DataStormContract::ElementConfig> _config;
    std::shared_ptr<CallbackExecutor> _executor;
    std::map<std::shared_ptr<Key>, std::vector<std::shared_ptr<Subscriber>>> _connectedKeys;
    std::map<ListenerKey, Listener> _listeners;

private:

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const override;

    std::shared_ptr<TopicI> _parent;
    mutable size_t _waiters;
    mutable size_t _notified;

    std::function<void(Id, std::shared_ptr<Key>)> _onKeyConnect;
    std::function<void(Id, std::shared_ptr<Key>)> _onKeyDisconnect;
    std::function<void(Id, std::shared_ptr<Filter>)> _onFilterConnect;
    std::function<void(Id, std::shared_ptr<Filter>)> _onFilterDisconnect;
};

class DataReaderI : public DataElementI, public DataReader
{
public:

    DataReaderI(TopicReaderI*, long long int, const std::string&, std::vector<unsigned char>,
                const DataStorm::ReaderConfig&);

    virtual int getInstanceCount() const override;

    virtual std::vector<std::shared_ptr<Sample>> getAllUnread() override;
    virtual void waitForUnread(unsigned int) const override;
    virtual bool hasUnread() const override;
    virtual std::shared_ptr<Sample> getNextUnread() override;

    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&, long long int, long long int, int,
                             const std::chrono::time_point<std::chrono::system_clock>&, bool) override;
    virtual void queue(const std::shared_ptr<Sample>&, int, const std::shared_ptr<SessionI>&, const std::string&,
                       const std::chrono::time_point<std::chrono::system_clock>&, bool) override;

    virtual void onSamples(std::function<void(const std::vector<std::shared_ptr<Sample>>&)>) override;

protected:

    virtual bool matchKey(const std::shared_ptr<Key>&) const = 0;
    virtual bool addConnectedKey(const std::shared_ptr<Key>&, const std::shared_ptr<Subscriber>&) override;

    TopicReaderI* _parent;

    std::deque<std::shared_ptr<Sample>> _samples;
    std::shared_ptr<Sample> _last;
    int _instanceCount;
    DataStorm::DiscardPolicy _discardPolicy;
    std::chrono::time_point<std::chrono::system_clock> _lastSendTime;
    std::function<void(const std::vector<std::shared_ptr<Sample>>&)> _onSamples;
};

class DataWriterI : public DataElementI, public DataWriter
{
public:

    DataWriterI(TopicWriterI*, long long int, const DataStorm::WriterConfig&);
    void init();

    virtual void publish(const std::shared_ptr<Key>&, const std::shared_ptr<Sample>&) override;

protected:

    virtual void send(const std::shared_ptr<Key>&, const std::shared_ptr<Sample>&) const = 0;

    TopicWriterI* _parent;
    std::shared_ptr<DataStormContract::SubscriberSessionPrx> _subscribers;
    std::deque<std::shared_ptr<Sample>> _samples;
    std::shared_ptr<Sample> _last;
};

class KeyDataReaderI : public DataReaderI
{
public:

    KeyDataReaderI(TopicReaderI*, long long int, const std::vector<std::shared_ptr<Key>>&, const std::string&,
                   std::vector<unsigned char>, const DataStorm::ReaderConfig&);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual std::string toString() const override;

private:

    virtual bool matchKey(const std::shared_ptr<Key>&) const override;

    const std::vector<std::shared_ptr<Key>> _keys;
};

class KeyDataWriterI : public DataWriterI
{
public:

    KeyDataWriterI(TopicWriterI*, long long int, const std::vector<std::shared_ptr<Key>>&,
                   const DataStorm::WriterConfig&);

    virtual void destroyImpl() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

    virtual std::shared_ptr<Sample> getLast() const override;
    virtual std::vector<std::shared_ptr<Sample>> getAll() const override;

    virtual std::string toString() const override;
    virtual DataStormContract::DataSamples getSamples(const std::shared_ptr<Key>&,
                                                      const std::shared_ptr<Filter>&,
                                                      const std::shared_ptr<DataStormContract::ElementConfig>&,
                                                      long long int,
                                                      const std::chrono::time_point<std::chrono::system_clock>&) override;

private:

    virtual void send(const std::shared_ptr<Key>&, const std::shared_ptr<Sample>&) const override;
    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const override;

    const std::vector<std::shared_ptr<Key>> _keys;
};

class FilteredDataReaderI : public DataReaderI
{
public:

    FilteredDataReaderI(TopicReaderI*, long long int, const std::shared_ptr<Filter>&, const std::string&,
                        std::vector<unsigned char>, const DataStorm::ReaderConfig&);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual std::string toString() const override;

private:

    virtual bool matchKey(const std::shared_ptr<Key>&) const override;

    const std::shared_ptr<Filter> _filter;
};

}
