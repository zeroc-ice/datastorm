// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/InternalI.h>
#include <DataStorm/ForwarderManager.h>
#include <DataStorm/Contract.h>

#include <deque>

namespace DataStormInternal
{

class SessionI;
class TopicI;
class TopicReaderI;
class TopicWriterI;

class TraceLevels;

class DataElementI : virtual public DataElement, private Forwarder
{
    struct ListenerKey
    {
        SessionI* session;
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

    template<typename T> struct Subscribers
    {
        bool add(long long int topic, long long int id, const std::shared_ptr<T>& subscriber)
        {
            return subscribers.emplace(std::make_pair(topic, id), subscriber).second;
        }

        bool remove(long long int topic, long long int id)
        {
            return subscribers.erase({ topic, id });
        }

        std::map<std::pair<long long int, long long int>, std::shared_ptr<T>> subscribers;
    };

    struct Listener
    {
        Listener(const std::shared_ptr<DataStormContract::SessionPrx>& proxy) : proxy(proxy)
        {
        }

        bool matchOne(const std::shared_ptr<Sample>& sample) const
        {
            if(!keys.subscribers.empty())
            {
                return true;
            }
            for(const auto& s : filters.subscribers)
            {
                if(s.second->writerMatch(sample))
                {
                    return true;
                }
            }
            return false;
        }

        bool empty() const
        {
            return keys.subscribers.empty() && filters.subscribers.empty();
        }

        std::shared_ptr<DataStormContract::SessionPrx> proxy;
        Subscribers<Key> keys;
        Subscribers<Filter> filters;
    };

public:

    DataElementI(TopicI*);
    virtual ~DataElementI();

    virtual void destroy() override;

    virtual bool attachKey(long long int, long long int, const std::shared_ptr<Key>&, SessionI*,
                           const std::shared_ptr<DataStormContract::SessionPrx>&);
    virtual void detachKey(long long int, long long int, SessionI*, const std::string&, bool = true);

    virtual bool attachFilter(long long int, long long int, const std::shared_ptr<Filter>&, SessionI*,
                              const std::shared_ptr<DataStormContract::SessionPrx>&);
    virtual void detachFilter(long long int, long long int, SessionI*, const std::string&, bool = true);

    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&);

    virtual void queue(const std::shared_ptr<Sample>&, const std::string& = std::string());

    void waitForListeners(int count) const;
    bool hasListeners() const;

    virtual long long int getSubscriberId() const;
    virtual std::string toString() const = 0;
    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

    const std::string& getFacet() const
    {
        return _facet;
    }

    TopicI* getTopic() const
    {
        return _parent;
    }

protected:

    void notifyListenerWaiters(std::unique_lock<std::mutex>&) const;
    void disconnect();
    virtual void destroyImpl() = 0;

    std::shared_ptr<TraceLevels> _traceLevels;
    const std::shared_ptr<DataStormContract::SessionPrx> _forwarder;
    mutable std::shared_ptr<Sample> _sample;
    std::string _facet;

private:

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const override;

    TopicI* _parent;
    std::map<ListenerKey, Listener> _listeners;
    size_t _listenerCount;
    mutable size_t _waiters;
    mutable size_t _notified;
};

class KeyDataElementI : virtual public DataElementI, public std::enable_shared_from_this<KeyDataElementI>
{
public:

    virtual DataStormContract::DataSampleSeq getSamples(long long int, const std::shared_ptr<Filter>&) const = 0;
};

class FilteredDataElementI : virtual public DataElementI, public std::enable_shared_from_this<FilteredDataElementI>
{
};

class DataReaderI : virtual public DataElementI, public DataReader
{
public:

    DataReaderI(TopicReaderI*);

    virtual int getInstanceCount() const override;

    virtual std::vector<std::shared_ptr<Sample>> getAll() override;
    virtual std::vector<std::shared_ptr<Sample>> getAllUnread() override;
    virtual void waitForUnread(unsigned int) const override;
    virtual bool hasUnread() const override;
    virtual std::shared_ptr<Sample> getNextUnread() override;

    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&) override;
    virtual void queue(const std::shared_ptr<Sample>&, const std::string& = std::string()) override;

protected:

    TopicReaderI* _parent;

    std::deque<std::shared_ptr<Sample>> _all;
    std::deque<std::shared_ptr<Sample>> _unread;
    int _instanceCount;
};

class DataWriterI : virtual public DataElementI, public DataWriter
{
public:

    DataWriterI(TopicWriterI*);

    virtual bool attachFilter(long long int, long long int, const std::shared_ptr<Filter>&, SessionI*,
                              const std::shared_ptr<DataStormContract::SessionPrx>&) override;

    virtual void publish(const std::shared_ptr<Sample>&) override;

protected:


    virtual void send(const std::shared_ptr<Sample>&) const = 0;

    TopicWriterI* _parent;
    std::shared_ptr<DataStormContract::SubscriberSessionPrx> _subscribers;
    std::deque<std::shared_ptr<Sample>> _all;
};

class KeyDataReaderI : public DataReaderI, public KeyDataElementI
{
public:

    KeyDataReaderI(TopicReaderI*, const std::vector<std::shared_ptr<Key>>&);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual std::string toString() const override;
    virtual DataStormContract::DataSampleSeq getSamples(long long int, const std::shared_ptr<Filter>&) const override;

private:

    const std::vector<std::shared_ptr<Key>> _keys;
};

class KeyDataWriterI : public DataWriterI, public KeyDataElementI
{
public:

    KeyDataWriterI(TopicWriterI*, const std::vector<std::shared_ptr<Key>>&);

    virtual void destroyImpl() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

    virtual std::string toString() const override;
    virtual DataStormContract::DataSampleSeq getSamples(long long int, const std::shared_ptr<Filter>&) const override;

private:

    virtual void send(const std::shared_ptr<Sample>&) const override;

    const std::vector<std::shared_ptr<Key>> _keys;
};

class FilteredDataReaderI : public DataReaderI, public FilteredDataElementI
{
public:

    FilteredDataReaderI(TopicReaderI*, const std::shared_ptr<Filter>&);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual long long int getSubscriberId() const override;
    virtual std::string toString() const override;
    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&) override;
    virtual void queue(const std::shared_ptr<Sample>&, const std::string& = std::string()) override;

private:

    const std::shared_ptr<Filter> _filter;
};

class FilteredDataWriterI : public DataWriterI, public FilteredDataElementI
{
public:

    FilteredDataWriterI(TopicWriterI*, const std::shared_ptr<Filter>&);

    virtual void destroyImpl() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

    virtual std::string toString() const override;

private:

    virtual void send(const std::shared_ptr<Sample>&) const override;

    const std::shared_ptr<Filter> _filter;
};

}