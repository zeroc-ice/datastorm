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
        bool add(long long int topic, long long int element, const std::shared_ptr<Filter>& sampleFilter)
        {
            return subscribers.emplace(std::make_pair(topic, element), sampleFilter).second;
        }

        bool remove(long long int topic, long long int element)
        {
            return subscribers.erase({ topic, element });
        }

        std::map<std::pair<long long int, long long int>, std::shared_ptr<Filter>> subscribers;
    };

    struct Listener
    {
        Listener(const std::shared_ptr<DataStormContract::SessionPrx>& proxy, const std::string& facet) :
            proxy(facet.empty() ? proxy : Ice::uncheckedCast<DataStormContract::SessionPrx>(proxy->ice_facet(facet)))
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
                if(!s.second || s.second->match(sample))
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

    DataElementI(TopicI*, long long int);
    virtual ~DataElementI();

    virtual void destroy() override;

    bool attachKey(long long int, long long int, const std::shared_ptr<Key>&, const std::shared_ptr<Filter>&,
                   SessionI*, const std::shared_ptr<DataStormContract::SessionPrx>&, const std::string&);
    void detachKey(long long int, long long int, SessionI*, const std::string&, bool = true);

    bool attachFilter(long long int, long long int, const std::shared_ptr<Filter>&, const std::shared_ptr<Filter>&,
                      SessionI*, const std::shared_ptr<DataStormContract::SessionPrx>&, const std::string&);
    void detachFilter(long long int, long long int, SessionI*, const std::string&, bool = true);

    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&, SessionI*, long long int, long long int);
    virtual DataStormContract::DataSampleSeq getSamples(long long int, const std::shared_ptr<Filter>&) const;
    virtual std::vector<unsigned char> getSampleFilterCriteria() const;
    virtual void queue(const std::shared_ptr<Sample>&, const std::string& = std::string());
    virtual std::shared_ptr<Filter> createSampleFilter(std::vector<unsigned char>) const;
    virtual std::string toString() const = 0;
    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

    long long int getId() const
    {
        return _id;
    }

    const std::string& getFacet() const
    {
        return _facet;
    }

    void waitForListeners(int count) const;
    bool hasListeners() const;

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
    long long int _id;
    size_t _listenerCount;

private:

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const override;

    TopicI* _parent;
    std::map<ListenerKey, Listener> _listeners;
    mutable size_t _waiters;
    mutable size_t _notified;
};

class KeyDataElementI : virtual public DataElementI, public std::enable_shared_from_this<KeyDataElementI>
{
};

class FilteredDataElementI : virtual public DataElementI, public std::enable_shared_from_this<FilteredDataElementI>
{
};

class DataReaderI : virtual public DataElementI, public DataReader
{
public:

    DataReaderI(TopicReaderI*, std::vector<unsigned char>);

    virtual int getInstanceCount() const override;

    virtual std::vector<std::shared_ptr<Sample>> getAll() override;
    virtual std::vector<std::shared_ptr<Sample>> getAllUnread() override;
    virtual void waitForUnread(unsigned int) const override;
    virtual bool hasUnread() const override;
    virtual std::shared_ptr<Sample> getNextUnread() override;

    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&, SessionI*, long long int, long long int) override;
    virtual std::vector<unsigned char> getSampleFilterCriteria() const override;
    virtual void queue(const std::shared_ptr<Sample>&, const std::string& = std::string()) override;

protected:

    TopicReaderI* _parent;

    std::vector<unsigned char> _sampleFilterCriteria;
    std::deque<std::shared_ptr<Sample>> _all;
    std::deque<std::shared_ptr<Sample>> _unread;
    int _instanceCount;
};

class DataWriterI : virtual public DataElementI, public DataWriter
{
public:

    DataWriterI(TopicWriterI*, const std::shared_ptr<FilterFactory>&);

    virtual void publish(const std::shared_ptr<Sample>&) override;
    virtual std::shared_ptr<Filter> createSampleFilter(std::vector<unsigned char>) const override;

protected:


    virtual void send(const std::shared_ptr<Sample>&) const = 0;

    TopicWriterI* _parent;
    std::shared_ptr<FilterFactory> _sampleFilterFactory;
    std::shared_ptr<DataStormContract::SubscriberSessionPrx> _subscribers;
    std::deque<std::shared_ptr<Sample>> _all;
};

class KeyDataReaderI : public DataReaderI, public KeyDataElementI
{
public:

    KeyDataReaderI(TopicReaderI*, long long int, const std::vector<std::shared_ptr<Key>>&, std::vector<unsigned char>);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual std::string toString() const override;

private:

    const std::vector<std::shared_ptr<Key>> _keys;
};

class KeyDataWriterI : public DataWriterI, public KeyDataElementI
{
public:

    KeyDataWriterI(TopicWriterI*, long long int, const std::shared_ptr<Key>&, const std::shared_ptr<FilterFactory>&);

    virtual void destroyImpl() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

    virtual std::string toString() const override;
    virtual DataStormContract::DataSampleSeq getSamples(long long int, const std::shared_ptr<Filter>&) const override;

private:

    virtual void send(const std::shared_ptr<Sample>&) const override;

    const std::shared_ptr<Key> _key;
};

class FilteredDataReaderI : public DataReaderI, public FilteredDataElementI
{
public:

    FilteredDataReaderI(TopicReaderI*, long long int, const std::shared_ptr<Filter>&, std::vector<unsigned char>);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual std::string toString() const override;

private:

    const std::shared_ptr<Filter> _filter;
};

class FilteredDataWriterI : public DataWriterI, public FilteredDataElementI
{
public:

    FilteredDataWriterI(TopicWriterI*, long long int, const std::shared_ptr<Filter>&, const std::shared_ptr<FilterFactory>&);

    virtual void destroyImpl() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

    virtual std::string toString() const override;

private:

    virtual void send(const std::shared_ptr<Sample>&) const override;

    const std::shared_ptr<Filter> _filter;
};

}