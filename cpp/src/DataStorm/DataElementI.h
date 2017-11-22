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

class DataElementI : virtual public DataElement, private Forwarder, public std::enable_shared_from_this<DataElementI>
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

    struct Subscriber
    {
        std::shared_ptr<Filter> sampleFilter;
    };

    template<typename T> struct Subscribers
    {
        bool add(long long int topic, long long int element, const std::shared_ptr<Filter>& sampleFilter)
        {
            return subscribers.emplace(std::make_pair(topic, element), Subscriber { sampleFilter }).second;
        }

        bool remove(long long int topic, long long int element)
        {
            return subscribers.erase({ topic, element });
        }

        std::map<std::pair<long long int, long long int>, Subscriber> subscribers;
    };

    struct Listener
    {
        Listener(const std::shared_ptr<DataStormContract::SessionPrx>& proxy, const std::string& facet) :
            proxy(facet.empty() ? proxy : Ice::uncheckedCast<DataStormContract::SessionPrx>(proxy->ice_facet(facet)))
        {
        }

        bool matchOne(const std::shared_ptr<Sample>& sample) const
        {
            for(const auto& s : keys.subscribers)
            {
                if(!s.second.sampleFilter || s.second.sampleFilter->match(sample))
                {
                    return true;
                }
            }
            for(const auto& s : filters.subscribers)
            {
                if(!s.second.sampleFilter || s.second.sampleFilter->match(sample))
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

    DataElementI(TopicI*, long long int, const DataStorm::Config&);
    virtual ~DataElementI();

    virtual void destroy() override;

    void attach(long long int,
                const std::shared_ptr<Key>&,
                const std::shared_ptr<Filter>&,
                SessionI*,
                const std::shared_ptr<DataStormContract::SessionPrx>&,
                const DataStormContract::ElementData&,
                const std::chrono::time_point<std::chrono::system_clock>&,
                DataStormContract::ElementDataAckSeq&);

    void attach(long long int,
                const std::shared_ptr<Key>&,
                const std::shared_ptr<Filter>&,
                SessionI*,
                const std::shared_ptr<DataStormContract::SessionPrx>&,
                const DataStormContract::ElementDataAck&,
                const std::chrono::time_point<std::chrono::system_clock>&,
                DataStormContract::DataSamplesSeq&);

    bool attachKey(long long int, long long int, const std::shared_ptr<Key>&, const std::shared_ptr<Filter>&,
                   SessionI*, const std::shared_ptr<DataStormContract::SessionPrx>&, const std::string&);
    void detachKey(long long int, long long int, SessionI*, const std::string&, bool = true);

    bool attachFilter(long long int, long long int, const std::shared_ptr<Filter>&, const std::shared_ptr<Filter>&,
                      SessionI*, const std::shared_ptr<DataStormContract::SessionPrx>&, const std::string&);
    void detachFilter(long long int, long long int, SessionI*, const std::string&, bool = true);

    virtual void onConnect(std::function<void(std::tuple<std::string, long long int, long long int>)>) override;
    virtual void onDisconnect(std::function<void(std::tuple<std::string, long long int, long long int>)>) override;

    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&, long long int, long long int,
                             const std::chrono::time_point<std::chrono::system_clock>&);
    virtual DataStormContract::DataSampleSeq getSamples(const std::shared_ptr<Key>&,
                                                        const std::shared_ptr<Filter>&,
                                                        const std::shared_ptr<DataStormContract::ElementConfig>&,
                                                        const std::chrono::time_point<std::chrono::system_clock>&);
    virtual void queue(const std::shared_ptr<Sample>&, const std::string&,
                       const std::chrono::time_point<std::chrono::system_clock>&);
    virtual std::shared_ptr<Filter> createSampleFilter(std::vector<unsigned char>) const;
    virtual std::string toString() const = 0;
    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

    long long int getId() const
    {
        return _id;
    }

    std::shared_ptr<DataStormContract::ElementConfig> getConfig() const
    {
        return _config;
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
    long long int _id;
    size_t _listenerCount;
    std::shared_ptr<DataStormContract::ElementConfig> _config;

private:

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const override;

    TopicI* _parent;
    std::map<ListenerKey, Listener> _listeners;
    mutable size_t _waiters;
    mutable size_t _notified;

    std::function<void(std::tuple<std::string, long long int, long long int>)> _onConnect;
    std::function<void(std::tuple<std::string, long long int, long long int>)> _onDisconnect;
};

class DataReaderI : public DataElementI, public DataReader
{
public:

    DataReaderI(TopicReaderI*, long long int, std::vector<unsigned char>, const DataStorm::ReaderConfig&);

    virtual int getInstanceCount() const override;

    virtual std::vector<std::shared_ptr<Sample>> getAll() override;
    virtual std::vector<std::shared_ptr<Sample>> getAllUnread() override;
    virtual void waitForUnread(unsigned int) const override;
    virtual bool hasUnread() const override;
    virtual std::shared_ptr<Sample> getNextUnread() override;

    virtual void initSamples(const std::vector<std::shared_ptr<Sample>>&, long long int, long long int,
                             const std::chrono::time_point<std::chrono::system_clock>&) override;
    virtual void queue(const std::shared_ptr<Sample>&, const std::string&,
                       const std::chrono::time_point<std::chrono::system_clock>&) override;

    virtual void onInit(std::function<void(const std::vector<std::shared_ptr<Sample>>&)>) override;
    virtual void onSample(std::function<void(const std::shared_ptr<Sample>&)>) override;

protected:

    TopicReaderI* _parent;

    std::deque<std::shared_ptr<Sample>> _all;
    std::deque<std::shared_ptr<Sample>> _unread;
    int _instanceCount;
    std::function<void(const std::vector<std::shared_ptr<Sample>>&)> _onInit;
    std::function<void(const std::shared_ptr<Sample>&)> _onSample;
};

class DataWriterI : public DataElementI, public DataWriter
{
public:

    DataWriterI(TopicWriterI*, long long int, const std::shared_ptr<FilterFactory>&, const DataStorm::WriterConfig&);

    virtual void publish(const std::shared_ptr<Key>&, const std::shared_ptr<Sample>&) override;
    virtual std::shared_ptr<Filter> createSampleFilter(std::vector<unsigned char>) const override;

protected:


    virtual void send(const std::shared_ptr<Key>&, const std::shared_ptr<Sample>&) const = 0;

    TopicWriterI* _parent;
    std::shared_ptr<FilterFactory> _sampleFilterFactory;
    std::shared_ptr<DataStormContract::SubscriberSessionPrx> _subscribers;
    std::deque<std::shared_ptr<Sample>> _all;
};

class KeyDataReaderI : public DataReaderI
{
public:

    KeyDataReaderI(TopicReaderI*, long long int, const std::vector<std::shared_ptr<Key>>&, std::vector<unsigned char>,
                   const DataStorm::ReaderConfig&);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual std::string toString() const override;

private:

    const std::vector<std::shared_ptr<Key>> _keys;
};

class KeyDataWriterI : public DataWriterI
{
public:

    KeyDataWriterI(TopicWriterI*, long long int, const std::vector<std::shared_ptr<Key>>&,
                   const std::shared_ptr<FilterFactory>&, const DataStorm::WriterConfig&);

    virtual void destroyImpl() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

    virtual std::string toString() const override;
    virtual DataStormContract::DataSampleSeq getSamples(const std::shared_ptr<Key>&,
                                                        const std::shared_ptr<Filter>&,
                                                        const std::shared_ptr<DataStormContract::ElementConfig>&,
                                                        const std::chrono::time_point<std::chrono::system_clock>&) override;

private:

    virtual void send(const std::shared_ptr<Key>&, const std::shared_ptr<Sample>&) const override;

    const std::vector<std::shared_ptr<Key>> _keys;
};

class FilteredDataReaderI : public DataReaderI
{
public:

    FilteredDataReaderI(TopicReaderI*, long long int, const std::shared_ptr<Filter>&, std::vector<unsigned char>,
                        const DataStorm::ReaderConfig&);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual std::string toString() const override;

private:

    const std::shared_ptr<Filter> _filter;
};

}