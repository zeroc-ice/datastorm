// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/DataStorm.h>
#include <DataStorm/DataElementI.h>
#include <DataStorm/ForwarderManager.h>

namespace DataStormInternal
{

class Instance;
class SessionI;
class SubscriberSessionI;
class PublisherI;
class SubscriberI;

class TopicFactoryI;

class TopicI : virtual public Topic, private Forwarder
{

    struct Listener
    {
        long long int id;
        SessionI* session;

        bool operator<(const Listener& other) const
        {
            if(id < other.id)
            {
                return true;
            }
            else if(other.id < id)
            {
                return false;
            }
            return session < other.session;
        }
    };

public:

    TopicI(const std::weak_ptr<TopicFactoryI>&,
           const std::shared_ptr<KeyFactory>&,
           const std::shared_ptr<FilterFactory>&,
           const std::string&,
           long long int);

    virtual ~TopicI();

    virtual std::string getName() const override;
    virtual std::shared_ptr<KeyFactory> getKeyFactory() const override;
    virtual std::shared_ptr<FilterFactory> getFilterFactory() const override;
    virtual void destroy() override;

    const std::shared_ptr<Instance>& getInstance() const
    {
        return _instance;
    }

    DataStormContract::KeyInfoSeq getKeyInfoSeq() const;
    DataStormContract::FilterInfoSeq getFilterInfoSeq() const;
    DataStormContract::TopicInfoAndContent getTopicInfoAndContent(long long int) const;

    void attach(long long int, SessionI*, const std::shared_ptr<DataStormContract::SessionPrx>&);
    void detach(long long int, SessionI*, bool = true);

    DataStormContract::KeyInfoAndSamplesSeq
    attachKeysAndFilters(long long int,
                         const DataStormContract::KeyInfoSeq&,
                         const DataStormContract::FilterInfoSeq&,
                         long long int,
                         SessionI*,
                         const std::shared_ptr<DataStormContract::SessionPrx>&);

    DataStormContract::DataSamplesSeq
    attachKeysAndFilters(long long int,
                         const DataStormContract::KeyInfoAndSamplesSeq&,
                         const DataStormContract::FilterInfoSeq&,
                         long long int,
                         SessionI*,
                         const std::shared_ptr<DataStormContract::SessionPrx>&);

    std::pair<DataStormContract::KeyInfoAndSamplesSeq, DataStormContract::FilterInfoSeq>
    attachKey(long long int,
              const DataStormContract::KeyInfo&,
              long long int,
              SessionI*,
              const std::shared_ptr<DataStormContract::SessionPrx>&);

    DataStormContract::KeyInfoAndSamplesSeq
    attachFilter(long long int,
                 const DataStormContract::FilterInfo&,
                 long long int,
                 SessionI*,
                 const std::shared_ptr<DataStormContract::SessionPrx>&);

    long long int getId() const
    {
        return _id;
    }

    std::mutex&
    getMutex()
    {
        return _mutex;
    }

protected:

    void disconnect();

    void attachKeyImpl(long long int,
                       const DataStormContract::KeyInfo&,
                       const DataStormContract::DataSampleSeq&,
                       long long int,
                       SessionI*,
                       const std::shared_ptr<DataStormContract::SessionPrx>&,
                       DataStormContract::KeyInfoAndSamplesSeq&,
                       DataStormContract::FilterInfoSeq&);

    void attachFilterImpl(long long int,
                          const DataStormContract::FilterInfo&,
                          long long int,
                          SessionI*,
                          const std::shared_ptr<DataStormContract::SessionPrx>&,
                          DataStormContract::KeyInfoAndSamplesSeq&);

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const override;

    template<typename T> std::shared_ptr<T>
    get(const std::shared_ptr<Key>& key)
    {
        auto p = _keyElements.find(key);
        if(p == _keyElements.end())
        {
            p = _keyElements.insert(make_pair(key, makeElement(key))).first;
        }
        return std::dynamic_pointer_cast<T>(p->second);
    }

    template<typename T> std::shared_ptr<T>
    getFiltered(const std::shared_ptr<Filter>& filter)
    {
        auto p = _filteredElements.find(filter);
        if(p == _filteredElements.end())
        {
            p = _filteredElements.insert(make_pair(filter, makeFilteredElement(filter))).first;
        }
        return std::dynamic_pointer_cast<T>(p->second);
    }

    virtual std::shared_ptr<KeyDataElementI> makeElement(const std::shared_ptr<Key>&) = 0;
    virtual std::shared_ptr<FilteredDataElementI> makeFilteredElement(const std::shared_ptr<Filter>&) = 0;

    friend class DataElementI;
    friend class DataReaderI;
    friend class DataWriterI;

    const std::weak_ptr<TopicFactoryI> _factory;
    const std::shared_ptr<KeyFactory> _keyFactory;
    const std::shared_ptr<FilterFactory> _filterFactory;
    const std::string _name;
    const std::shared_ptr<Instance> _instance;
    const std::shared_ptr<TraceLevels> _traceLevels;
    const long long int _id;
    const std::shared_ptr<DataStormContract::SessionPrx> _forwarder;

    std::mutex _mutex;
    std::condition_variable _cond;
    std::map<std::shared_ptr<Key>, std::shared_ptr<KeyDataElementI>> _keyElements;
    std::map<std::shared_ptr<Filter>, std::shared_ptr<FilteredDataElementI>> _filteredElements;
    std::map<Listener, std::shared_ptr<DataStormContract::SessionPrx>> _sessions;
    long long int _nextSampleId;
};

class TopicReaderI : public TopicReader, public TopicI
{
public:

    TopicReaderI(const std::shared_ptr<TopicFactoryI>&,
                 const std::shared_ptr<KeyFactory>&,
                 const std::shared_ptr<FilterFactory>&,
                 const std::string&,
                 long long int);

    virtual std::shared_ptr<DataReader> getFilteredDataReader(const std::shared_ptr<Filter>&) override;
    virtual std::shared_ptr<DataReader> getDataReader(const std::shared_ptr<Key>&) override;
    virtual void destroy() override;

    void removeFiltered(const std::shared_ptr<Filter>&);
    void remove(const std::shared_ptr<Key>&);

private:

    virtual std::shared_ptr<KeyDataElementI> makeElement(const std::shared_ptr<Key>&) override;
    virtual std::shared_ptr<FilteredDataElementI> makeFilteredElement(const std::shared_ptr<Filter>&) override;
};

class TopicWriterI : public TopicWriter, public TopicI
{
public:

    TopicWriterI(const std::shared_ptr<TopicFactoryI>&,
                 const std::shared_ptr<KeyFactory>&,
                 const std::shared_ptr<FilterFactory>&,
                 const std::string&,
                 long long int);

    virtual std::shared_ptr<DataWriter> getFilteredDataWriter(const std::shared_ptr<Filter>&) override;
    virtual std::shared_ptr<DataWriter> getDataWriter(const std::shared_ptr<Key>&) override;
    virtual void destroy() override;

    void removeFiltered(const std::shared_ptr<Filter>&);
    void remove(const std::shared_ptr<Key>&);

private:

    virtual std::shared_ptr<KeyDataElementI> makeElement(const std::shared_ptr<Key>&) override;
    virtual std::shared_ptr<FilteredDataElementI> makeFilteredElement(const std::shared_ptr<Filter>&) override;
};

class TopicFactoryI : public TopicFactory, public std::enable_shared_from_this<TopicFactoryI>
{
public:

    TopicFactoryI(const std::shared_ptr<Ice::Communicator>&);

    virtual void init() override;

    virtual std::shared_ptr<TopicReader> getTopicReader(const std::string&,
                                                        std::function<std::shared_ptr<KeyFactory>()>,
                                                        std::function<std::shared_ptr<FilterFactory>()>) override;

    virtual std::shared_ptr<TopicWriter> getTopicWriter(const std::string&,
                                                        std::function<std::shared_ptr<KeyFactory>()>,
                                                        std::function<std::shared_ptr<FilterFactory>()>) override;

    virtual void destroy(bool) override;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

    void removeTopicReader(const std::string&);
    void removeTopicWriter(const std::string&);

    std::shared_ptr<TopicReaderI> getTopicReader(const std::string&) const;
    std::shared_ptr<TopicWriterI> getTopicWriter(const std::string&) const;

    void createSession(const std::string&, const std::shared_ptr<DataStormContract::PublisherPrx>&);
    void createSession(const std::string&, const std::shared_ptr<DataStormContract::SubscriberPrx>&);

    std::shared_ptr<Instance> getInstance() const
    {
        return _instance;
    }

    DataStormContract::TopicInfoSeq getTopicReaders() const;
    DataStormContract::TopicInfoSeq getTopicWriters() const;

private:

    mutable std::mutex _mutex;
    std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    std::map<std::string, std::shared_ptr<TopicReaderI>> _readers;
    std::map<std::string, std::shared_ptr<TopicWriterI>> _writers;
    std::shared_ptr<SubscriberI> _subscriber;
    std::shared_ptr<PublisherI> _publisher;
    long long int _nextReaderId;
    long long int _nextWriterId;
};

}