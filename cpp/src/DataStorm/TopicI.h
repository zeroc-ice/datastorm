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

namespace DataStormInternal
{

class Instance;
class SessionI;
class SubscriberSessionI;
class TopicPeer;
class TopicSubscriber;
class TopicPublisher;
class PublisherI;
class SubscriberI;

class TopicFactoryI;

class TopicI : virtual public Topic
{
public:

    TopicI(std::weak_ptr<TopicFactoryI>, std::shared_ptr<KeyFactory>, const std::string&);

    virtual std::shared_ptr<DataStorm::TopicFactory> getTopicFactory() const override;
    virtual void destroy() override;

    virtual std::string
    getName() const override
    {
        return _name;
    }

    std::shared_ptr<Instance> getInstance() const
    {
        return _instance;
    }

    void attach(SessionI*);
    void detach(SessionI*);

    void initKeysAndFilters(const DataStormContract::KeyInfoSeq&, const DataStormContract::StringSeq&, long long int,
                            SessionI*);

    void attachKey(const DataStormContract::KeyInfo&, long long int, SessionI*);
    void detachKey(const DataStormContract::Key&, SessionI*);

    void attachFilter(const std::string&, long long int, SessionI*);
    void detachFilter(const std::string&, SessionI*);

    std::shared_ptr<TopicSubscriber> getSubscriber() const;
    std::shared_ptr<TopicPublisher> getPublisher() const;

    std::shared_ptr<TopicPeer> getPeer() const
    {
        return _impl;
    }

protected:

    virtual void keysAndFiltersAttached(const DataStormContract::KeyInfoSeq&, const DataStormContract::StringSeq&,
                                        long long int, SessionI*) = 0;

    virtual void keyAttached(const DataStormContract::KeyInfo&, long long int, SessionI*) = 0;
    virtual void keyDetached(const DataStormContract::Key&, SessionI*) = 0;

    virtual void filterAttached(const std::string&, long long int, SessionI*) = 0;
    virtual void filterDetached(const std::string&, SessionI*) = 0;

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
    getFiltered(const std::string& filter)
    {
        auto p = _filteredElements.find(filter);
        if(p == _filteredElements.end())
        {
            p = _filteredElements.insert(make_pair(filter, makeFilteredElement(filter))).first;
        }
        return std::dynamic_pointer_cast<T>(p->second);
    }

    virtual std::shared_ptr<KeyDataElement> makeElement(const std::shared_ptr<Key>&) = 0;
    virtual std::shared_ptr<FilteredDataElement> makeFilteredElement(const std::string&) = 0;

    friend class DataElementI;
    friend class DataReaderI;
    friend class DataWriterI;

    std::mutex _mutex;
    std::condition_variable _cond;
    std::weak_ptr<TopicFactoryI> _factory;
    std::shared_ptr<KeyFactory> _keyFactory;
    std::string _name;
    std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    std::shared_ptr<TopicPeer> _impl;
    std::map<std::shared_ptr<Key>, std::shared_ptr<KeyDataElement>> _keyElements;
    std::map<std::string, std::shared_ptr<FilteredDataElement>> _filteredElements;
    long long int _id;
};

class TopicReaderI : public TopicReader, public TopicI
{
public:

    TopicReaderI(std::shared_ptr<TopicFactoryI>, std::shared_ptr<KeyFactory>, const std::string&, SubscriberI*);

    virtual std::shared_ptr<DataReader> getFilteredDataReader(const std::string&) override;
    virtual std::shared_ptr<DataReader> getDataReader(const std::shared_ptr<Key>&) override;
    virtual void destroy() override;

    void removeFiltered(const std::string&, const std::shared_ptr<FilteredDataReaderI>&);
    void remove(const std::shared_ptr<Key>&, const std::shared_ptr<KeyDataReaderI>&);

    void queue(const DataStormContract::Key&, const DataStormContract::DataSampleSeq&, SubscriberSessionI*);
    void queue(const DataStormContract::Key&, const DataStormContract::DataSamplePtr&, SubscriberSessionI*);
    void queue(const std::string&, const DataStormContract::DataSamplePtr&, SubscriberSessionI*);

private:

    virtual std::shared_ptr<KeyDataElement> makeElement(const std::shared_ptr<Key>& key) override
    {
        return std::make_shared<KeyDataReaderI>(this, key);
    }

    virtual std::shared_ptr<FilteredDataElement> makeFilteredElement(const std::string& filter) override
    {
        return std::make_shared<FilteredDataReaderI>(this, filter);
    }

    virtual void keysAndFiltersAttached(const DataStormContract::KeyInfoSeq&, const DataStormContract::StringSeq&,
                                        long long int, SessionI*) override;

    virtual void keyAttached(const DataStormContract::KeyInfo&, long long int, SessionI*) override;
    virtual void keyDetached(const DataStormContract::Key&, SessionI*) override;

    virtual void filterAttached(const std::string&, long long int, SessionI*) override;
    virtual void filterDetached(const std::string&, SessionI*) override;

    std::map<std::shared_ptr<Key>, std::set<std::shared_ptr<DataReaderI>>> _elements;
    std::map<std::shared_ptr<Key>, std::set<std::shared_ptr<DataReaderI>>> _filters;
};

class TopicWriterI : public TopicWriter, public TopicI
{
public:

    TopicWriterI(std::shared_ptr<TopicFactoryI>, std::shared_ptr<KeyFactory>, const std::string&, PublisherI*);

    virtual std::shared_ptr<DataWriter> getFilteredDataWriter(const std::string&) override;
    virtual std::shared_ptr<DataWriter> getDataWriter(const std::shared_ptr<Key>&) override;
    virtual void destroy() override;

    void removeFiltered(const std::string&, const std::shared_ptr<FilteredDataWriterI>&);
    void remove(const std::shared_ptr<Key>&, const std::shared_ptr<KeyDataWriterI>&);

private:

    virtual std::shared_ptr<KeyDataElement> makeElement(const std::shared_ptr<Key>& key) override
    {
        return std::make_shared<KeyDataWriterI>(this, key);
    }

    virtual std::shared_ptr<FilteredDataElement> makeFilteredElement(const std::string& filter) override
    {
        return std::make_shared<FilteredDataWriterI>(this, filter);
    }

    virtual void keysAndFiltersAttached(const DataStormContract::KeyInfoSeq&, const DataStormContract::StringSeq&,
                                        long long int, SessionI*) override;

    virtual void keyAttached(const DataStormContract::KeyInfo&, long long int, SessionI*) override;
    virtual void keyDetached(const DataStormContract::Key&, SessionI*) override;

    virtual void filterAttached(const std::string&, long long int, SessionI*) override;
    virtual void filterDetached(const std::string&, SessionI*) override;
};

class TopicFactoryI : public TopicFactory, public std::enable_shared_from_this<TopicFactoryI>
{
public:

    TopicFactoryI(std::shared_ptr<Ice::Communicator> = nullptr);

    virtual void init(std::weak_ptr<DataStorm::TopicFactory>) override;

    virtual std::shared_ptr<TopicReader> createTopicReader(const std::string&, std::shared_ptr<KeyFactory>) override;
    virtual std::shared_ptr<TopicWriter> createTopicWriter(const std::string&, std::shared_ptr<KeyFactory>) override;

    virtual void waitForShutdown() override;
    virtual void shutdown() override;
    virtual void destroy() override;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

    void removeTopicReader(const std::string&);
    void removeTopicWriter(const std::string&);

    std::shared_ptr<TopicReaderI> getTopicReader(const std::string&) const;
    std::shared_ptr<TopicWriterI> getTopicWriter(const std::string&) const;

    void createSession(const std::string&, std::shared_ptr<DataStormContract::PublisherPrx>);
    void createSession(const std::string&, std::shared_ptr<DataStormContract::SubscriberPrx>);

    std::shared_ptr<Instance> getInstance() const
    {
        return _instance;
    }

    std::vector<std::string> getTopicReaders() const;
    std::vector<std::string> getTopicWriters() const;

private:

    mutable std::mutex _mutex;
    std::shared_ptr<Instance> _instance;
    std::shared_ptr<TraceLevels> _traceLevels;
    std::map<std::string, std::shared_ptr<TopicReaderI>> _readers;
    std::map<std::string, std::shared_ptr<TopicWriterI>> _writers;
    std::shared_ptr<SubscriberI> _subscriber;
    std::shared_ptr<PublisherI> _publisher;
};

}