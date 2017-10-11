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
#include <DataStorm/DataElementI.h>
#include <DataStorm/ForwarderManager.h>
#include <DataStorm/Instance.h>

namespace DataStormInternal
{

class SessionI;
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
           typename Sample::FactoryType,
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
    attachKeys(long long int,
               const DataStormContract::KeyInfoSeq&,
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

    const typename Sample::FactoryType&
    getSampleFactory() const
    {
        return _sampleFactory;
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
    add(const std::shared_ptr<T>& element, const std::vector<std::shared_ptr<Key>>& keys)
    {
        DataStormContract::KeyInfoSeq infos;
        for(const auto& key : keys)
        {
            auto p = _keyElements.find(key);
            if(p == _keyElements.end())
            {
                p = _keyElements.emplace(key, std::set<std::shared_ptr<KeyDataElementI>>()).first;
                infos.push_back({ key->getId(), key->encode(_instance->getCommunicator()) });
            }
            p->second.insert(element);
        }
        if(!infos.empty())
        {
            _forwarder->announceKeys(_id, infos);
        }
        return element;
    }

    template<typename T> std::shared_ptr<T>
    addFiltered(const std::shared_ptr<T>& element, const std::shared_ptr<Filter>& filter)
    {
        auto p = _filteredElements.find(filter);
        if(p == _filteredElements.end())
        {
            p = _filteredElements.emplace(filter, std::set<std::shared_ptr<FilteredDataElementI>>()).first;
            _forwarder->announceFilter(_id, { filter->getId(), filter->encode(_instance->getCommunicator()) });
        }
        p->second.insert(element);
        return element;
    }

    friend class DataElementI;
    friend class DataReaderI;
    friend class DataWriterI;

    const std::weak_ptr<TopicFactoryI> _factory;
    const std::shared_ptr<KeyFactory> _keyFactory;
    const std::shared_ptr<FilterFactory> _filterFactory;
    const typename Sample::FactoryType _sampleFactory;
    const std::string _name;
    const std::shared_ptr<Instance> _instance;
    const std::shared_ptr<TraceLevels> _traceLevels;
    const long long int _id;
    const std::shared_ptr<DataStormContract::SessionPrx> _forwarder;

    std::mutex _mutex;
    std::condition_variable _cond;
    std::map<std::shared_ptr<Key>, std::set<std::shared_ptr<KeyDataElementI>>> _keyElements;
    std::map<std::shared_ptr<Filter>, std::set<std::shared_ptr<FilteredDataElementI>>> _filteredElements;
    std::map<Listener, std::shared_ptr<DataStormContract::SessionPrx>> _sessions;
    long long int _nextSampleId;
};

class TopicReaderI : public TopicReader, public TopicI
{
public:

    TopicReaderI(const std::shared_ptr<TopicFactoryI>&,
                 const std::shared_ptr<KeyFactory>&,
                 const std::shared_ptr<FilterFactory>&,
                 typename Sample::FactoryType,
                 const std::string&,
                 long long int);

    virtual std::shared_ptr<DataReader> createFilteredDataReader(const std::shared_ptr<Filter>&) override;
    virtual std::shared_ptr<DataReader> createDataReader(const std::vector<std::shared_ptr<Key>>&) override;
    virtual void destroy() override;

    void removeFiltered(const std::shared_ptr<Filter>&, const std::shared_ptr<FilteredDataElementI>&);
    void remove(const std::vector<std::shared_ptr<Key>>&, const std::shared_ptr<KeyDataElementI>&);
};

class TopicWriterI : public TopicWriter, public TopicI
{
public:

    TopicWriterI(const std::shared_ptr<TopicFactoryI>&,
                 const std::shared_ptr<KeyFactory>&,
                 const std::shared_ptr<FilterFactory>&,
                 typename Sample::FactoryType,
                 const std::string&,
                 long long int);

    virtual std::shared_ptr<DataWriter> createFilteredDataWriter(const std::shared_ptr<Filter>&) override;
    virtual std::shared_ptr<DataWriter> createDataWriter(const std::vector<std::shared_ptr<Key>>&) override;
    virtual void destroy() override;

    void removeFiltered(const std::shared_ptr<Filter>&, const std::shared_ptr<FilteredDataElementI>&);
    void remove(const std::vector<std::shared_ptr<Key>>&, const std::shared_ptr<KeyDataElementI>&);
};

}