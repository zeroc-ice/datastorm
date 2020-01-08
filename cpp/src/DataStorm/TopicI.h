// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/InternalI.h>
#include <DataStorm/DataElementI.h>
#include <DataStorm/ForwarderManager.h>
#include <DataStorm/Instance.h>
#include <DataStorm/Types.h>

namespace DataStormI
{

class SessionI;
class TopicFactoryI;

class TopicI : virtual public Topic, public std::enable_shared_from_this<TopicI>
{
    struct ListenerKey
    {
        std::shared_ptr<SessionI> session;

        bool operator<(const ListenerKey& other) const
        {
            return session < other.session;
        }
    };

    struct Listener
    {
        Listener(const std::shared_ptr<DataStormContract::SessionPrx>& proxy) : proxy(proxy)
        {
        }

        std::set<long long int> topics;
        std::shared_ptr<DataStormContract::SessionPrx> proxy;
    };

public:

    TopicI(const std::weak_ptr<TopicFactoryI>&,
           const std::shared_ptr<KeyFactory>&,
           const std::shared_ptr<TagFactory>&,
           const std::shared_ptr<SampleFactory>&,
           const std::shared_ptr<FilterManager>&,
           const std::shared_ptr<FilterManager>&,
           const std::string&,
           long long int);

    virtual ~TopicI();

    void init();

    virtual std::string getName() const override;
    virtual void destroy() override;

    void shutdown();

    const std::shared_ptr<Instance>& getInstance() const
    {
        return _instance;
    }

    DataStormContract::TopicSpec getTopicSpec() const;
    DataStormContract::ElementInfoSeq getTags() const;
    DataStormContract::ElementSpecSeq getElementSpecs(long long int, const DataStormContract::ElementInfoSeq&,
                                                      const std::shared_ptr<SessionI>&);

    void attach(long long int, const std::shared_ptr<SessionI>&, const std::shared_ptr<DataStormContract::SessionPrx>&);
    void detach(long long int, const std::shared_ptr<SessionI>&);

    DataStormContract::ElementSpecAckSeq attachElements(long long int,
                                                        const DataStormContract::ElementSpecSeq&,
                                                        const std::shared_ptr<SessionI>&,
                                                        const std::shared_ptr<DataStormContract::SessionPrx>&,
                                                        const std::chrono::time_point<std::chrono::system_clock>&);

    DataStormContract::DataSamplesSeq attachElementsAck(long long int,
                                                        const DataStormContract::ElementSpecAckSeq&,
                                                        const std::shared_ptr<SessionI>&,
                                                        const std::shared_ptr<DataStormContract::SessionPrx>&,
                                                        const std::chrono::time_point<std::chrono::system_clock>&,
                                                        DataStormContract::LongSeq&);

    virtual void setUpdater(const std::shared_ptr<Tag>&, Updater) override;
    const Updater& getUpdater(const std::shared_ptr<Tag>&) const;

    virtual void setUpdaters(std::map<std::shared_ptr<Tag>, Updater>) override;
    virtual std::map<std::shared_ptr<Tag>, Updater> getUpdaters() const override;

    bool isDestroyed() const
    {
        return _destroyed;
    }

    long long int getId() const
    {
        return _id;
    }

    std::mutex& getMutex()
    {
        return _mutex;
    }

    const std::shared_ptr<KeyFactory>& getKeyFactory() const
    {
        return _keyFactory;
    }

    const std::shared_ptr<TagFactory>& getTagFactory() const
    {
        return _tagFactory;
    }

    const std::shared_ptr<SampleFactory>& getSampleFactory() const
    {
        return _sampleFactory;
    }

    const std::shared_ptr<FilterManager>& getSampleFilterFactories() const
    {
        return _sampleFilterFactories;
    }

    void incListenerCount(const std::shared_ptr<SessionI>&);
    void decListenerCount(const std::shared_ptr<SessionI>&);
    void decListenerCount(size_t);

    void removeFiltered(const std::shared_ptr<DataElementI>&, const std::shared_ptr<Filter>&);
    void remove(const std::shared_ptr<DataElementI>&, const std::vector<std::shared_ptr<Key>>&);

protected:

    void waitForListeners(int count) const;
    bool hasListeners() const;
    void notifyListenerWaiters(std::unique_lock<std::mutex>&) const;

    void disconnect();

    void forward(const Ice::ByteSeq&, const Ice::Current&) const;
    void forwarderException() const;

    void add(const std::shared_ptr<DataElementI>&, const std::vector<std::shared_ptr<Key>>&);
    void addFiltered(const std::shared_ptr<DataElementI>&, const std::shared_ptr<Filter>&);

    void parseConfigImpl(const Ice::PropertyDict&, const std::string&, DataStorm::Config&) const;

    friend class DataElementI;
    friend class DataReaderI;
    friend class FilteredDataReaderI;
    friend class DataWriterI;
    friend class KeyDataWriterI;
    friend class KeyDataReaderI;

    const std::weak_ptr<TopicFactoryI> _factory;
    const std::shared_ptr<KeyFactory> _keyFactory;
    const std::shared_ptr<TagFactory> _tagFactory;
    const std::shared_ptr<SampleFactory> _sampleFactory;
    const std::shared_ptr<FilterManager> _keyFilterFactories;
    const std::shared_ptr<FilterManager> _sampleFilterFactories;
    const std::string _name;
    const std::shared_ptr<Instance> _instance;
    const std::shared_ptr<TraceLevels> _traceLevels;
    const long long int _id;
    std::shared_ptr<DataStormContract::SessionPrx> _forwarder;

    mutable std::mutex _mutex;
    mutable std::condition_variable _cond;
    bool _destroyed;
    std::map<std::shared_ptr<Key>, std::set<std::shared_ptr<DataElementI>>> _keyElements;
    std::map<std::shared_ptr<Filter>, std::set<std::shared_ptr<DataElementI>>> _filteredElements;
    std::map<ListenerKey, Listener> _listeners;
    std::map<std::shared_ptr<Tag>, Updater> _updaters;
    size_t _listenerCount;
    mutable size_t _waiters;
    mutable size_t _notified;
    long long int _nextId;
    long long int _nextFilteredId;
    long long int _nextSampleId;
};

class TopicReaderI : public TopicReader, public TopicI
{
public:

    TopicReaderI(const std::shared_ptr<TopicFactoryI>&,
                 const std::shared_ptr<KeyFactory>&,
                 const std::shared_ptr<TagFactory>&,
                 const std::shared_ptr<SampleFactory>&,
                 const std::shared_ptr<FilterManager>&,
                 const std::shared_ptr<FilterManager>&,
                 const std::string&,
                 long long int);

    virtual std::shared_ptr<DataReader> createFiltered(const std::shared_ptr<Filter>&,
                                                       const std::string&,
                                                       DataStorm::ReaderConfig,
                                                       const std::string&,
                                                       std::vector<unsigned char>) override;
    virtual std::shared_ptr<DataReader> create(const std::vector<std::shared_ptr<Key>>&,
                                               const std::string&,
                                               DataStorm::ReaderConfig,
                                               const std::string&,
                                               std::vector<unsigned char>) override;

    virtual void setDefaultConfig(DataStorm::ReaderConfig) override;
    virtual void waitForWriters(int) const override;
    virtual bool hasWriters() const override;
    virtual void destroy() override;

private:

    DataStorm::ReaderConfig parseConfig(const std::string&) const;
    DataStorm::ReaderConfig mergeConfigs(DataStorm::ReaderConfig) const;

    DataStorm::ReaderConfig _defaultConfig;
};

class TopicWriterI : public TopicWriter, public TopicI
{
public:

    TopicWriterI(const std::shared_ptr<TopicFactoryI>&,
                 const std::shared_ptr<KeyFactory>&,
                 const std::shared_ptr<TagFactory>&,
                 const std::shared_ptr<SampleFactory>&,
                 const std::shared_ptr<FilterManager>&,
                 const std::shared_ptr<FilterManager>&,
                 const std::string&,
                 long long int);

    virtual std::shared_ptr<DataWriter> create(const std::vector<std::shared_ptr<Key>>&,
                                               const std::string&,
                                               DataStorm::WriterConfig) override;

    virtual void setDefaultConfig(DataStorm::WriterConfig) override;
    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;
    virtual void destroy() override;

private:

    DataStorm::WriterConfig parseConfig(const std::string&) const;
    DataStorm::WriterConfig mergeConfigs(DataStorm::WriterConfig) const;

    DataStorm::WriterConfig _defaultConfig;
};

}
