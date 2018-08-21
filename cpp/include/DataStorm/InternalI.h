// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Communicator.h>

#include <DataStorm/Config.h>
#include <DataStorm/Sample.h>
#include <DataStorm/Types.h>

//
// Private abstract API used by the template based API and the internal DataStorm implementation.
//
namespace DataStormI
{

class Instance;

class Filterable
{
};

class Element
{
public:

    virtual std::string toString() const = 0;
    virtual std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>&) const = 0;
    virtual long long int getId() const = 0;
};

class Key : public Filterable, virtual public Element
{
};

class KeyFactory
{
public:
    virtual std::shared_ptr<Key> get(long long int) const = 0;
    virtual std::shared_ptr<Key> decode(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&) = 0;
};

class Tag : virtual public Element
{
};

class TagFactory
{
public:

    virtual std::shared_ptr<Tag> get(long long int) const = 0;
    virtual std::shared_ptr<Tag> decode(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&) = 0;
};

class Sample : public Filterable
{
public:

    Sample(const std::string& session,
           long long int topic,
           long long int element,
           long long int id,
           DataStorm::SampleEvent event,
           const std::shared_ptr<Key>& key,
           const std::shared_ptr<Tag>& tag,
           std::vector<unsigned char> value,
           long long int timestamp) :
        session(session), topic(topic), element(element), id(id), event(event), key(key), tag(tag),
        timestamp(std::chrono::microseconds(timestamp)),
        _encodedValue(std::move(value))
    {
    }

    Sample(DataStorm::SampleEvent event, const std::shared_ptr<Tag>& tag = nullptr) : event(event), tag(tag)
    {
    }

    virtual bool hasValue() const = 0;
    virtual void setValue(const std::shared_ptr<Sample>&) = 0;

    virtual void decode(const std::shared_ptr<Ice::Communicator>&) = 0;
    virtual const std::vector<unsigned char>& encode(const std::shared_ptr<Ice::Communicator>&) = 0;
    virtual std::vector<unsigned char> encodeValue(const std::shared_ptr<Ice::Communicator>&) = 0;

    const std::vector<unsigned char>& getEncodedValue() const
    {
        return _encodedValue;
    }

    std::string session;
    long long int topic;
    long long int element;
    long long int id;
    DataStorm::SampleEvent event;
    std::shared_ptr<Key> key;
    std::shared_ptr<Tag> tag;
    std::chrono::time_point<std::chrono::system_clock> timestamp;

protected:

    std::vector<unsigned char> _encodedValue;
};

class SampleFactory
{
public:

    virtual std::shared_ptr<Sample> create(const std::string&,
                                           long long int,
                                           long long int,
                                           long long int,
                                           DataStorm::SampleEvent,
                                           const std::shared_ptr<Key>&,
                                           const std::shared_ptr<Tag>&,
                                           std::vector<unsigned char>,
                                           long long int) = 0;
};

class Filter : virtual public Element
{
public:

    virtual bool match(const std::shared_ptr<Filterable>&) const = 0;
    virtual const std::string& getName() const = 0;
};

class FilterFactory
{
public:

    virtual std::shared_ptr<Filter> get(long long int) const = 0;
    virtual std::shared_ptr<Filter> decode(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&) = 0;
};

class FilterManager
{
public:

    virtual std::shared_ptr<Filter> get(const std::string&, long long int) const = 0;

    virtual std::shared_ptr<Filter>
    decode(const std::shared_ptr<Ice::Communicator>&, const std::string&, const std::vector<unsigned char>&) = 0;
};

class DataElement
{
public:

    using Id = std::tuple<std::string, long long int, long long int>;

    virtual void onKeyConnect(std::function<void(Id, std::shared_ptr<Key>)>) = 0;
    virtual void onKeyDisconnect(std::function<void(Id, std::shared_ptr<Key>)>) = 0;
    virtual std::vector<std::shared_ptr<Key>> getConnectedKeys() const = 0;

    virtual void onFilterConnect(std::function<void(Id, std::shared_ptr<Filter>)>) = 0;
    virtual void onFilterDisconnect(std::function<void(Id, std::shared_ptr<Filter>)>) = 0;

    virtual void destroy() = 0;
    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const = 0;
};

class DataReader : virtual public DataElement
{
public:

    virtual bool hasWriters() = 0;
    virtual void waitForWriters(int) = 0;
    virtual int getInstanceCount() const = 0;

    virtual std::vector<std::shared_ptr<Sample>> getAllUnread() = 0;
    virtual void waitForUnread(unsigned int) const = 0;
    virtual bool hasUnread() const = 0;
    virtual std::shared_ptr<Sample> getNextUnread() = 0;

    virtual void onInit(std::function<void(const std::vector<std::shared_ptr<Sample>>&)>) = 0;
    virtual void onSample(std::function<void(const std::shared_ptr<Sample>&)>) = 0;
};

class DataWriter : virtual public DataElement
{
public:

    virtual bool hasReaders() const = 0;
    virtual void waitForReaders(int) const = 0;

    virtual std::shared_ptr<Sample> getLast() const = 0;
    virtual std::vector<std::shared_ptr<Sample>> getAll() const = 0;

    virtual void publish(const std::shared_ptr<Key>&, const std::shared_ptr<Sample>&) = 0;
};

class Topic
{
public:

    using Updater = std::function<void(const std::shared_ptr<Sample>&,
                                       const std::shared_ptr<Sample>&,
                                       const std::shared_ptr<Ice::Communicator>&)>;

    virtual void setUpdater(const std::shared_ptr<Tag>&, Updater) = 0;

    virtual void setUpdaters(std::map<std::shared_ptr<Tag>, Updater>) = 0;
    virtual std::map<std::shared_ptr<Tag>, Updater> getUpdaters() const = 0;

    virtual std::string getName() const = 0;
    virtual void destroy() = 0;
};

class TopicReader : virtual public Topic
{
public:

    virtual std::shared_ptr<DataReader> createFiltered(const std::shared_ptr<Filter>&,
                                                       DataStorm::ReaderConfig,
                                                       const std::string& = std::string(),
                                                       std::vector<unsigned char> = {}) = 0;

    virtual std::shared_ptr<DataReader> create(const std::vector<std::shared_ptr<Key>>&,
                                               DataStorm::ReaderConfig,
                                               const std::string& = std::string(),
                                               std::vector<unsigned char> = {}) = 0;

    virtual void setDefaultConfig(DataStorm::ReaderConfig) = 0;
    virtual bool hasWriters() const = 0;
    virtual void waitForWriters(int) const = 0;
};

class TopicWriter : virtual public Topic
{
public:

    virtual std::shared_ptr<DataWriter> create(const std::vector<std::shared_ptr<Key>>&, DataStorm::WriterConfig) = 0;

    virtual void setDefaultConfig(DataStorm::WriterConfig) = 0;
    virtual bool hasReaders() const = 0;
    virtual void waitForReaders(int) const = 0;
};

class TopicFactory
{
public:

    virtual std::shared_ptr<TopicReader> createTopicReader(const std::string&,
                                                           const std::shared_ptr<KeyFactory>&,
                                                           const std::shared_ptr<TagFactory>&,
                                                           const std::shared_ptr<SampleFactory>&,
                                                           const std::shared_ptr<FilterManager>&,
                                                           const std::shared_ptr<FilterManager>&) = 0;

    virtual std::shared_ptr<TopicWriter> createTopicWriter(const std::string&,
                                                           const std::shared_ptr<KeyFactory>&,
                                                           const std::shared_ptr<TagFactory>&,
                                                           const std::shared_ptr<SampleFactory>&,
                                                           const std::shared_ptr<FilterManager>&,
                                                           const std::shared_ptr<FilterManager>&) = 0;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const = 0;
};

};
