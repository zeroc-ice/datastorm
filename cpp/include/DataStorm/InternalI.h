// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Communicator.h>

#include <DataStorm/Config.h>
#include <DataStorm/SampleType.h>
#include <DataStorm/Filter.h>

//
// Private abstract API used by the template based API and the internal DataStorm implementation.
//
namespace DataStormInternal
{

class Element
{
public:

    virtual std::string toString() const = 0;
    virtual std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>&) const = 0;
    virtual long long int getId() const = 0;
};

class Key : virtual public Element
{
};

class KeyFactory
{
public:

    virtual std::shared_ptr<Key> decode(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&) = 0;
};

class Sample
{
public:

    using FactoryType = std::function<std::shared_ptr<Sample>(DataStorm::SampleType,
                                                              const std::shared_ptr<Key>&,
                                                              std::vector<unsigned char>,
                                                              long long int)>;

    Sample(DataStorm::SampleType type,
           const std::shared_ptr<Key>& key,
           std::vector<unsigned char> value,
           long long int timestamp) :
        type(type), key(key), timestamp(timestamp), _encodedValue(std::move(value))
    {
    }

    virtual void decode(const std::shared_ptr<Ice::Communicator>&) = 0;
    virtual const std::vector<unsigned char>& encode(const std::shared_ptr<Ice::Communicator>&) = 0;

    long long int id;
    DataStorm::SampleType type;
    std::shared_ptr<Key> key;
    long long int timestamp;

protected:

    std::vector<unsigned char> _encodedValue;
};

class Filter : virtual public Element
{
public:

    virtual bool match(const std::shared_ptr<Key>&) const = 0;
    virtual bool match(const std::shared_ptr<Sample>&, bool) const = 0;
};

class FilterFactory
{
public:

    virtual std::shared_ptr<Filter> decode(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&) = 0;
};

class DataElement
{
public:

    virtual void destroy() = 0;
    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const = 0;
};

class DataReader : virtual public DataElement
{
public:

    virtual bool hasWriters() = 0;
    virtual void waitForWriters(int) = 0;
    virtual int getInstanceCount() const = 0;

    virtual std::vector<std::shared_ptr<Sample>> getAll() = 0;

    virtual std::vector<std::shared_ptr<Sample>> getAllUnread() = 0;
    virtual void waitForUnread(unsigned int) const = 0;
    virtual bool hasUnread() const = 0;
    virtual std::shared_ptr<Sample> getNextUnread() = 0;
};

class DataWriter : virtual public DataElement
{
public:

    virtual bool hasReaders() const = 0;
    virtual void waitForReaders(int) const = 0;

    virtual void publish(const std::shared_ptr<Sample>&) = 0;
};

class Topic
{
public:

    virtual std::string getName() const = 0;
    virtual std::shared_ptr<KeyFactory> getKeyFactory() const = 0;
    virtual std::shared_ptr<FilterFactory> getFilterFactory() const = 0;
    virtual void destroy() = 0;
};

class TopicReader : virtual public Topic
{
public:

    virtual std::shared_ptr<DataReader> createFilteredDataReader(const std::shared_ptr<Filter>&) = 0;
    virtual std::shared_ptr<DataReader> createDataReader(const std::vector<std::shared_ptr<Key>>&) = 0;
};

class TopicWriter : virtual public Topic
{
public:

    virtual std::shared_ptr<DataWriter> createFilteredDataWriter(const std::shared_ptr<Filter>&) = 0;
    virtual std::shared_ptr<DataWriter> createDataWriter(const std::vector<std::shared_ptr<Key>>&) = 0;
};

class TopicFactory
{
public:

    virtual void init() = 0;

    virtual std::shared_ptr<TopicReader> getTopicReader(const std::string&,
                                                        std::function<std::shared_ptr<KeyFactory>()>,
                                                        std::function<std::shared_ptr<FilterFactory>()>,
                                                        typename Sample::FactoryType) = 0;

    virtual std::shared_ptr<TopicWriter> getTopicWriter(const std::string&,
                                                        std::function<std::shared_ptr<KeyFactory>()>,
                                                        std::function<std::shared_ptr<FilterFactory>()>,
                                                        typename Sample::FactoryType) = 0;

    virtual void destroy(bool) = 0;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const = 0;
};
std::shared_ptr<TopicFactory> createTopicFactory(const std::shared_ptr<Ice::Communicator>&);

};
