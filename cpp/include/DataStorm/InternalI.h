// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Communicator.h>

#include <DataStorm/Config.h>
#include <DataStorm/Sample.h>

//
// Private abstract API used by the template based API and the internal DataStorm implementation.
//
namespace DataStormInternal
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

class Sample : public Filterable, public std::enable_shared_from_this<Sample>
{
public:

    Sample(const std::string& session,
           long long int topic,
           long long int element,
           long long int id,
           DataStorm::SampleType type,
           const std::shared_ptr<Key>& key,
           std::vector<unsigned char> value,
           long long int timestamp) :
        session(session), topic(topic), element(element), id(id), type(type), key(key), timestamp(timestamp),
        _encodedValue(std::move(value))
    {
    }

    Sample(DataStorm::SampleType type) : type(type)
    {
    }

    virtual void decode(const std::shared_ptr<Ice::Communicator>&) = 0;
    virtual const std::vector<unsigned char>& encode(const std::shared_ptr<Ice::Communicator>&) = 0;

    std::string session;
    long long int topic;
    long long int element;
    long long int id;
    DataStorm::SampleType type;
    std::shared_ptr<Key> key;
    long long int timestamp;

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
                                           DataStorm::SampleType,
                                           const std::shared_ptr<Key>&,
                                           std::vector<unsigned char>,
                                           long long int) = 0;
};

class Filter : virtual public Element
{
public:

    virtual bool match(const std::shared_ptr<Filterable>&) const = 0;
};

class FilterFactory
{
public:

    virtual std::shared_ptr<Filter> get(long long int) const = 0;
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

    virtual std::shared_ptr<DataReader> createFiltered(const std::shared_ptr<Filter>&,
                                                       std::vector<unsigned char> = {}) = 0;
    virtual std::shared_ptr<DataReader> create(const std::vector<std::shared_ptr<Key>>&,
                                               std::vector<unsigned char> = {}) = 0;

    virtual bool hasWriters() const = 0;
    virtual void waitForWriters(int) const = 0;
};

class TopicWriter : virtual public Topic
{
public:

    virtual std::shared_ptr<DataWriter> createFiltered(const std::shared_ptr<Filter>&,
                                                       const std::shared_ptr<FilterFactory>& = nullptr) = 0;
    virtual std::shared_ptr<DataWriter> create(const std::shared_ptr<Key>&,
                                               const std::shared_ptr<FilterFactory>& = nullptr) = 0;

    virtual bool hasReaders() const = 0;
    virtual void waitForReaders(int) const = 0;
};

class TopicFactory
{
public:

    virtual std::shared_ptr<TopicReader> createTopicReader(const std::string&,
                                                           const std::shared_ptr<KeyFactory>&,
                                                           const std::shared_ptr<FilterFactory>&,
                                                           const std::shared_ptr<SampleFactory>&) = 0;

    virtual std::shared_ptr<TopicWriter> createTopicWriter(const std::string&,
                                                           const std::shared_ptr<KeyFactory>&,
                                                           const std::shared_ptr<FilterFactory>&,
                                                           const std::shared_ptr<SampleFactory>&) = 0;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const = 0;
};

};
