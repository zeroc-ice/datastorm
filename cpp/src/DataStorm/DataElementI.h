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
#include <DataStorm/ForwarderManager.h>

#include <Internal.h>

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
    struct Listener
    {
        long long int topic;
        long long int id;
        SessionI* session;

        bool operator<(const Listener& other) const
        {
            if(topic < other.topic)
            {
                return true;
            }
            else if(other.topic < topic)
            {
                return false;
            }
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

    DataElementI(TopicI*);
    virtual ~DataElementI();

    virtual void destroy() override;

    bool attachKey(long long int, long long int, const std::shared_ptr<Key>&, SessionI*,
                   const std::shared_ptr<DataStormContract::SessionPrx>&);
    void detachKey(long long int, long long int, SessionI*, bool = true);

    bool attachFilter(long long int, long long int, const std::shared_ptr<Filter>&, SessionI*,
                      const std::shared_ptr<DataStormContract::SessionPrx>&);
    void detachFilter(long long int, long long int, SessionI*, bool = true);

    virtual void queue(const std::shared_ptr<Sample>&);

    void waitForListeners(int count) const;
    bool hasListeners() const;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const override;

protected:

    void notifyListenerWaiters(std::unique_lock<std::mutex>&) const;
    void disconnect();
    virtual void destroyImpl() = 0;

    std::shared_ptr<TraceLevels> _traceLevels;
    const std::shared_ptr<DataStormContract::SessionPrx> _forwarder;

private:

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const override;

    TopicI* _parent;
    std::map<Listener, std::shared_ptr<DataStormContract::SessionPrx>> _keys;
    std::map<Listener, std::shared_ptr<DataStormContract::SessionPrx>> _filters;
    mutable size_t _waiters;
    mutable size_t _notified;
};

class KeyDataElementI : virtual public DataElementI
{
public:

    virtual DataStormContract::KeyInfo getKeyInfo() const = 0;
    virtual DataStormContract::KeyInfoAndSamples getKeyInfoAndSamples(long long int) const = 0;
};

class FilteredDataElementI : virtual public DataElementI
{
public:

    virtual DataStormContract::FilterInfo getFilterInfo() const = 0;
};

class DataReaderI : virtual public DataElementI, public DataReader
{
public:

    DataReaderI(TopicReaderI*);

    virtual int getInstanceCount() const override;

    virtual std::vector<std::shared_ptr<Sample>> getAll() const override;
    virtual std::vector<std::shared_ptr<Sample>> getAllUnread() override;
    virtual void waitForUnread(unsigned int) const override;
    virtual bool hasUnread() const override;
    virtual std::shared_ptr<Sample> getNextUnread() override;

    virtual void queue(const std::shared_ptr<Sample>&) override;

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

    virtual void add(const std::vector<unsigned char>&) override;
    virtual void update(const std::vector<unsigned char>&) override;
    virtual void remove() override;

protected:

    void publish(const std::shared_ptr<DataStormContract::DataSample>&);

    virtual void send(const std::shared_ptr<DataStormContract::DataSample>&) const = 0;

    TopicWriterI* _parent;
    std::shared_ptr<DataStormContract::SubscriberSessionPrx> _subscribers;
    std::deque<std::shared_ptr<DataStormContract::DataSample>> _all;
};

class KeyDataReaderI : public DataReaderI, public KeyDataElementI
{
public:

    KeyDataReaderI(TopicReaderI*, const std::shared_ptr<Key>&);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual DataStormContract::KeyInfo getKeyInfo() const override;
    virtual DataStormContract::KeyInfoAndSamples getKeyInfoAndSamples(long long int) const override;

private:

    const std::shared_ptr<Key> _key;
};

class KeyDataWriterI : public DataWriterI, public KeyDataElementI
{
public:

    KeyDataWriterI(TopicWriterI*, const std::shared_ptr<Key>&);

    virtual void destroyImpl() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

    virtual DataStormContract::KeyInfo getKeyInfo() const override;
    virtual DataStormContract::KeyInfoAndSamples getKeyInfoAndSamples(long long int) const override;

private:

    virtual void send(const std::shared_ptr<DataStormContract::DataSample>&) const override;

    const std::shared_ptr<Key> _key;
};

class FilteredDataReaderI : public DataReaderI, public FilteredDataElementI
{
public:

    FilteredDataReaderI(TopicReaderI*, const std::shared_ptr<Filter>&);

    virtual void destroyImpl() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual DataStormContract::FilterInfo getFilterInfo() const override;

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

    virtual DataStormContract::FilterInfo getFilterInfo() const override;

private:

    virtual void send(const std::shared_ptr<DataStormContract::DataSample>&) const override;

    const std::shared_ptr<Filter> _filter;
};

}