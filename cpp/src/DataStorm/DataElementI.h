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

#include <Internal.h>

#include <deque>

namespace DataStormInternal
{

class TopicSubscriber;
class TopicPublisher;

class TopicI;
class TopicReaderI;
class TopicWriterI;

class TraceLevels;

class DataElementI : virtual public DataElement
{
public:

    DataElementI(TopicI*);

    virtual std::shared_ptr<DataStorm::TopicFactory> getTopicFactory() const override;

protected:

    std::shared_ptr<TraceLevels> _traceLevels;

private:

    TopicI* _parent;
};

class KeyDataElement : virtual public DataElement
{
public:

    virtual DataStormContract::KeyInfo getKeyInfo() const = 0;
};

class FilteredDataElement : virtual public DataElement
{
};

class DataReaderI : public DataElementI, public DataReader
{
public:

    DataReaderI(TopicReaderI*);

    virtual int getInstanceCount() const override;

    virtual std::vector<std::shared_ptr<Sample>> getAll() const override;
    virtual std::vector<std::shared_ptr<Sample>> getAllUnread() override;
    virtual void waitForUnread(unsigned int) const override;
    virtual bool hasUnread() const override;
    virtual std::shared_ptr<Sample> getNextUnread() override;

    void queue(std::shared_ptr<Sample>);

protected:

    TopicReaderI* _parent;
    std::shared_ptr<TopicSubscriber> _subscriber;

    std::deque<std::shared_ptr<Sample>> _all;
    std::deque<std::shared_ptr<Sample>> _unread;
    int _instanceCount;
};

class DataWriterI : public DataElementI, public DataWriter
{
public:

    DataWriterI(TopicWriterI*);

    virtual void add(std::vector<unsigned char>) override;
    virtual void update(std::vector<unsigned char>) override;
    virtual void remove() override;

protected:

    void publish(std::shared_ptr<DataStormContract::DataSample>);

    virtual void send(const std::shared_ptr<DataStormContract::DataSample>&) const = 0;

    TopicWriterI* _parent;
    std::shared_ptr<TopicPublisher> _publisher;

    std::deque<std::shared_ptr<DataStormContract::DataSample>> _all;
};

class KeyDataReaderI : public DataReaderI, public KeyDataElement, public std::enable_shared_from_this<KeyDataReaderI>
{
public:

    KeyDataReaderI(TopicReaderI*, const std::shared_ptr<Key>&);

    virtual void destroy() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

    virtual DataStormContract::KeyInfo getKeyInfo() const override;

private:

    std::shared_ptr<Key> _key;
};

class KeyDataWriterI : public DataWriterI, public KeyDataElement, public std::enable_shared_from_this<KeyDataWriterI>
{
public:

    KeyDataWriterI(TopicWriterI*, const std::shared_ptr<Key>&);

    virtual void destroy() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

    virtual DataStormContract::KeyInfo getKeyInfo() const override;

    void init(long long int, DataStormContract::DataSamplesSeq&);

private:

    virtual void send(const std::shared_ptr<DataStormContract::DataSample>&) const override;

    std::shared_ptr<Key> _key;
    std::shared_ptr<DataStormContract::SubscriberSessionPrx> _subscribers;
};

class FilteredDataReaderI : public DataReaderI,
                            public FilteredDataElement,
                            public std::enable_shared_from_this<FilteredDataReaderI>
{
public:

    FilteredDataReaderI(TopicReaderI*, const std::string&);

    virtual void destroy() override;

    virtual void waitForWriters(int) override;
    virtual bool hasWriters() override;

private:

    const std::string _filter;
};

class FilteredDataWriterI : public DataWriterI,
                            public FilteredDataElement,
                            public std::enable_shared_from_this<FilteredDataWriterI>
{
public:

    FilteredDataWriterI(TopicWriterI*, const std::string&);

    virtual void destroy() override;

    virtual void waitForReaders(int) const override;
    virtual bool hasReaders() const override;

private:

    virtual void send(const std::shared_ptr<DataStormContract::DataSample>&) const override;

    const std::string _filter;
    std::shared_ptr<DataStormContract::SubscriberSessionPrx> _subscribers;
};

}