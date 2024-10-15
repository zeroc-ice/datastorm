//
// Copyright (c) ZeroC, Inc. All rights reserved.
//

#ifndef DATASTORM_INTERNALI_H
#define DATASTORM_INTERNALI_H

#include "DataStorm/Sample.h"
#include "Ice/Ice.h"
#include "Types.h"

#include <chrono>
#include <string>
#include <vector>

//
// Private abstract API used by the template based API and the internal DataStorm implementation.
//
namespace DataStormI
{

    class Instance;

    class Filterable
    {
    public:
        virtual ~Filterable() = default;
    };

    class Element
    {
    public:
        virtual ~Element() = default;
        virtual std::string toString() const = 0;
        virtual Ice::ByteSeq encode(const Ice::CommunicatorPtr&) const = 0;
        virtual std::int64_t getId() const = 0;
    };

    class Key : public Filterable, virtual public Element
    {
    };

    class KeyFactory
    {
    public:
        virtual ~KeyFactory() = default;
        virtual std::shared_ptr<Key> get(std::int64_t) const = 0;
        virtual std::shared_ptr<Key> decode(const Ice::CommunicatorPtr&, const Ice::ByteSeq&) = 0;
    };

    class Tag : virtual public Element
    {
    };

    class TagFactory
    {
    public:
        virtual ~TagFactory() = default;
        virtual std::shared_ptr<Tag> get(std::int64_t) const = 0;
        virtual std::shared_ptr<Tag> decode(const Ice::CommunicatorPtr&, const Ice::ByteSeq&) = 0;
    };

    class Sample : public Filterable
    {
    public:
        Sample(
            const std::string& session,
            const std::string& origin,
            std::int64_t id,
            DataStorm::SampleEvent event,
            const std::shared_ptr<Key>& key,
            const std::shared_ptr<Tag>& tag,
            Ice::ByteSeq value,
            std::int64_t timestamp)
            : session(session),
              origin(origin),
              id(id),
              event(event),
              key(key),
              tag(tag),
              timestamp(std::chrono::microseconds(timestamp)),
              _encodedValue(std::move(value))
        {
        }

        Sample(DataStorm::SampleEvent event, const std::shared_ptr<Tag>& tag = nullptr) : event(event), tag(tag) {}

        virtual bool hasValue() const = 0;
        virtual void setValue(const std::shared_ptr<Sample>&) = 0;

        virtual void decode(const Ice::CommunicatorPtr&) = 0;
        virtual const Ice::ByteSeq& encode(const Ice::CommunicatorPtr&) = 0;
        virtual Ice::ByteSeq encodeValue(const Ice::CommunicatorPtr&) = 0;

        const Ice::ByteSeq& getEncodedValue() const { return _encodedValue; }

        std::string session;
        std::string origin;
        std::int64_t id;
        DataStorm::SampleEvent event;
        std::shared_ptr<Key> key;
        std::shared_ptr<Tag> tag;
        std::chrono::time_point<std::chrono::system_clock> timestamp;

    protected:
        Ice::ByteSeq _encodedValue;
    };

    class SampleFactory
    {
    public:
        virtual ~SampleFactory() = default;

        virtual std::shared_ptr<Sample> create(
            const std::string&,
            const std::string&,
            std::int64_t,
            DataStorm::SampleEvent,
            const std::shared_ptr<Key>&,
            const std::shared_ptr<Tag>&,
            Ice::ByteSeq,
            std::int64_t) = 0;
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
        virtual ~FilterFactory() = default;

        virtual std::shared_ptr<Filter> get(std::int64_t) const = 0;
        virtual std::shared_ptr<Filter> decode(const Ice::CommunicatorPtr&, const Ice::ByteSeq&) = 0;
    };

    class FilterManager
    {
    public:
        virtual ~FilterManager() = default;

        virtual std::shared_ptr<Filter> get(const std::string&, std::int64_t) const = 0;

        virtual std::shared_ptr<Filter>
        decode(const Ice::CommunicatorPtr&, const std::string&, const Ice::ByteSeq&) = 0;
    };

    class DataElement
    {
    public:
        virtual ~DataElement() = default;

        using Id = std::tuple<std::string, std::int64_t, std::int64_t>;

        virtual std::vector<std::string> getConnectedElements() const = 0;
        virtual std::vector<std::shared_ptr<Key>> getConnectedKeys() const = 0;
        virtual void onConnectedKeys(
            std::function<void(std::vector<std::shared_ptr<Key>>)>,
            std::function<void(DataStorm::CallbackReason, std::shared_ptr<Key>)>) = 0;
        virtual void onConnectedElements(
            std::function<void(std::vector<std::string>)>,
            std::function<void(DataStorm::CallbackReason, std::string)>) = 0;

        virtual void destroy() = 0;
        virtual Ice::CommunicatorPtr getCommunicator() const = 0;
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

        virtual void onSamples(
            std::function<void(const std::vector<std::shared_ptr<Sample>>&)>,
            std::function<void(const std::shared_ptr<Sample>&)>) = 0;
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
        virtual ~Topic() = default;

        using Updater = std::function<
            void(const std::shared_ptr<Sample>&, const std::shared_ptr<Sample>&, const Ice::CommunicatorPtr&)>;

        virtual void setUpdater(const std::shared_ptr<Tag>&, Updater) = 0;

        virtual void setUpdaters(std::map<std::shared_ptr<Tag>, Updater>) = 0;
        virtual std::map<std::shared_ptr<Tag>, Updater> getUpdaters() const = 0;

        virtual std::string getName() const = 0;
        virtual void destroy() = 0;
    };

    class TopicReader : virtual public Topic
    {
    public:
        virtual std::shared_ptr<DataReader> createFiltered(
            const std::shared_ptr<Filter>&,
            const std::string&,
            DataStorm::ReaderConfig,
            const std::string& = std::string(),
            Ice::ByteSeq = {}) = 0;

        virtual std::shared_ptr<DataReader> create(
            const std::vector<std::shared_ptr<Key>>&,
            const std::string&,
            DataStorm::ReaderConfig,
            const std::string& = std::string(),
            Ice::ByteSeq = {}) = 0;

        virtual void setDefaultConfig(DataStorm::ReaderConfig) = 0;
        virtual bool hasWriters() const = 0;
        virtual void waitForWriters(int) const = 0;
    };

    class TopicWriter : virtual public Topic
    {
    public:
        virtual std::shared_ptr<DataWriter>
        create(const std::vector<std::shared_ptr<Key>>&, const std::string&, DataStorm::WriterConfig) = 0;

        virtual void setDefaultConfig(DataStorm::WriterConfig) = 0;
        virtual bool hasReaders() const = 0;
        virtual void waitForReaders(int) const = 0;
    };

    class TopicFactory
    {
    public:
        virtual ~TopicFactory() = default;

        virtual std::shared_ptr<TopicReader> createTopicReader(
            const std::string&,
            const std::shared_ptr<KeyFactory>&,
            const std::shared_ptr<TagFactory>&,
            const std::shared_ptr<SampleFactory>&,
            const std::shared_ptr<FilterManager>&,
            const std::shared_ptr<FilterManager>&) = 0;

        virtual std::shared_ptr<TopicWriter> createTopicWriter(
            const std::string&,
            const std::shared_ptr<KeyFactory>&,
            const std::shared_ptr<TagFactory>&,
            const std::shared_ptr<SampleFactory>&,
            const std::shared_ptr<FilterManager>&,
            const std::shared_ptr<FilterManager>&) = 0;

        virtual Ice::CommunicatorPtr getCommunicator() const = 0;
    };

}
#endif
