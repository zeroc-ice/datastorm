// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>

#include <Ice/Communicator.h>
#include <Ice/Initialize.h>
#include <Ice/InputStream.h>
#include <Ice/OutputStream.h>

#include <regex>

#ifndef DATASTORM_API
#   ifdef DATASTORM_API_EXPORTS
#       define DATASTORM_API DATASTORM_DECLSPEC_EXPORT
#   elif defined(DATASTORM_STATIC_LIBS)
#       define DATASTORM_API /**/
#   else
#       define DATASTORM_API DATASTORM_DECLSPEC_IMPORT
#   endif
#endif

namespace DataStorm
{

enum class SampleType
{
    Add,
    Update,
    Remove
};

template<typename T> struct Encoder
{
    static std::string
    toString(const T& value)
    {
        std::ostringstream os;
        os << value;
        return os.str();
    }

    static std::vector<unsigned char>
    marshal(std::shared_ptr<Ice::Communicator> communicator, const T& value)
    {
        std::vector<unsigned char> v;
        Ice::OutputStream stream(communicator);
        stream.write(value);
        stream.finished(v);
        return v;
    }

    static T
    unmarshal(std::shared_ptr<Ice::Communicator> communicator, const std::vector<unsigned char>& value)
    {
        T v;
        if(value.empty())
        {
            v = T();
        }
        else
        {
            Ice::InputStream(communicator, value).read(v);
        }
        return v;
    }
};

};

//
// Private abstract API used by public template based API
//

namespace DataStormInternal
{

class Key
{
public:

    virtual bool match(const std::string&) const = 0;
    virtual std::string toString() const = 0;
    virtual std::vector<unsigned char> marshal() const = 0;
};

class KeyFactory
{
public:

    virtual std::shared_ptr<Key> unmarshal(const std::vector<unsigned char>&) = 0;
};

template<typename T> class KeyT : public Key
{
public:

    KeyT(const T& key, const std::shared_ptr<Ice::Communicator>& communicator) :
        _key(key), _communicator(communicator)
    {
    }

    KeyT(T&& key, const std::shared_ptr<Ice::Communicator>& communicator) :
        _key(std::move(key)), _communicator(communicator)
    {
    }

    virtual bool
    match(const std::string& filter) const override
    {
        return std::regex_match(DataStorm::Encoder<T>::toString(_key), std::regex(filter));
    }

    virtual std::string
    toString() const override
    {
        return DataStorm::Encoder<T>::toString(_key);
    }

    virtual std::vector<unsigned char>
    marshal() const override
    {
        return DataStorm::Encoder<T>::marshal(_communicator, _key);
    }

    const T&
    getKey() const
    {
        return _key;
    }

    std::shared_ptr<Ice::Communicator>
    getCommunicator() const
    {
        return _communicator;
    }

private:

    T _key;
    const std::shared_ptr<Ice::Communicator> _communicator;
};

template<typename T> class KeyFactoryT : public KeyFactory, public std::enable_shared_from_this<KeyFactoryT<T>>
{
    struct Deleter
    {
        void operator()(KeyT<T>* obj)
        {
            auto factory = _factory.lock();
            if(factory)
            {
                factory->remove(obj);
            }
        }

        std::weak_ptr<KeyFactoryT<T>> _factory;

    } _deleter;

public:

    KeyFactoryT(const std::shared_ptr<Ice::Communicator>& communicator) : _communicator(communicator)
    {
    }

    void
    init()
    {
        _deleter = { std::enable_shared_from_this<KeyFactoryT<T>>::shared_from_this() };
    }

    template<typename K> std::shared_ptr<Key>
    create(K&& key)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto p = _keys.find(key);
        if(p != _keys.end())
        {
            auto k = p->second.lock();
            if(k)
            {
                return k;
            }
        }

        auto k = std::shared_ptr<KeyT<T>>(new KeyT<T>(std::forward<K>(key), _communicator), _deleter);
        _keys[k->getKey()] = k;
        return k;
    }

    virtual std::shared_ptr<Key>
    unmarshal(const std::vector<unsigned char>& data) override
    {
        return create(DataStorm::Encoder<T>::unmarshal(_communicator, data));
    }

private:

    friend struct Deleter;

    void remove(KeyT<T>* obj)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto p = _keys.find(obj->getKey());
        if(p != _keys.end())
        {
            _keys.erase(p);
        }
    }

    std::mutex _mutex;
    std::shared_ptr<Ice::Communicator> _communicator;
    std::map<T, std::weak_ptr<Key>> _keys;
};

using Value = std::vector<unsigned char>;

class Sample
{
public:

    DataStorm::SampleType type;

    /** The key */
    std::shared_ptr<Key> key;

    /** The marshaled value */
    Value value;

    /** The timestamp */
    IceUtil::Time timestamp;
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

    virtual int getDisableCount() const = 0;
    virtual int getInstanceCount() const = 0;

    virtual std::vector<std::shared_ptr<Sample>> getAll() const = 0;

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

    virtual void add(Value) = 0;
    virtual void update(Value) = 0;
    virtual void remove() = 0;
};

class Topic
{
public:

    virtual std::string getName() const = 0;
    virtual Ice::CommunicatorPtr getCommunicator() const = 0;
    virtual void destroy() = 0;
};

class TopicReader : virtual public Topic
{
public:

    virtual std::shared_ptr<DataReader> getFilteredDataReader(const std::string&) = 0;
    virtual std::shared_ptr<DataReader> getDataReader(const std::shared_ptr<Key>&) = 0;
};

class TopicWriter : virtual public Topic
{
public:

    virtual std::shared_ptr<DataWriter> getFilteredDataWriter(const std::string&) = 0;
    virtual std::shared_ptr<DataWriter> getDataWriter(const std::shared_ptr<Key>&) = 0;
};

class TopicFactory
{
public:

    virtual std::shared_ptr<TopicReader> createTopicReader(const std::string&, std::shared_ptr<KeyFactory>) = 0;
    virtual std::shared_ptr<TopicWriter> createTopicWriter(const std::string&, std::shared_ptr<KeyFactory>) = 0;

    virtual void waitForShutdown() = 0;
    virtual void shutdown() = 0;
    virtual void destroy() = 0;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const = 0;
};
DATASTORM_API std::shared_ptr<TopicFactory> createTopicFactory(std::shared_ptr<Ice::Communicator>);

};


//
// Public templated based API
//

namespace DataStorm
{


template<typename T> struct DataElementTraits
{
};

template<typename K, typename V> struct DataElementTraits<std::pair<K, V>>
{
    using Key = K;
    using Value = V;
};

template<typename T> class DataReader;
template<typename T> class TopicReader;

template<typename T> class Sample
{
public:

    using Key = typename DataElementTraits<T>::Key;
    using Value = typename DataElementTraits<T>::Value;

    SampleType getType() const
    {
        return _impl->type;
    }

    const Key& getKey() const
    {
        return std::static_pointer_cast<DataStormInternal::KeyT<Key>>(_impl->key)->getKey();
    }

    Value getValue() const
    {
        auto communicator = std::static_pointer_cast<DataStormInternal::KeyT<Key>>(_impl->key)->getCommunicator();
        return Encoder<Value>::unmarshal(communicator, _impl->value);
    }

    const IceUtil::Time
    getTimestamp() const
    {
        return _impl->timestamp;
    }

private:

    Sample(std::shared_ptr<DataStormInternal::Sample> impl) : _impl(impl)
    {
    }

    std::shared_ptr<DataStormInternal::Sample> _impl;

    friend class TopicReader<T>;
    friend class DataReader<T>;
};

template<typename T> class DataElement
{
public:

    DataElement(std::shared_ptr<DataStormInternal::DataElement> impl) : _impl(impl)
    {
    }

    void destroy()
    {
        _impl->destroy();
    }

private:

    std::shared_ptr<DataStormInternal::DataElement> _impl;
};

template<typename T> class DataReader : public DataElement<T>
{
public:

    DataReader(std::shared_ptr<DataStormInternal::DataReader> impl) : DataElement<T>(impl), _impl(impl)
    {
    }

    void waitForWriters(unsigned int count)
    {
        _impl->waitForWriters(count);
    }

    void waitForNoWriters()
    {
        _impl->waitForWriters(-1);
    }

    bool hasWriters()
    {
        return _impl->hasWriters();
    }

    int getDisableCount() const
    {
        return _impl->getDisableCount();
    }

    int getInstanceCount() const
    {
        return _impl->getInstanceCount();
    }

    std::vector<Sample<T>> getAll() const
    {
        auto all = _impl->getAll();
        std::vector<std::unique_ptr<Sample<T>>> samples;
        samples.reserve(all.size());
        for(const auto& sample : all)
        {
            samples.emplace_back(sample);
        }
        return samples;
    }

    std::vector<Sample<T>> getAllUnread()
    {
        auto unread = _impl->getAllUnread();
        std::vector<std::unique_ptr<Sample<T>>> samples;
        samples.reserve(unread.size());
        for(auto sample : unread)
        {
            samples.emplace_back(sample);
        }
        return samples;
    }

    void waitForUnread(unsigned int count) const
    {
        _impl->waitForUnread(count);
    }

    bool hasUnread() const
    {
        return _impl->hasUnread();
    }

    Sample<T> getNextUnread()
    {
        return Sample<T>(_impl->getNextUnread());
    }

private:

    std::shared_ptr<DataStormInternal::DataReader> _impl;
};

template<typename T> class DataWriter : public DataElement<T>
{
    using Value = typename DataElementTraits<T>::Value;

public:

    DataWriter(std::shared_ptr<DataStormInternal::DataWriter> impl) : DataElement<T>(impl), _impl(impl)
    {
    }

    bool hasReaders() const
    {
        return _impl->hasReaders();
    }

    void waitForReaders(unsigned int count) const
    {
        return _impl->waitForReaders(count);
    }

    void waitForNoReaders() const
    {
        return _impl->waitForReaders(-1);
    }

    void add(const Value& v)
    {
        _impl->add(Encoder<Value>::marshal(_impl->getCommunicator(), v));
    }

    void update(const Value& v)
    {
        _impl->update(Encoder<Value>::marshal(_impl->getCommunicator(), v));
    }

    void remove()
    {
        _impl->remove();
    }

private:

    std::shared_ptr<DataStormInternal::DataWriter> _impl;
};

template<typename T> class Topic
{
public:

    Topic(std::shared_ptr<DataStormInternal::Topic> impl) : _impl(impl)
    {
    }

    void destroy()
    {
        _impl->destroy();
    }

private:

    std::shared_ptr<DataStormInternal::Topic> _impl;
};

template<typename T> class TopicReader : public Topic<T>
{
    using Key = typename DataElementTraits<T>::Key;

public:

    TopicReader(std::shared_ptr<DataStormInternal::TopicReader> impl,
                std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> keyFactory) :
        Topic<T>(impl), _impl(impl), _keyFactory(keyFactory)
    {
    }

    std::unique_ptr<DataReader<T>> getFilteredDataReader(const std::string& filter)
    {
        return std::unique_ptr<DataReader<T>>(new DataReader<T>(_impl->getFilteredDataReader(filter)));
    }

    template<typename K>
    std::unique_ptr<DataReader<T>> getDataReader(K&& key)
    {
        return std::unique_ptr<DataReader<T>>(new DataReader<T>(_impl->getDataReader(_keyFactory->create(key))));
    }

private:

    std::shared_ptr<DataStormInternal::TopicReader> _impl;
    std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> _keyFactory;
};

template<typename T> class TopicWriter : public Topic<T>
{
    using Key = typename DataElementTraits<T>::Key;

public:

    TopicWriter(std::shared_ptr<DataStormInternal::TopicWriter> impl,
                std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> keyFactory) :
        Topic<T>(impl), _impl(impl), _keyFactory(keyFactory)
    {
    }

    std::unique_ptr<DataWriter<T>> getFilteredDataWriter(const std::string& filter)
    {
        return std::unique_ptr<DataWriter<T>>(new DataWriter<T>(_impl->getFilteredDataWriter(filter)));
    }

    template<typename K>
    std::unique_ptr<DataWriter<T>> getDataWriter(K&& key)
    {
        return std::unique_ptr<DataWriter<T>>(new DataWriter<T>(_impl->getDataWriter(_keyFactory->create(key))));
    }

private:

    std::shared_ptr<DataStormInternal::TopicWriter> _impl;
    std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> _keyFactory;
};

class TopicFactory
{
public:

    TopicFactory(std::shared_ptr<DataStormInternal::TopicFactory> impl) : _impl(impl)
    {
    }

    template<typename K, typename V>
    std::unique_ptr<TopicReader<std::pair<K, V>>> createTopicReader(const std::string& topic)
    {
        auto keyFactory = std::make_shared<DataStormInternal::KeyFactoryT<K>>(_impl->getCommunicator());
        keyFactory->init();
        auto reader = _impl->createTopicReader(topic, keyFactory);
        return std::unique_ptr<TopicReader<std::pair<K, V>>>(new TopicReader<std::pair<K, V>>(reader, keyFactory));
    }

    template<typename K, typename V>
    std::unique_ptr<TopicWriter<std::pair<K, V>>> createTopicWriter(const std::string& topic)
    {
        auto keyFactory = std::make_shared<DataStormInternal::KeyFactoryT<K>>(_impl->getCommunicator());
        keyFactory->init();
        auto writer = _impl->createTopicWriter(topic, keyFactory);
        return std::unique_ptr<TopicWriter<std::pair<K, V>>>(new TopicWriter<std::pair<K, V>>(writer, keyFactory));
    }

    void waitForShutdown()
    {
        _impl->waitForShutdown();
    }

    void shutdown()
    {
        _impl->shutdown();
    }

    void destroy()
    {
        _impl->destroy();
    }

private:

    std::shared_ptr<DataStormInternal::TopicFactory> _impl;
};

inline std::unique_ptr<TopicFactory>
createTopicFactory(std::shared_ptr<Ice::Communicator> communicator = Ice::CommunicatorPtr())
{
    return std::unique_ptr<TopicFactory>(new TopicFactory(DataStormInternal::createTopicFactory(communicator)));
}

inline std::unique_ptr<TopicFactory>
createTopicFactory(int& argc, char* argv[])
{
    auto communicator = Ice::initialize(argc, argv);
    auto args = Ice::argsToStringSeq(argc, argv);
    communicator->getProperties()->parseCommandLineOptions("DataStorm", args);
    Ice::stringSeqToArgs(args, argc, argv);
    return std::unique_ptr<TopicFactory>(new TopicFactory(DataStormInternal::createTopicFactory(communicator)));
}

}