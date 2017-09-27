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

/**
 * \mainpage %DataStorm API Reference
 *
 * \section namespaces Namespaces
 *
 * @ref DataStorm — The %DataStorm core library.
 */

namespace DataStormInternal
{

class Sample;
class Topic;
class TopicWriter;
class TopicReader;
class DataElement;
class DataWriter;
class DataReader;
class TopicFactory;

template<typename T> class KeyFactoryT;

};

namespace DataStorm
{

class TopicFactory;

/**
 * The Encoder template provides methods to encode and decode user types.
 *
 * The encoder template can be specialized to provide marshalling and un-marshalling
 * methods for types that don't support being marshalled with Ice. By default, the
 * Ice marshalling is used if no Encoder template specialization is provided for the
 * type.
 */
template<typename T> struct Encoder
{
    /**
     * Marshals the given value. This method marshals the given value and returns the
     * resulting byte sequence. The factory parameter is provided to allow the implementation
     * to retrieve configuration or any other information required by the marhsalling.
     *
     * @see unmarshal
     *
     * @param factory The topic factory
     * @param value The value to marshal
     * @return The resulting byte sequence
     */
    static std::vector<unsigned char> marshal(std::shared_ptr<TopicFactory>, const T&);

    /**
     * Unmarshals a value. This method unmarshals the given byte sequence and returns the
     * resulting value. The factory parameter is provided to allow the implementation
     * to retrieve configuration or any other information required by the un-marshalling.
     *
     * @see marshal
     *
     * @param factory The topic factory
     * @param value The byte sequence to unmarshal
     * @return The resulting value
     */
    static T unmarshal(std::shared_ptr<TopicFactory>, const std::vector<unsigned char>&);
};

/**
 * The Traits template provides information on a given type either used as a Topic key or value.
 *
 * This template should be specialized if the default behavior can't be used. The Traits
 * template is used by DataStorm to figure out the filter type for keys and to implement
 * the filtering.
 *
 * The default template implementation assumes the type `T` can be transformed to a string
 * using the `std::ostream::operator<<` and uses regular expression to implement the match
 * method (the value is first transformed to a string).
 *
 */
template<typename T> struct Traits
{
    /**
     * The filter type. Specialization can set a different type. The filter type
     * must support encoding. If it doesn't support the default encoding, you
     * should provide a specialized Encoder.
     */
    using FilterType = std::string;

    /**
     * Transforms the given value to a string. Specialization can define this
     * method to provide a custom stringification implementation. The default
     * implementation uses `std::ostream::operator<<` to transform the value
     * to string.
     *
     * @param value The value to stringify
     * @return The string representation of the value
     */
    static std::string toString(const T&);

    /**
     * Returns wether or not the value matches the given filter. Specialization
     * can define this method to provide a custom filter implementation. The
     * default implementation uses `std::regex_match` to match the stringified
     * value against the given filter.
     *
     * @param value The value to match against the filter.
     * @param filter The filter value.
     * @return True is the value matches the filter, false otherwise.
     */
    static bool match(const T&, const FilterType&);
};

/**
 * The sample type.
 *
 * The sample type matches the operation used by the DataWriter to update
 * the data element. It also provides information on what to expect from
 * the sample. A sample with the Add or Udpate type always provide a value
 * while a sample with the Remove type doesn't.
 *
 */
enum class SampleType
{
    /** The data element has been added. */
    Add,

    /** The data element has been updated. */
    Update,

    /** The data element has been removed. */
    Remove
};

template<typename T> class DataReader;
template<typename T> class TopicReader;
template<typename T> class DataWriter;
template<typename T> class TopicWriter;
template<typename T> class Sample;

/**
 * The data element traits provides information on the key and value types of a data element.
 */
template<typename T> struct DataElementTraits
{
    /** The Key type. Key must be defined to the type of the key for the data element. */
    using Key = void;
    /** The Value type. Value must be defined to the type of the value for the data element. */
    using Value = void;
};

/**
 * Data element traits specialization for `std::pair<K, V>`.
 */
template<typename K, typename V> struct DataElementTraits<std::pair<K, V>>
{
    using Key = K;
    using Value = V;
};

/**
 * A sample provides information about an update of a data element.
 *
 * The Sample template provides access to key, value and type of
 * an update to a data element.
 *
 */
template<typename T> class Sample
{
    using Key = typename DataElementTraits<T>::Key;
    using Value = typename DataElementTraits<T>::Value;

public:

    /**
     * The type of the sample.
     *
     * @return The sample type.
     */
    SampleType getType() const;

    /**
     * The key of the sample.
     *
     * @return The sample key.
     */
    const Key& getKey() const;

    /**
     * The value of the sample.
     *
     * Depending on the sample type, the sample value might not always be
     * available. It is for instance the case if the sample type is Remove.
     *
     * @return The sample value.
     */
    Value getValue() const;

    /**
     * The timestamp of the sample.
     *
     * The timestamp is generated by the writer. It corresponds to time at
     * which the sample was sent.
     *
     * TODO: use C++11 type.
     *
     * @return The timestamp.
     */
    IceUtil::Time getTimestamp() const;

private:

    Sample(std::shared_ptr<DataStormInternal::Sample>);

    std::shared_ptr<DataStormInternal::Sample> _impl;

    friend class TopicReader<T>;
    friend class DataReader<T>;
};

/**
 * The DataElement base class.
 *
 * A DataElement is obtained from a Topic. It's the base class for
 * DataWriter and DataReader which are respecitively used to write
 * or a read a data element.
 *
 */
template<typename T> class DataElement
{
public:

    /**
     * Destroy the data element.
     *
     * Destroying a data element indicates that the application is
     * no longer interested in updating or receiving updates for
     * the data element.
     */
    void destroy();

protected:

    /** @private */
    DataElement(std::shared_ptr<DataStormInternal::DataElement>);

private:

    std::shared_ptr<DataStormInternal::DataElement> _impl;
};

/**
 * The DataReader class is used to retrieve samples for a data element.
 */
template<typename T> class DataReader : public DataElement<T>
{
public:

    /**
     * Indicates whether or not data writers are online.
     *
     * @return True if data writers are connected, false otherwise.
     */
    bool hasWriters() const;

    /**
     * Wait for given number of writers to be online.
     *
     * @param count The number of writers to wait.
     */
    void waitForWriters(unsigned int = 1) const;

    /**
     * Wait for writers to be offline.
     */
    void waitForNoWriters() const;

    /**
     * Returns the number of times the data element was instantiated.
     *
     * This returns the number of times {@link DataWriter::add} was called for the
     * data element.
     */
    int getInstanceCount() const;

    /**
     * Returns all the data samples available with this reader.
     *
     * @return The data samples.
     */
    std::vector<Sample<T>> getAll() const;

    /**
     * Returns all the unread data samples.
     *
     * @return The unread data samples.
     */
    std::vector<Sample<T>> getAllUnread();

    /**
     * Wait for given number of unread data samples to be available.
     */
    void waitForUnread(unsigned int = 1) const;

    /**
     * Returns wether or not data unread data samples are available.
     */
    bool hasUnread() const;

    /**
     * Returns the next unread data sample.
     *
     * @return The unread data sample.
     */
    Sample<T> getNextUnread();

private:

    DataReader(std::shared_ptr<DataStormInternal::DataReader>);
    friend class TopicReader<T>;

    std::shared_ptr<DataStormInternal::DataReader> _impl;
};

/**
 * The DataWriter class is used to write samples for a data element.
 */
template<typename T> class DataWriter : public DataElement<T>
{
    using Value = typename DataElementTraits<T>::Value;

public:

    /**
     * Indicates whether or not data readers are online.
     *
     * @return True if data readers are connected, false otherwise.
     */
    bool hasReaders() const;

    /**
     * Wait for given number of readers to be online.
     *
     * @param count The number of readers to wait.
     */
    void waitForReaders(unsigned int = 1) const;

    /**
     * Wait for readers to be offline.
     */
    void waitForNoReaders() const;

    /**
     * Add the data element. This generates an {@link Add} data sample with the
     * given value.
     *
     * @param value The data element value.
     */
    void add(const Value&);

    /**
     * Update the data element. This generates an {@link Update} data sample with the
     * given value.
     *
     * @param value The data element value.
     */
    void update(const Value&);

    /**
     * Remove the data element. This generates a {@link Remove} data sample.
     */
    void remove();

private:

    DataWriter(std::shared_ptr<DataStormInternal::DataWriter>);
    friend class TopicWriter<T>;

    std::shared_ptr<DataStormInternal::DataWriter> _impl;
};

/**
 * The Topic base class.
 *
 * A topic can be viewed as a dictionnary of DataElement objects. The key for
 * the data elements has the type DataElementTraits<T>::Key and the value has
 * the type DataElementTraits<T>::Value.
 */
template<typename T> class Topic
{
public:

    /**
     * Destroy the topic.
     *
     * Destroying a topic indicates that the application is no longer interested in
     * updating or receiving updates for the topic.
     */
    void destroy();

protected:

    /** @private */
    Topic(std::shared_ptr<DataStormInternal::Topic>);

private:

    std::shared_ptr<DataStormInternal::Topic> _impl;
};

/**
 * The TopicReader class is used to obtain DataReader objects.
 */
template<typename T> class TopicReader : public Topic<T>
{
public:

    using Key = typename DataElementTraits<T>::Key;

    /**
     * Get a DataReader object to read the topic data element matching
     * the given key.
     *
     * @param key The key to lookup the data element.
     */
    std::shared_ptr<DataReader<T>> getDataReader(Key);

    /**
     * Get a DataReader object to read the topic data elements matching
     * whose keys match the given filter.
     *
     * @param filter The filter used to find data elements with matching keys.
     */
    std::shared_ptr<DataReader<T>> getFilteredDataReader(typename Traits<Key>::FilterType);

private:

    TopicReader(std::shared_ptr<DataStormInternal::TopicReader>, std::shared_ptr<DataStormInternal::KeyFactoryT<Key>>);
    friend class TopicFactory;

    std::shared_ptr<DataStormInternal::TopicReader> _impl;
    std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> _keyFactory;
};

/**
 * The TopicWriter class is used to obtain DataWriter objects.
 */
template<typename T> class TopicWriter : public Topic<T>
{
public:

    using Key = typename DataElementTraits<T>::Key;

    /**
     * Get a DataReader object to read the topic data element matching
     * the given key.
     *
     * @param key The key to lookup the data element.
     */
    std::shared_ptr<DataWriter<T>> getDataWriter(Key);

    /**
     * Get a DataReader object to read the topic data elements matching
     * whose keys match the given filter.
     *
     * @param filter The filter used to find data elements with matching keys.
     */
    std::shared_ptr<DataWriter<T>> getFilteredDataWriter(typename Traits<Key>::FilterType);

private:

    TopicWriter(std::shared_ptr<DataStormInternal::TopicWriter>, std::shared_ptr<DataStormInternal::KeyFactoryT<Key>>);
    friend class TopicFactory;

    std::shared_ptr<DataStormInternal::TopicWriter> _impl;
    std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> _keyFactory;
};

/**
 * The TopicFactory class allows creating topic readers and writers.
 *
 * A TopicFactory instance is the main DataStorm object which allows creating
 * topic readers or writers.
 */
class TopicFactory : public std::enable_shared_from_this<TopicFactory>
{
public:

    /**
     * Creates a new topic reader for the topic with the given name.
     *
     * @param name The topic name
     * @return The topic reader
     */
    template<typename T> std::shared_ptr<TopicReader<T>> createTopicReader(const std::string&);

    /**
     * Creates a new topic writer for the topic with the given name.
     *
     * @param name The topic name
     * @return The topic writer
     */
    template<typename T> std::shared_ptr<TopicWriter<T>> createTopicWriter(const std::string&);

    /**
     * Creates a new topic reader for the topic with the given name and for the specified
     * key/value types.
     *
     * @param name The topic name
     * @return The topic reader
     */
    template<typename K, typename V> std::shared_ptr<TopicReader<std::pair<K, V>>> createTopicReader(const std::string&);

    /**
     * Creates a new topic writer for the topic with the given name and for the specified
     * key/value types.
     *
     * @param name The topic name
     * @return The topic writer
     */
    template<typename K, typename V> std::shared_ptr<TopicWriter<std::pair<K, V>>> createTopicWriter(const std::string&);

    /**
     * Wait for the shutdown of the topic factory.
     *
     * This methods returns when {@link shutdown} is called on the topic factory or
     * when the associated Ice communicator is shutdown.
     */
    void waitForShutdown();

    /**
     * Shuts down of the topic factory.
     *
     * This methods shutdowns the associated Ice communicator.
     */
    void shutdown();

    /**
     * Destroy the topic factory.
     *
     * This methods destroys the topic factory and associated Ice communicator.
     * Calling destroy is necessary to ensure all the resources allocated by the
     * factory are released.
     */
    void destroy();

    /**
     * Returns the Ice communicator assocated with the topic factory.
     */
    std::shared_ptr<Ice::Communicator> getCommunicator() const;

private:

    TopicFactory(std::shared_ptr<DataStormInternal::TopicFactory>);
    void init();
    friend std::shared_ptr<TopicFactory> createTopicFactory(std::shared_ptr<Ice::Communicator>);
    friend std::shared_ptr<TopicFactory> createTopicFactory(int&, char*[]);

    std::shared_ptr<DataStormInternal::TopicFactory> _impl;
};

}

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

    KeyT(const T& key, const std::shared_ptr<DataStorm::TopicFactory>& factory) :
        _key(key), _factory(factory)
    {
    }

    KeyT(T&& key, const std::shared_ptr<DataStorm::TopicFactory>& factory) :
        _key(std::move(key)), _factory(factory)
    {
    }

    virtual bool
    match(const std::string& filter) const override
    {
        return DataStorm::Traits<T>::match(_key, filter);
    }

    virtual std::string
    toString() const override
    {
        return DataStorm::Traits<T>::toString(_key);
    }

    virtual std::vector<unsigned char>
    marshal() const override
    {
        return DataStorm::Encoder<T>::marshal(_factory, _key);
    }

    const T&
    getKey() const
    {
        return _key;
    }

    std::shared_ptr<DataStorm::TopicFactory>
    getTopicFactory() const
    {
        return _factory;
    }

private:

    T _key;
    const std::shared_ptr<DataStorm::TopicFactory> _factory;
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

    KeyFactoryT(const std::shared_ptr<DataStorm::TopicFactory>& factory) : _factory(factory)
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

        auto k = std::shared_ptr<KeyT<T>>(new KeyT<T>(std::forward<K>(key), _factory), _deleter);
        _keys[k->getKey()] = k;
        return k;
    }

    virtual std::shared_ptr<Key>
    unmarshal(const std::vector<unsigned char>& data) override
    {
        return create(DataStorm::Encoder<T>::unmarshal(_factory, data));
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
    std::shared_ptr<DataStorm::TopicFactory> _factory;
    std::map<T, std::weak_ptr<Key>> _keys;
};

class Sample
{
public:

    DataStorm::SampleType type;
    std::shared_ptr<Key> key;
    std::vector<unsigned char> value;
    IceUtil::Time timestamp;
};

class DataElement
{
public:

    virtual void destroy() = 0;
    virtual std::shared_ptr<DataStorm::TopicFactory> getTopicFactory() const = 0;
};

class DataReader : virtual public DataElement
{
public:

    virtual bool hasWriters() = 0;
    virtual void waitForWriters(int) = 0;
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

    virtual void add(std::vector<unsigned char>) = 0;
    virtual void update(std::vector<unsigned char>) = 0;
    virtual void remove() = 0;
};

class Topic
{
public:

    virtual std::string getName() const = 0;
    virtual std::shared_ptr<DataStorm::TopicFactory> getTopicFactory() const = 0;
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

    virtual void init(std::weak_ptr<DataStorm::TopicFactory>) = 0;

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
// Public template based API implementation
//

namespace DataStorm
{

//
// Encoder template implementation
//
template<typename T> std::vector<unsigned char>
Encoder<T>::marshal(std::shared_ptr<TopicFactory> factory, const T& value)
{
    std::vector<unsigned char> v;
    Ice::OutputStream stream(factory->getCommunicator());
    stream.write(value);
    stream.finished(v);
    return v;
}

template<typename T> T
Encoder<T>::unmarshal(std::shared_ptr<TopicFactory> factory, const std::vector<unsigned char>& value)
{
    T v;
    if(value.empty())
    {
        v = T();
    }
    else
    {
        Ice::InputStream(factory->getCommunicator(), value).read(v);
    }
    return v;
}

//
// Traits template implementation
//
template<typename T> std::string
Traits<T>::toString(const T& value)
{
    std::ostringstream os;
    os << value;
    return os.str();
}

template<typename T> bool
Traits<T>::match(const T& value, const FilterType& filter)
{
    return std::regex_match(toString(value), std::regex(filter));
}

//
// Sample template implementation
//
template<typename T> SampleType
Sample<T>::getType() const
{
    return _impl->type;
}

template<typename T> const typename Sample<T>::Key&
Sample<T>::getKey() const
{
    return std::static_pointer_cast<DataStormInternal::KeyT<Sample<T>::Key>>(_impl->key)->getKey();
}

template<typename T> typename Sample<T>::Value
Sample<T>::getValue() const
{
    auto factory = std::static_pointer_cast<DataStormInternal::KeyT<Key>>(_impl->key)->getTopicFactory();
    return Encoder<typename Sample<T>::Value>::unmarshal(factory, _impl->value);
}

template<typename T> IceUtil::Time
Sample<T>::getTimestamp() const
{
    return _impl->timestamp;
}

template<typename T>
Sample<T>::Sample(std::shared_ptr<DataStormInternal::Sample> impl) : _impl(std::move(impl))
{
}

//
// DataElement template implementation
//
template<typename T>
DataElement<T>::DataElement(std::shared_ptr<DataStormInternal::DataElement> impl) : _impl(std::move(impl))
{
}

template<typename T> void
DataElement<T>::destroy()
{
    _impl->destroy();
}

//
// DataReader template implementation
//
template<typename T>
DataReader<T>::DataReader(std::shared_ptr<DataStormInternal::DataReader> impl) : DataElement<T>(impl), _impl(std::move(impl))
{
}

template<typename T> bool
DataReader<T>::hasWriters() const
{
    return _impl->hasWriters();
}

template<typename T> void
DataReader<T>::waitForWriters(unsigned int count) const
{
    _impl->waitForWriters(count);
}

template<typename T> void
DataReader<T>::waitForNoWriters() const
{
    _impl->waitForWriters(-1);
}

template<typename T> int
DataReader<T>::getInstanceCount() const
{
    return _impl->getInstanceCount();
}

template<typename T> std::vector<Sample<T>>
DataReader<T>::getAll() const
{
    auto all = _impl->getAll();
    std::vector<Sample<T>> samples;
    samples.reserve(all.size());
    for(const auto& sample : all)
    {
        samples.emplace_back(Sample<T>(sample));
    }
    return samples;
}

template<typename T> std::vector<Sample<T>>
DataReader<T>::getAllUnread()
{
    auto unread = _impl->getAllUnread();
    std::vector<Sample<T>> samples;
    samples.reserve(unread.size());
    for(auto sample : unread)
    {
        samples.emplace_back(Sample<T>(sample));
    }
    return samples;
}

template<typename T> void
DataReader<T>::waitForUnread(unsigned int count) const
{
    _impl->waitForUnread(count);
}

template<typename T> bool
DataReader<T>::hasUnread() const
{
    return _impl->hasUnread();
}

template<typename T> Sample<T>
DataReader<T>::getNextUnread()
{
    return Sample<T>(_impl->getNextUnread());
}

//
// DataWriter template implementation
//
template<typename T>
DataWriter<T>::DataWriter(std::shared_ptr<DataStormInternal::DataWriter> impl) : DataElement<T>(impl), _impl(std::move(impl))
{
}

template<typename T> bool
DataWriter<T>::hasReaders() const
{
    return _impl->hasReaders();
}

template<typename T> void
DataWriter<T>::waitForReaders(unsigned int count) const
{
    return _impl->waitForReaders(count);
}

template<typename T> void
DataWriter<T>::waitForNoReaders() const
{
    return _impl->waitForReaders(-1);
}

template<typename T> void
DataWriter<T>::add(const Value& value)
{
    _impl->add(Encoder<Value>::marshal(_impl->getTopicFactory(), value));
}

template<typename T> void
DataWriter<T>::update(const Value& value)
{
    _impl->update(Encoder<Value>::marshal(_impl->getTopicFactory(), value));
}

template<typename T> void
DataWriter<T>::remove()
{
    _impl->remove();
}

//
// Topic template implementation
//
template<typename T>
Topic<T>::Topic(std::shared_ptr<DataStormInternal::Topic> impl) : _impl(std::move(impl))
{
}

template<typename T> void
Topic<T>::destroy()
{
    _impl->destroy();
}

//
// TopicReader template implementation
//
template<typename T>
TopicReader<T>::TopicReader(std::shared_ptr<DataStormInternal::TopicReader> impl,
                            std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> keyFactory) :
    Topic<T>(impl), _impl(std::move(impl)), _keyFactory(keyFactory)
{
}

template<typename T> std::shared_ptr<DataReader<T>>
TopicReader<T>::getDataReader(Key key)
{
    return std::shared_ptr<DataReader<T>>(new DataReader<T>(_impl->getDataReader(_keyFactory->create(std::move(key)))));
}

template<typename T> std::shared_ptr<DataReader<T>>
TopicReader<T>::getFilteredDataReader(typename Traits<Key>::FilterType filter)
{
    return std::shared_ptr<DataReader<T>>(new DataReader<T>(_impl->getFilteredDataReader(std::move(filter))));
}

//
// TopicWriter template implementation
//
template<typename T>
TopicWriter<T>::TopicWriter(std::shared_ptr<DataStormInternal::TopicWriter> impl,
                            std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> keyFactory) :
    Topic<T>(impl), _impl(std::move(impl)), _keyFactory(keyFactory)
{
}

template<typename T> std::shared_ptr<DataWriter<T>>
TopicWriter<T>::getDataWriter(Key key)
{
    return std::shared_ptr<DataWriter<T>>(new DataWriter<T>(_impl->getDataWriter(_keyFactory->create(std::move(key)))));
}

template<typename T> std::shared_ptr<DataWriter<T>>
TopicWriter<T>::getFilteredDataWriter(typename Traits<Key>::FilterType filter)
{
    return std::shared_ptr<DataWriter<T>>(new DataWriter<T>(_impl->getFilteredDataWriter(std::move(filter))));
}

//
// TopicFactory template implementation
//
inline TopicFactory::TopicFactory(std::shared_ptr<DataStormInternal::TopicFactory> impl) : _impl(std::move(impl))
{
}

template<typename K, typename V> std::shared_ptr<TopicReader<std::pair<K, V>>>
TopicFactory::createTopicReader(const std::string& name)
{
    return createTopicReader<std::pair<K, V>>(name);
}

template<typename K, typename V> std::shared_ptr<TopicWriter<std::pair<K, V>>>
TopicFactory::createTopicWriter(const std::string& name)
{
    return createTopicWriter<std::pair<K, V>>(name);
}

template<typename T> std::shared_ptr<TopicReader<T>>
TopicFactory::createTopicReader(const std::string& name)
{
    using Key = typename DataElementTraits<T>::Key;
    auto f = std::make_shared<DataStormInternal::KeyFactoryT<Key>>(shared_from_this());
    f->init();
    auto reader = _impl->createTopicReader(name, f);
    return std::shared_ptr<TopicReader<T>>(new TopicReader<T>(reader, f));
}

template<typename T> std::shared_ptr<TopicWriter<T>>
TopicFactory::createTopicWriter(const std::string& name)
{
    using Key = typename DataElementTraits<T>::Key;
    auto f = std::make_shared<DataStormInternal::KeyFactoryT<Key>>(shared_from_this());
    f->init();
    auto writer = _impl->createTopicWriter(name, f);
    return std::shared_ptr<TopicWriter<T>>(new TopicWriter<T>(writer, f));
}

inline void
TopicFactory::init()
{
    _impl->init(shared_from_this());
}

inline void
TopicFactory::waitForShutdown()
{
    _impl->waitForShutdown();
}

inline void
TopicFactory::shutdown()
{
    _impl->shutdown();
}

inline void
TopicFactory::destroy()
{
    _impl->destroy();
}

inline std::shared_ptr<Ice::Communicator>
TopicFactory::getCommunicator() const
{
    return _impl->getCommunicator();
}

/**
 * Create a topic factory.
 *
 * This function return a topic factory which eventually uses the given
 * Ice communicator if not null.
 *
 * @param communicator The Ice communicator used by the topic factory for its configuration
 *                     and communications.
 * @return The topic factory.
 */
inline std::shared_ptr<TopicFactory>
createTopicFactory(std::shared_ptr<Ice::Communicator> communicator = Ice::CommunicatorPtr())
{
    auto factory = std::shared_ptr<TopicFactory>(new TopicFactory(DataStormInternal::createTopicFactory(communicator)));
    factory->init();
    return factory;
}

/**
 * Creates a topic factory using configuration from command line arguments.
 *
 * This function parses the command line arguments into Ice properties and
 * initialize a new topic factory.
 *
 * @param argc The number of command line arguments in the argv array.
 * @param argv The command line arguments
 * @return The topic factory.
 */
inline std::shared_ptr<TopicFactory>
createTopicFactory(int& argc, char* argv[])
{
    auto communicator = Ice::initialize(argc, argv);
    auto args = Ice::argsToStringSeq(argc, argv);
    communicator->getProperties()->parseCommandLineOptions("DataStorm", args);
    Ice::stringSeqToArgs(args, argc, argv);
    auto factory = std::shared_ptr<TopicFactory>(new TopicFactory(DataStormInternal::createTopicFactory(communicator)));
    factory->init();
    return factory;
}

/**
 * A holder class to facilitate the creation and destruction of the of
 * the topic factory.
 */
class ICE_API TopicFactoryHolder
{
public:

    //
    // Empty holder
    //
    TopicFactoryHolder();

    //
    // Call initialize to create factory with the provided args (all except default ctor above)
    //
    //
    template<class... T>
    explicit TopicFactoryHolder(T&&... args) :
        _factory(std::move(initialize(std::forward<T>(args)...)))
    {
    }

    //
    // Adopt topic factory
    //
    explicit TopicFactoryHolder(std::shared_ptr<TopicFactory>);
    TopicFactoryHolder& operator=(std::shared_ptr<TopicFactory>);

    TopicFactoryHolder(const TopicFactoryHolder&) = delete;

    TopicFactoryHolder(TopicFactoryHolder&&) = default;
    TopicFactoryHolder& operator=(TopicFactoryHolder&&);

    explicit operator bool() const;

    ~TopicFactoryHolder();

    const std::shared_ptr<TopicFactory>& factory() const;
    const std::shared_ptr<TopicFactory>& operator->() const;
    std::shared_ptr<TopicFactory> release();

private:

    std::shared_ptr<TopicFactory>  _factory;
};

}