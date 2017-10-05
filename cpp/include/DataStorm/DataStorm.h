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
template<typename F, typename T> class FilterFactoryT;

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
    static std::vector<unsigned char> marshal(const std::shared_ptr<TopicFactory>&, const T&);

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
    static T unmarshal(const std::shared_ptr<TopicFactory>&, const std::vector<unsigned char>&);

    /**
     * Transforms the given value to a string. Specialization can define this to provide
     * a custom stringification implementation. The default implementation uses
     * `std::ostream::operator<<` to transform the value to a string. This option is
     * mandatory for key types.
     *
     * @param value The value to stringify
     * @return The string representation of the value
     */
    static std::string toString(const T&);
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
    using Key = std::string;

    /** The Value type. Value must be defined to the type of the value for the data element. */
    using Value = T;

    /** The Filter type. */
    using Filter = std::string;

    /**
     * Returns wether or not the key matches the given filter. Specialization
     * can define this method to provide a custom filter implementation. The
     * default implementation uses `std::regex_match` to match the stringified
     * key against the given filter.
     *
     * @param filter The filter.
     * @param key The key to match against the filter.
     * @return True is the key matches the filter, false otherwise.
     */
    static bool match(const std::string& filter, const Key& key)
    {
        return std::regex_match(DataStorm::Encoder<Key>::toString(key), std::regex(filter));
    }

    /**
     * Returns wether or not the sample matches the given filter. Specialization
     * can define this method to provide a custom filter implementation. The
     * default implementation always returns false.
     *
     * @param filter The filter value.
     * @param sample The sample to match against the filter.
     * @return True is the sample matches the filter, false otherwise.
     */
    static bool match(const std::string& filter, const Sample<T>& sample)
    {
        return false;
    }
};

/**
 * Data element traits specialization for `std::pair<K, V>`.
 */
template<typename K, typename V> struct DataElementTraits<std::pair<K, V>>
{
    using Key = K;
    using Value = V;
    using Filter = std::string;

    static bool match(const std::string& filter, const Key& key)
    {
        return std::regex_match(DataStorm::Encoder<Key>::toString(key), std::regex(filter));
    }

    static bool match(const std::string& filter, const Sample<std::pair<K, V>>& sample)
    {
        return false;
    }
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
    Key getKey() const;

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


    /** @private */
    Sample(const std::shared_ptr<DataStormInternal::Sample>&);

private:

    const std::shared_ptr<DataStormInternal::Sample> _impl;
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
    DataElement(const std::shared_ptr<DataStormInternal::DataElement>&);

private:

    const std::shared_ptr<DataStormInternal::DataElement> _impl;
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

    DataReader(const std::shared_ptr<DataStormInternal::DataReader>&);
    friend class TopicReader<T>;

    const std::shared_ptr<DataStormInternal::DataReader> _impl;
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

    DataWriter(const std::shared_ptr<DataStormInternal::DataWriter>&);
    friend class TopicWriter<T>;

    const std::shared_ptr<DataStormInternal::DataWriter> _impl;
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
    using Key = typename DataElementTraits<T>::Key;
    using Filter = typename DataElementTraits<T>::Filter;

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
    Topic(const std::shared_ptr<DataStormInternal::Topic>&,
          const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>>&,
          const std::shared_ptr<DataStormInternal::FilterFactoryT<Filter, T>>&);

    std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> _keyFactory;
    std::shared_ptr<DataStormInternal::FilterFactoryT<Filter, T>> _filterFactory;

private:

    const std::shared_ptr<DataStormInternal::Topic> _impl;
};

/**
 * The TopicReader class is used to obtain DataReader objects.
 */
template<typename T> class TopicReader : public Topic<T>
{
public:

    using Key = typename DataElementTraits<T>::Key;
    using Filter = typename DataElementTraits<T>::Filter;

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
    std::shared_ptr<DataReader<T>> getFilteredDataReader(Filter);

private:

    TopicReader(const std::shared_ptr<DataStormInternal::TopicReader>&,
                const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>>&,
                const std::shared_ptr<DataStormInternal::FilterFactoryT<Filter, T>>&);

    friend class TopicFactory;

    const std::shared_ptr<DataStormInternal::TopicReader> _impl;
};

/**
 * The TopicWriter class is used to obtain DataWriter objects.
 */
template<typename T> class TopicWriter : public Topic<T>
{
public:

    using Key = typename DataElementTraits<T>::Key;
    using Filter = typename DataElementTraits<T>::Filter;

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
    std::shared_ptr<DataWriter<T>> getFilteredDataWriter(Filter);

private:

    TopicWriter(const std::shared_ptr<DataStormInternal::TopicWriter>&,
                const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>>&,
                const std::shared_ptr<DataStormInternal::FilterFactoryT<Filter, T>>&);

    friend class TopicFactory;

    const std::shared_ptr<DataStormInternal::TopicWriter> _impl;
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

    TopicFactory(const std::shared_ptr<DataStormInternal::TopicFactory>&);
    void init();

    friend std::shared_ptr<TopicFactory> createTopicFactory(const std::shared_ptr<Ice::Communicator>&);
    friend std::shared_ptr<TopicFactory> createTopicFactory(int&, char*[]);

    const std::shared_ptr<DataStormInternal::TopicFactory> _impl;
};

}

//
// Private abstract API used by public template based API
//

namespace DataStormInternal
{

template<typename K, typename V> class AbstractFactoryT : public std::enable_shared_from_this<AbstractFactoryT<K, V>>
{
    struct Deleter
    {
        void operator()(V* obj)
        {
            auto factory = _factory.lock();
            if(factory)
            {
                factory->remove(obj);
            }
        }

        std::weak_ptr<AbstractFactoryT<K, V>> _factory;

    } _deleter;

public:

    AbstractFactoryT() : _nextId(0)
    {
    }

    void
    init()
    {
        _deleter = { std::enable_shared_from_this<AbstractFactoryT<K, V>>::shared_from_this() };
    }

    template<typename T> std::shared_ptr<V>
    create(T&& v)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto p = _elements.find(v);
        if(p != _elements.end())
        {
            auto k = p->second.lock();
            if(k)
            {
                return k;
            }

            //
            // The key is being removed concurrently by the deleter, remove it now
            // to allow the insertion of a new key. The deleter won't remove the
            // new key.
            //
            _elements.erase(p);
        }

        auto k = std::shared_ptr<V>(new V(std::forward<T>(v), _nextId++), _deleter);
        _elements[k->get()] = k;
        return k;
    }

protected:

    friend struct Deleter;

    void remove(V* v)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto p = _elements.find(v->get());
        if(p != _elements.end())
        {
            auto e = p->second.lock();
            if(e && e.get() == v)
            {
                _elements.erase(p);
            }
        }
    }

    std::mutex _mutex;
    std::map<K, std::weak_ptr<V>> _elements;
    long long int _nextId;
};

class Element
{
public:

    virtual std::string toString() const = 0;
    virtual std::vector<unsigned char> marshal(const std::shared_ptr<DataStorm::TopicFactory>&) const = 0;
    virtual long long int getId() const = 0;
};

template<typename T> class AbstractElementT : virtual public Element
{
public:

    AbstractElementT(T&& v, long long int id) : _value(std::move(v)), _id(id)
    {
    }

    virtual std::string
    toString() const override
    {
        std::ostringstream os;
        os << DataStorm::Encoder<T>::toString(_value) << ':' << _id;
        return os.str();
    }

    virtual std::vector<unsigned char>
    marshal(const std::shared_ptr<DataStorm::TopicFactory>& factory) const override
    {
        return DataStorm::Encoder<T>::marshal(factory, _value);
    }

    virtual long long int getId() const override
    {
        return _id;
    }

    const T&
    get() const
    {
        return _value;
    }

protected:

    const T _value;
    const long long int _id;
};

class Key : virtual public Element
{
};

class KeyFactory
{
public:

    virtual std::shared_ptr<Key> unmarshal(const std::shared_ptr<DataStorm::TopicFactory>&,
                                           const std::vector<unsigned char>&) = 0;
};

using KeyCompare = Ice::TargetCompare<std::shared_ptr<Key>, std::less>;

template<typename T> class KeyT : public Key, public AbstractElementT<T>
{
public:

    using AbstractElementT<T>::AbstractElementT;
};

template<typename K> class KeyFactoryT : public KeyFactory, public AbstractFactoryT<K, KeyT<K>>
{
public:

    using AbstractFactoryT<K, KeyT<K>>::AbstractFactoryT;

    virtual std::shared_ptr<Key>
    unmarshal(const std::shared_ptr<DataStorm::TopicFactory>& factory, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<K, KeyT<K>>::create(DataStorm::Encoder<K>::unmarshal(factory, data));
    }
};

class Sample
{
public:

    DataStorm::SampleType type;
    std::shared_ptr<Key> key;
    std::vector<unsigned char> value;
    IceUtil::Time timestamp;
    std::shared_ptr<DataStorm::TopicFactory> factory;
};

class Filter : virtual public Element
{
public:

    virtual bool match(const std::shared_ptr<Key>&) const = 0;
    virtual bool match(const std::shared_ptr<Sample>&) const = 0;
};

class FilterFactory
{
public:

    virtual std::shared_ptr<Filter> unmarshal(const std::shared_ptr<DataStorm::TopicFactory>&,
                                              const std::vector<unsigned char>&) = 0;
};

using FilterCompare = Ice::TargetCompare<std::shared_ptr<Filter>, std::less>;

template<typename T, typename E> class FilterT : public Filter, public AbstractElementT<T>
{
public:

    using AbstractElementT<T>::AbstractElementT;

    virtual bool match(const std::shared_ptr<Key>& key) const override
    {
        return DataStorm::DataElementTraits<E>::match(AbstractElementT<T>::_value,
                          std::static_pointer_cast<KeyT<typename DataStorm::DataElementTraits<E>::Key>>(key)->get());
    }

    virtual bool match(const std::shared_ptr<Sample>& sample) const override
    {
        return DataStorm::DataElementTraits<E>::match(AbstractElementT<T>::_value, DataStorm::Sample<E>(sample));
    }
};

template<typename K, typename E> class FilterFactoryT : public FilterFactory, public AbstractFactoryT<K, FilterT<K, E>>
{
public:

    using AbstractFactoryT<K, FilterT<K, E>>::AbstractFactoryT;

    virtual std::shared_ptr<Filter>
    unmarshal(const std::shared_ptr<DataStorm::TopicFactory>& factory, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<K, FilterT<K, E>>::create(DataStorm::Encoder<K>::unmarshal(factory, data));
    }
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

    virtual void add(const std::vector<unsigned char>&) = 0;
    virtual void update(const std::vector<unsigned char>&) = 0;
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

    virtual std::shared_ptr<DataReader> getFilteredDataReader(const std::shared_ptr<Filter>&) = 0;
    virtual std::shared_ptr<DataReader> getDataReader(const std::shared_ptr<Key>&) = 0;
};

class TopicWriter : virtual public Topic
{
public:

    virtual std::shared_ptr<DataWriter> getFilteredDataWriter(const std::shared_ptr<Filter>&) = 0;
    virtual std::shared_ptr<DataWriter> getDataWriter(const std::shared_ptr<Key>&) = 0;
};

class TopicFactory
{
public:

    virtual void init(const std::weak_ptr<DataStorm::TopicFactory>&) = 0;

    virtual std::shared_ptr<TopicReader> createTopicReader(const std::string&,
                                                           const std::shared_ptr<KeyFactory>&,
                                                           const std::shared_ptr<FilterFactory>&) = 0;

    virtual std::shared_ptr<TopicWriter> createTopicWriter(const std::string&,
                                                           const std::shared_ptr<KeyFactory>&,
                                                           const std::shared_ptr<FilterFactory>&) = 0;

    virtual void waitForShutdown() = 0;
    virtual void shutdown() = 0;
    virtual void destroy() = 0;

    virtual std::shared_ptr<Ice::Communicator> getCommunicator() const = 0;
};
DATASTORM_API std::shared_ptr<TopicFactory> createTopicFactory(const std::shared_ptr<Ice::Communicator>&);

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
Encoder<T>::marshal(const std::shared_ptr<TopicFactory>& factory, const T& value)
{
    std::vector<unsigned char> v;
    Ice::OutputStream stream(factory->getCommunicator());
    stream.write(value);
    stream.finished(v);
    return v;
}

template<typename T> T
Encoder<T>::unmarshal(const std::shared_ptr<TopicFactory>& factory, const std::vector<unsigned char>& value)
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

template<typename T>
class is_streamable
{
    template<typename SS, typename TT>
    static auto test(int) -> decltype( std::declval<SS&>() << std::declval<TT>(), std::true_type() );

    template<typename, typename>
    static auto test(...) -> std::false_type;

public:

    static const bool value = decltype(test<std::ostringstream,T>(0))::value;
};

template<typename T> struct Stringifier
{
    template<typename U=T> static std::string
    toString(const T& value, std::enable_if_t<is_streamable<U>::value, int> = 0)
    {
        std::ostringstream os;
        os << value;
        return os.str();
    }

    template<typename U=T> static std::string
    toString(const T& value, std::enable_if_t<!is_streamable<U>::value, int> = 0)
    {
        std::ostringstream os;
        os << &value;
        return os.str();
    }
};

template<typename T> std::string
Encoder<T>::toString(const T& value)
{
    return Stringifier<T>::toString(value);
}

//
// Sample template implementation
//
template<typename T> SampleType
Sample<T>::getType() const
{
    return _impl->type;
}

template<typename T> typename Sample<T>::Key
Sample<T>::getKey() const
{
    if(_impl->key)
    {
        return std::static_pointer_cast<DataStormInternal::KeyT<Sample<T>::Key>>(_impl->key)->get();
    }
    return Sample<T>::Key();
}

template<typename T> typename Sample<T>::Value
Sample<T>::getValue() const
{
    return Encoder<typename Sample<T>::Value>::unmarshal(_impl->factory, _impl->value);
}

template<typename T> IceUtil::Time
Sample<T>::getTimestamp() const
{
    return _impl->timestamp;
}

template<typename T>
Sample<T>::Sample(const std::shared_ptr<DataStormInternal::Sample>& impl) : _impl(std::move(impl))
{
}

//
// DataElement template implementation
//
template<typename T>
DataElement<T>::DataElement(const std::shared_ptr<DataStormInternal::DataElement>& impl) :
    _impl(impl)
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
DataReader<T>::DataReader(const std::shared_ptr<DataStormInternal::DataReader>& impl) :
    DataElement<T>(impl),
    _impl(impl)
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
DataWriter<T>::DataWriter(const std::shared_ptr<DataStormInternal::DataWriter>& impl) :
    DataElement<T>(impl),
    _impl(impl)
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
Topic<T>::Topic(const std::shared_ptr<DataStormInternal::Topic>& impl,
                const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>>& keyFactory,
                const std::shared_ptr<DataStormInternal::FilterFactoryT<Filter, T>>& filterFactory) :
    _keyFactory(keyFactory),
    _filterFactory(filterFactory),
    _impl(impl)
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
TopicReader<T>::TopicReader(const std::shared_ptr<DataStormInternal::TopicReader>& impl,
                            const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>>& keyFactory,
                            const std::shared_ptr<DataStormInternal::FilterFactoryT<Filter, T>>& filterFactory) :
    Topic<T>(impl, keyFactory, filterFactory),
    _impl(impl)
{
}

template<typename T> std::shared_ptr<DataReader<T>>
TopicReader<T>::getDataReader(Key key)
{
    return std::shared_ptr<DataReader<T>>(new DataReader<T>(_impl->getDataReader(this->_keyFactory->create(std::move(key)))));
}

template<typename T> std::shared_ptr<DataReader<T>>
TopicReader<T>::getFilteredDataReader(Filter filter)
{
    return std::shared_ptr<DataReader<T>>(new DataReader<T>(_impl->getFilteredDataReader(this->_filterFactory->create(std::move(filter)))));
}

//
// TopicWriter template implementation
//
template<typename T>
TopicWriter<T>::TopicWriter(const std::shared_ptr<DataStormInternal::TopicWriter>& impl,
                            const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>>& keyFactory,
                            const std::shared_ptr<DataStormInternal::FilterFactoryT<Filter, T>>& filterFactory) :
    Topic<T>(impl, keyFactory, filterFactory),
    _impl(impl)
{
}

template<typename T> std::shared_ptr<DataWriter<T>>
TopicWriter<T>::getDataWriter(Key key)
{
    return std::shared_ptr<DataWriter<T>>(new DataWriter<T>(_impl->getDataWriter(this->_keyFactory->create(std::move(key)))));
}

template<typename T> std::shared_ptr<DataWriter<T>>
TopicWriter<T>::getFilteredDataWriter(Filter filter)
{
    return std::shared_ptr<DataWriter<T>>(new DataWriter<T>(_impl->getFilteredDataWriter(this->_filterFactory->create(std::move(filter)))));
}

//
// TopicFactory template implementation
//
inline TopicFactory::TopicFactory(const std::shared_ptr<DataStormInternal::TopicFactory>& impl) :
    _impl(impl)
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
    auto kf = std::make_shared<DataStormInternal::KeyFactoryT<Key>>();
    kf->init();

    using Filter = typename DataElementTraits<T>::Filter;
    auto ff = std::make_shared<DataStormInternal::FilterFactoryT<Filter, T>>();
    ff->init();

    return std::shared_ptr<TopicReader<T>>(new TopicReader<T>(_impl->createTopicReader(name, kf, ff), kf, ff));
}

template<typename T> std::shared_ptr<TopicWriter<T>>
TopicFactory::createTopicWriter(const std::string& name)
{
    using Key = typename DataElementTraits<T>::Key;
    auto kf = std::make_shared<DataStormInternal::KeyFactoryT<Key>>();
    kf->init();

    using Filter = typename DataElementTraits<T>::Filter;
    auto ff = std::make_shared<DataStormInternal::FilterFactoryT<Filter, T>>();
    ff->init();

    return std::shared_ptr<TopicWriter<T>>(new TopicWriter<T>(_impl->createTopicWriter(name, kf, ff), kf, ff));
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
createTopicFactory(const std::shared_ptr<Ice::Communicator>& communicator = Ice::CommunicatorPtr())
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
    explicit TopicFactoryHolder(T&&... args) : _factory(std::move(initialize(std::forward<T>(args)...)))
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