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

#include <DataStorm/SampleType.h>
#include <DataStorm/Filter.h>

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

class Node;
template<typename T> class DataReader;
template<typename T> class TopicReader;
template<typename T> class DataWriter;
template<typename T> class TopicWriter;
template<typename T> class Sample;

/**
 * The data traits provides information on the key, value and filter types used by data
 * readers or writers.
 */
template<typename V> struct DataTraits
{
    /** The Key type. Key must be defined to the type of the key for the data element. */
    using KeyType = std::string;

    /** The ValueType type. ValueType must be defined to the type of the value for the data element. */
    using ValueType = V;

    /** The Filter type. */
    using FilterType = std::string;
};

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
    static std::vector<unsigned char> marshal(const std::shared_ptr<Ice::Communicator>&, const T&);

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
    static T unmarshal(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&);

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
 * The filter template provide methods to filter key and samples. The template should
 * be specified for each filter type. Two default implementations are provided, one
 * for filtering keys based on a std::regex regular expression and the other to filter
 * keys and samples based on a key and value regular expression and a sample types.
 **/
template<typename T> struct Filter
{
    /**
     * Returns wether or not the key matches the given filter. Specialization
     * can define this method to provide a custom key filter implementation.
     *
     * @param filter The filter.
     * @param key The key to match against the filter.
     * @return True is the key matches the filter, false otherwise.
     */
    template<typename Key> static bool
    match(const T& filter, const Key& key)
    {
        return true;
    }

    /**
     * Returns wether or not the sample matches the given filter. Specialization
     * can define this method to provide a custom sample filter implementation.
     *
     * @param filter The filter value.
     * @param sample The sample to match against the filter.
     * @return True is the sample matches the filter, false otherwise.
     */
    template<typename V> static bool
    match(const T& filter, const Sample<V>& sample)
    {
        return true;
    }
};

template<> struct Filter<std::string>
{
    template<typename Key> static bool
    match(const std::string& filter, const Key& key)
    {
        return std::regex_match(DataStorm::Encoder<Key>::toString(key), std::regex(filter));
    }

    template<typename V> static bool
    match(const std::string&, const Sample<V>&)
    {
        return true;
    }
};

template<> struct Filter<RegexFilter>
{
    template<typename Key> static bool
    match(const RegexFilter& filter, const Key& key)
    {
        if(!filter.key.empty())
        {
            return std::regex_match(DataStorm::Encoder<Key>::toString(key), std::regex(filter.key));
        }
        else
        {
            return true;
        }
    }

    template<typename V> static bool
    match(const RegexFilter& filter, const Sample<V>& sample)
    {
        if(!filter.types.empty())
        {
            if(std::find(filter.types.begin(), filter.types.end(), sample.getType()) == filter.types.end())
            {
                return false;
            }
        }
        if(!filter.value.empty())
        {
            return std::regex_match(DataStorm::Encoder<V>::toString(sample.getValue()), std::regex(filter.value));
        }
        else
        {
            return true;
        }
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
public:

    using KeyType = typename DataTraits<T>::KeyType;
    using ValueType = typename DataTraits<T>::ValueType;

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
    KeyType getKey() const;

    /**
     * The value of the sample.
     *
     * Depending on the sample type, the sample value might not always be
     * available. It is for instance the case if the sample type is Remove.
     *
     * @return The sample value.
     */
    ValueType getValue() const;

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
 * The DataReader class is used to retrieve samples for a data element.
 */
template<typename T> class DataReader
{
public:

    using ValueType = typename DataTraits<T>::ValueType;

    /**
     * Destruct the data reader. The destruction of the data reader disconnects to the
     * the reader from the writers.
     */
    ~DataReader();

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

protected:

    /** @private */
    DataReader(const std::shared_ptr<DataStormInternal::DataReader>& impl) : _impl(impl)
    {
    }

private:

    const std::shared_ptr<DataStormInternal::DataReader> _impl;
};

/**
 * The key data reader to read the data element associated with a given key.
 */
template<typename T> class KeyDataReader : public DataReader<T>
{
public:

    /**
     * Construct a new data reader for the given key. The construction of the data reader
     * connects the reader to writers with a matching key.
     *
     * @param topic The topic reader.
     * @param key The key of the data element to read.
     */
    KeyDataReader(TopicReader<T>&, typename DataTraits<T>::KeyType);
};

/**
 * The filtered data reader to read data elements whose key match a given filter.
 */
template<typename T> class FilteredDataReader : public DataReader<T>
{
public:

    /**
     * Construct a new data reader for the given filter. The construction of the data reader
     * connects the reader to writers whose key matches the filter.
     *
     * @param topic The topic reader.
     * @param filter The filter.
     */
    FilteredDataReader(TopicReader<T>&, typename DataTraits<T>::FilterType);
};

/**
 * The DataWriter class is used to write samples for a data element.
 */
template<typename T> class DataWriter
{
public:

    using ValueType = typename DataTraits<T>::ValueType;

    /**
     * Destruct the data writer. The destruction of the data writer disconnects the
     * the writer from the readers.
     */
    ~DataWriter();

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
    void add(const ValueType&);

    /**
     * Update the data element. This generates an {@link Update} data sample with the
     * given value.
     *
     * @param value The data element value.
     */
    void update(const ValueType&);

    /**
     * Remove the data element. This generates a {@link Remove} data sample.
     */
    void remove();

protected:

    /** @private */
    DataWriter(const std::shared_ptr<DataStormInternal::DataWriter>& impl) : _impl(impl)
    {
    }

private:

    const std::shared_ptr<DataStormInternal::DataWriter> _impl;
};

/**
 * The key data writer to write the data element associated with a given key.
 */
template<typename T> class KeyDataWriter : public DataWriter<T>
{
public:

    /**
     * Construct a new data writer for the given key. The construction of the data writer
     * connects the writer to writers with a matching key.
     *
     * @param topic The topic writer.
     * @param key The key of the data element to write.
     */
    KeyDataWriter(TopicWriter<T>&, typename DataTraits<T>::KeyType);
};

/**
 * The filtered data writer to write data elements whose key match a given filter.
 */
template<typename T> class FilteredDataWriter : public DataWriter<T>
{
public:

    /**
     * Construct a new data writer for the given filter. The construction of the data writer
     * connects the writer to readers whose key matches the filter.
     *
     * @param topic The topic writer.
     * @param filter The filter.
     */
    FilteredDataWriter(TopicWriter<T>&, typename DataTraits<T>::FilterType);
};

/**
 * The TopicReader class allows to construct DataReader objects.
 */
template<typename T> class TopicReader
{
public:

    using KeyType = typename DataTraits<T>::KeyType;
    using FilterType = typename DataTraits<T>::FilterType;

    /**
     * Construct a new TopicReader for the topic with the given name. This connects
     * the reader to topic writers with a matching name.
     *
     * @param node The DataStorm node
     * @param name The name of the topic
     */
    TopicReader(Node&, const std::string&);

    /**
     * Destruct the new TopicReader. This disconnects the reader from the writers.
     */
    ~TopicReader();

private:

    friend class KeyDataReader<T>;
    friend class FilteredDataReader<T>;

    const std::shared_ptr<DataStormInternal::TopicReader> _impl;
    const std::shared_ptr<DataStormInternal::KeyFactoryT<KeyType>> _keyFactory;
    const std::shared_ptr<DataStormInternal::FilterFactoryT<FilterType, T>> _filterFactory;
};

/**
 * The TopicWriter class allows to construct DataWriter objects.
 */
template<typename T> class TopicWriter
{
public:

    using KeyType = typename DataTraits<T>::KeyType;
    using FilterType = typename DataTraits<T>::FilterType;

    /**
     * Construct a new TopicWriter for the topic with the given name. This connects
     * the writer to topic readers with a matching name.
     *
     * @param node The DataStorm node
     * @param name The name of the topic
     */
    TopicWriter(Node&, const std::string&);

    /**
     * Destruct the new TopicWriter. This disconnects the writer from the readers.
     */
    ~TopicWriter();

private:

    friend class KeyDataWriter<T>;
    friend class FilteredDataWriter<T>;

    const std::shared_ptr<DataStormInternal::TopicWriter> _impl;
    const std::shared_ptr<DataStormInternal::KeyFactoryT<KeyType>> _keyFactory;
    const std::shared_ptr<DataStormInternal::FilterFactoryT<FilterType, T>> _filterFactory;
};

/**
 * The Node class allows creating topic readers and writers.
 *
 * A Node is the main DataStorm object which allows creating topic readers or writers.
 */
class DATASTORM_API Node
{
public:

    /**
     * Construct a DataStorm node.
     *
     * A node is the main DataStorm object. It is required to construct topic readers or writers.
     * The node uses the given Ice communicator if provided.
     *
     * @param communicator The Ice communicator used by the topic factory for its configuration
     *                     and communications.
     */
    Node(const std::shared_ptr<Ice::Communicator>& = nullptr);

    /**
     * Construct a DataStorm node.
     *
     * This constructor parses the command line arguments into Ice properties and
     * initialize a new Node.
     *
     * @param argc The number of command line arguments in the argv array.
     * @param argv The command line arguments
     */
    Node(int&, char*[]);

    /**
     * Destruct the node. The node destruction releases associated resources.
     */
    ~Node();

    /**
     * Returns the Ice communicator associated with the node.
     */
    std::shared_ptr<Ice::Communicator> getCommunicator() const;

private:

    template<typename T> friend class TopicReader;
    template<typename T> friend class TopicWriter;

    std::shared_ptr<DataStormInternal::TopicFactory> _impl;
    const bool _ownsCommunicator;
};

}

//
// Private abstract API used by public template based API
//

namespace DataStormInternal
{

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
    virtual std::vector<unsigned char> marshal(const std::shared_ptr<Ice::Communicator>&) const = 0;
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
    marshal(const std::shared_ptr<Ice::Communicator>& communicator) const override
    {
        return DataStorm::Encoder<T>::marshal(communicator, _value);
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

    virtual std::shared_ptr<Key> unmarshal(const std::shared_ptr<Ice::Communicator>&,
                                           const std::vector<unsigned char>&) = 0;
};

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
    unmarshal(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<K, KeyT<K>>::create(DataStorm::Encoder<K>::unmarshal(communicator, data));
    }

    static std::function<std::shared_ptr<KeyFactory>()> factory()
    {
        return [] {
            auto f = std::make_shared<KeyFactoryT<K>>();
            f->init();
            return f;
        };
    }
};

class Sample
{
public:

    DataStorm::SampleType type;
    std::shared_ptr<Key> key;
    std::vector<unsigned char> value;
    IceUtil::Time timestamp;
    std::shared_ptr<Ice::Communicator> communicator;
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

    virtual std::shared_ptr<Filter> unmarshal(const std::shared_ptr<Ice::Communicator>&,
                                              const std::vector<unsigned char>&) = 0;
};

template<typename T, typename E> class FilterT : public Filter, public AbstractElementT<T>
{
public:

    using AbstractElementT<T>::AbstractElementT;

    virtual bool match(const std::shared_ptr<Key>& key) const override
    {
        return DataStorm::Filter<typename DataStorm::DataTraits<E>::FilterType>::match(AbstractElementT<T>::_value,
                                 std::static_pointer_cast<KeyT<typename DataStorm::DataTraits<E>::KeyType>>(key)->get());
    }

    virtual bool match(const std::shared_ptr<Sample>& sample) const override
    {
        return DataStorm::Filter<typename DataStorm::DataTraits<E>::FilterType>::match(AbstractElementT<T>::_value,
                                                                          DataStorm::Sample<E>(sample));
    }
};

template<typename K, typename E> class FilterFactoryT : public FilterFactory, public AbstractFactoryT<K, FilterT<K, E>>
{
public:

    using AbstractFactoryT<K, FilterT<K, E>>::AbstractFactoryT;

    virtual std::shared_ptr<Filter>
    unmarshal(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<K, FilterT<K, E>>::create(DataStorm::Encoder<K>::unmarshal(communicator, data));
    }

    static std::function<std::shared_ptr<FilterFactory>()> factory()
    {
        return [] {
            auto f = std::make_shared<FilterFactoryT<K, E>>();
            f->init();
            return f;
        };
    }
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
    virtual std::shared_ptr<KeyFactory> getKeyFactory() const = 0;
    virtual std::shared_ptr<FilterFactory> getFilterFactory() const = 0;
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

    virtual void init() = 0;

    virtual std::shared_ptr<TopicReader> getTopicReader(const std::string&,
                                                        std::function<std::shared_ptr<KeyFactory>()>,
                                                        std::function<std::shared_ptr<FilterFactory>()>) = 0;

    virtual std::shared_ptr<TopicWriter> getTopicWriter(const std::string&,
                                                        std::function<std::shared_ptr<KeyFactory>()>,
                                                        std::function<std::shared_ptr<FilterFactory>()>) = 0;

    virtual void destroy(bool) = 0;

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
Encoder<T>::marshal(const std::shared_ptr<Ice::Communicator>& communicator, const T& value)
{
    std::vector<unsigned char> v;
    Ice::OutputStream stream(communicator);
    stream.write(value);
    stream.finished(v);
    return v;
}

template<typename T> T
Encoder<T>::unmarshal(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& value)
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

template<typename T> std::string
Encoder<T>::toString(const T& value)
{
    return DataStormInternal::Stringifier<T>::toString(value);
}

//
// Sample template implementation
//
template<typename T> SampleType
Sample<T>::getType() const
{
    return _impl->type;
}

template<typename T> typename Sample<T>::KeyType
Sample<T>::getKey() const
{
    if(_impl->key)
    {
        return std::static_pointer_cast<DataStormInternal::KeyT<Sample<T>::KeyType>>(_impl->key)->get();
    }
    return Sample<T>::KeyType();
}

template<typename T> typename Sample<T>::ValueType
Sample<T>::getValue() const
{
    return Encoder<typename Sample<T>::ValueType>::unmarshal(_impl->communicator, _impl->value);
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
// DataReader template implementation
//
template<typename T>
DataReader<T>::~DataReader()
{
    _impl->destroy();
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

template<typename T>
KeyDataReader<T>::KeyDataReader(TopicReader<T>& topic, typename DataTraits<T>::KeyType key) :
    DataReader<T>(topic._impl->getDataReader(topic._keyFactory->create(std::move(key))))
{
}

template<typename T>
FilteredDataReader<T>::FilteredDataReader(TopicReader<T>& topic, typename DataTraits<T>::FilterType filter) :
    DataReader<T>(topic._impl->getFilteredDataReader(topic._filterFactory->create(std::move(filter))))
{
}

//
// DataWriter template implementation
//
template<typename T>
DataWriter<T>::~DataWriter()
{
    _impl->destroy();
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
DataWriter<T>::add(const ValueType& value)
{
    _impl->add(Encoder<ValueType>::marshal(_impl->getCommunicator(), value));
}

template<typename T> void
DataWriter<T>::update(const ValueType& value)
{
    _impl->update(Encoder<ValueType>::marshal(_impl->getCommunicator(), value));
}

template<typename T> void
DataWriter<T>::remove()
{
    _impl->remove();
}

template<typename T>
KeyDataWriter<T>::KeyDataWriter(TopicWriter<T>& topic, typename DataTraits<T>::KeyType key) :
    DataWriter<T>(topic._impl->getDataWriter(topic._keyFactory->create(std::move(key))))
{
}

template<typename T>
FilteredDataWriter<T>::FilteredDataWriter(TopicWriter<T>& topic, typename DataTraits<T>::FilterType filter) :
    DataWriter<T>(topic._impl->getFilteredDataWriter(topic._filterFactory->create(std::move(filter))))
{
}

//
// TopicReader template implementation
//
template<typename T>
TopicReader<T>::TopicReader(Node& node, const std::string& name) :
    _impl(node._impl->getTopicReader(name,
                                     DataStormInternal::KeyFactoryT<KeyType>::factory(),
                                     DataStormInternal::FilterFactoryT<FilterType, T>::factory())),
    _keyFactory(std::static_pointer_cast<DataStormInternal::KeyFactoryT<KeyType>>(_impl->getKeyFactory())),
    _filterFactory(std::static_pointer_cast<DataStormInternal::FilterFactoryT<FilterType, T>>(_impl->getFilterFactory()))
{
}

template<typename T>
TopicReader<T>::~TopicReader()
{
    _impl->destroy();
}

//
// TopicWriter template implementation
//
template<typename T>
TopicWriter<T>::TopicWriter(Node& node, const std::string& name) :
    _impl(node._impl->getTopicWriter(name,
                                     DataStormInternal::KeyFactoryT<KeyType>::factory(),
                                     DataStormInternal::FilterFactoryT<FilterType, T>::factory())),
    _keyFactory(std::static_pointer_cast<DataStormInternal::KeyFactoryT<KeyType>>(_impl->getKeyFactory())),
    _filterFactory(std::static_pointer_cast<DataStormInternal::FilterFactoryT<FilterType, T>>(_impl->getFilterFactory()))
{
}

template<typename T>
TopicWriter<T>::~TopicWriter()
{
    _impl->destroy();
}


}