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

template<typename K> class KeyFactoryT;
template<typename K, typename V, typename F> class FilterFactoryT;

};

namespace DataStorm
{

class Node;
template<typename K, typename V> class KeyDataReader;
template<typename K, typename V> class KeyDataWriter;
template<typename K, typename V> class FilteredDataReader;
template<typename K, typename V> class FilteredDataWriter;
template<typename K, typename V> class Sample;

/**
 * The Encoder template provides methods to encode and decode user types.
 *
 * The encoder template can be specialized to provide encodeling and un-encodeling
 * methods for types that don't support being encodeled with Ice. By default, the
 * Ice encodeling is used if no Encoder template specialization is provided for the
 * type.
 */
template<typename T> struct Encoder
{
    /**
     * Marshals the given value. This method encodes the given value and returns the
     * resulting byte sequence. The factory parameter is provided to allow the implementation
     * to retrieve configuration or any other information required by the marhsalling.
     *
     * @see decode
     *
     * @param communicator The communicator associated with the node
     * @param value The value to encode
     * @return The resulting byte sequence
     */
    static std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>&, const T&);

    /**
     * Unencodes a value. This method decodes the given byte sequence and returns the
     * resulting value. The factory parameter is provided to allow the implementation
     * to retrieve configuration or any other information required by the un-encodeling.
     *
     * @see encode
     *
     * @param communicator The communicator associated with the node
     * @param value The byte sequence to decode
     * @return The resulting value
     */
    static T decode(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&);
};

template<typename T> struct Stringifier
{
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
 * The RegexKeyFilter template filters keys matching a regular expression.
 **/
template<typename K, typename V> class RegexKeyFilter
{
public:

    /**
     * The type used to initialize and encode/decode the filter over the wire.
     */
    using FilterType = std::string;

    /**
     * Construct the filter with the given filter value.
     *
     * @param value The value of the filter.
     */
    RegexKeyFilter(const std::string& value) : _regex(value)
    {
    }

    /**
     * Returns wether or not the key matches the regular expression.
     *
     * @param key The key to match against the regular expression.
     * @return True if the key matches the regular expression, false otherwise.
     */
    bool match(const K& key) const
    {
        return std::regex_match(DataStorm::Stringifier<K>::toString(key), _regex);
    }

    /**
     * Returns wether or not the sample matches the filter. Always returns
     * true.
     *
     * @param sample The sample to match against the filter.
     * @return Always returns true.
     */
    bool match(const Sample<K, V>& sample) const
    {
        return true;
    }

private:

    std::regex _regex;
};

/**
 * The RegexKeyValueFilter template filters keys and values matching regular expressions
 * and a set of sample types.
 **/
template<typename K, typename V> struct RegexKeyValueFilter
{
public:

    /**
     * The type used to initialize and encode/decode the filter over the wire.
     */
    using FilterType = RegexFilter;

    /**
     * Construct the filter with the given RegexFilter value.
     *
     * @param value The value of the filter.
     */
    RegexKeyValueFilter(const RegexFilter& value) : _filter(value), _key(value.key), _value(value.value)
    {
    }

    /**
     * Returns wether or not the key matches the key regular expression.
     *
     * @param key The key to match against the key regular expression.
     * @return True if the key matches the key regular expression, false otherwise.
     */
    bool match(const K& key) const
    {
        if(!_filter.key.empty())
        {
            return std::regex_match(DataStorm::Stringifier<K>::toString(key), _key);
        }
        else
        {
            return true;
        }
    }

    /**
     * Returns wether or not the sample matches the value regular expression
     * and sample types.
     *
     * @param sample The sample to match against the filter.
     * @return True if the key matches the value regular expression and sample
     *         types, false otherwise.
     */
    bool match(const Sample<K, V>& sample) const
    {
        if(!_filter.types.empty())
        {
            if(std::find(_filter.types.begin(), _filter.types.end(), sample.getType()) == _filter.types.end())
            {
                return false;
            }
        }
        if(!_filter.value.empty())
        {
            return std::regex_match(DataStorm::Stringifier<V>::toString(sample.getValue()), _value);
        }
        else
        {
            return true;
        }
    }

private:

    const RegexFilter _filter;
    const std::regex _key;
    const std::regex _value;
};

/**
 * The TopicReader class allows to construct DataReader objects.
 */
template<typename Key, typename Value, typename Filter=RegexKeyFilter<Key, Value>> class TopicReader
{
public:

    using KeyType = Key;
    using ValueType = Value;
    using FilterType = Filter;

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

    friend class KeyDataReader<Key, Value>;
    friend class FilteredDataReader<Key, Value>;

    const std::shared_ptr<DataStormInternal::TopicReader> _impl;
    const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> _keyFactory;
    const std::shared_ptr<DataStormInternal::FilterFactoryT<Key, Value, Filter>> _filterFactory;
};

/**
 * The TopicWriter class allows to construct DataWriter objects.
 */
template<typename Key, typename Value, typename Filter=RegexKeyFilter<Key, Value>> class TopicWriter
{
public:

    using KeyType = Key;
    using ValueType = Value;
    using FilterType = Filter;

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

    friend class KeyDataWriter<Key, Value>;
    friend class FilteredDataWriter<Key, Value>;

    const std::shared_ptr<DataStormInternal::TopicWriter> _impl;
    const std::shared_ptr<DataStormInternal::KeyFactoryT<Key>> _keyFactory;
    const std::shared_ptr<DataStormInternal::FilterFactoryT<Key, Value, Filter>> _filterFactory;
};

/**
 * A sample provides information about an update of a data element.
 *
 * The Sample template provides access to key, value and type of
 * an update to a data element.
 *
 */
template<typename Key, typename Value> class Sample
{
public:

    using KeyType = Key;
    using ValueType = Value;

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
 * The DataReader class is used to retrieve samples for a data element.
 */
template<typename Key, typename Value> class DataReader
{
public:

    using KeyType = Key;
    using ValueType = Value;

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
    std::vector<Sample<Key, Value>> getAll() const;

    /**
     * Returns all the unread data samples.
     *
     * @return The unread data samples.
     */
    std::vector<Sample<Key, Value>> getAllUnread();

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
    Sample<Key, Value> getNextUnread();

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
template<typename Key, typename Value> class KeyDataReader : public DataReader<Key, Value>
{
public:

    /**
     * Construct a new data reader for the given key. The construction of the data reader
     * connects the reader to writers with a matching key.
     *
     * @param topic The topic reader.
     * @param key The key of the data element to read.
     */
    template<typename Filter>
    KeyDataReader(TopicReader<Key, Value, Filter>&, Key);
};

/**
 * The filtered data reader to read data elements whose key match a given filter.
 */
template<typename Key, typename Value> class FilteredDataReader : public DataReader<Key, Value>
{
public:

    /**
     * Construct a new data reader for the given filter. The construction of the data reader
     * connects the reader to writers whose key matches the filter.
     *
     * @param topic The topic reader.
     * @param filter The filter.
     */
    template<typename Filter>
    FilteredDataReader(TopicReader<Key, Value, Filter>&, typename Filter::FilterType);
};

/**
 * The DataWriter class is used to write samples for a data element.
 */
template<typename Key, typename Value> class DataWriter
{
public:

    using KeyType = Key;
    using ValueType = Value;

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
template<typename Key, typename Value> class KeyDataWriter : public DataWriter<Key, Value>
{
public:

    /**
     * Construct a new data writer for the given key. The construction of the data writer
     * connects the writer to writers with a matching key.
     *
     * @param topic The topic writer.
     * @param key The key of the data element to write.
     */
    template<typename Filter>
    KeyDataWriter(TopicWriter<Key, Value, Filter>&, Key);
};

/**
 * The filtered data writer to write data elements whose key match a given filter.
 */
template<typename Key, typename Value> class FilteredDataWriter : public DataWriter<Key, Value>
{
public:

    /**
     * Construct a new data writer for the given filter. The construction of the data writer
     * connects the writer to readers whose key matches the filter.
     *
     * @param topic The topic writer.
     * @param filter The filter.
     */
    template<typename Filter>
    FilteredDataWriter(TopicWriter<Key, Value, Filter>&, typename Filter::FilterType);
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
    Node(const std::shared_ptr<Ice::Communicator>& communicator = nullptr);

    /**
     * Construct a DataStorm node.
     *
     * This constructor parses the command line arguments into Ice properties and
     * initialize a new Node.
     *
     * @param argc The number of command line arguments in the argv array.
     * @param argv The command line arguments
     */
    Node(int& argc, char* argv[]);

    /**
     * Destruct the node. The node destruction releases associated resources.
     */
    ~Node();

    /**
     * Returns the Ice communicator associated with the node.
     */
    std::shared_ptr<Ice::Communicator> getCommunicator() const;

private:

    std::shared_ptr<DataStormInternal::TopicFactory> _impl;

    template<typename, typename, typename> friend class TopicReader;
    template<typename, typename, typename> friend class TopicWriter;

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
    template<typename TT, typename SS>
    static auto test(int) -> decltype(std::declval<SS&>() << std::declval<TT>(), std::true_type());

    template<typename, typename>
    static auto test(...) -> std::false_type;

public:

    static const bool value = decltype(test<T, std::ostream>(0))::value;
};

template<typename T, typename C=void> struct Stringifier
{
    static std::string
    toString(const T& value)
    {
        std::ostringstream os;
        os << typeid(value).name() << '(' << &value << ')';
        return os.str();
    }
};

template<typename T> struct Stringifier<T, typename std::enable_if<is_streamable<T>::value>::type>
{
    static std::string
    toString(const T& value)
    {
        std::ostringstream os;
        os << value;
        return os.str();
    }
};

template<typename T> class AbstractFactoryT : public std::enable_shared_from_this<AbstractFactoryT<T>>
{
    using K = typename T::CachedType;
    using V = T;

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

        std::weak_ptr<AbstractFactoryT<T>> _factory;

    } _deleter;

public:

    AbstractFactoryT() : _nextId(0)
    {
    }

    void
    init()
    {
        _deleter = { std::enable_shared_from_this<AbstractFactoryT<T>>::shared_from_this() };
    }

    template<typename F> std::shared_ptr<V>
    create(F&& v)
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

        auto k = std::shared_ptr<V>(new V(std::forward<F>(v), _nextId++), _deleter);
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
    virtual std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>&) const = 0;
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
        os << DataStorm::Stringifier<T>::toString(_value) << ':' << _id;
        return os.str();
    }

    virtual std::vector<unsigned char>
    encode(const std::shared_ptr<Ice::Communicator>& communicator) const override
    {
        return DataStorm::Encoder<T>::encode(communicator, _value);
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

    virtual std::shared_ptr<Key> decode(const std::shared_ptr<Ice::Communicator>&,
                                        const std::vector<unsigned char>&) = 0;
};

template<typename K> class KeyT : public Key, public AbstractElementT<K>
{
public:

    using CachedType = K;
    using AbstractElementT<K>::AbstractElementT;
};

template<typename K> class KeyFactoryT : public KeyFactory, public AbstractFactoryT<KeyT<K>>
{
public:

    using AbstractFactoryT<KeyT<K>>::AbstractFactoryT;

    virtual std::shared_ptr<Key>
    decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<KeyT<K>>::create(DataStorm::Encoder<K>::decode(communicator, data));
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

    virtual std::shared_ptr<Filter> decode(const std::shared_ptr<Ice::Communicator>&,
                                           const std::vector<unsigned char>&) = 0;
};

template<typename K, typename V, typename F, typename C=void> class FilterT :
    public Filter, public AbstractElementT<typename F::FilterType>
{
public:

    using CachedType = typename F::FilterType;

    FilterT(typename F::FilterType&& v, long long int id) :
        AbstractElementT<typename F::FilterType>::AbstractElementT(std::forward<typename F::FilterType>(v), id),
        _filter(AbstractElementT<typename F::FilterType>::_value)
    {
    }

    virtual bool match(const std::shared_ptr<Key>& key) const override
    {
        return _filter.match(std::static_pointer_cast<KeyT<K>>(key)->get());
    }

    virtual bool match(const std::shared_ptr<Sample>& sample) const override
    {
        return _filter.match(DataStorm::Sample<K, V>(sample));
    }

private:

    F _filter;
};

template<typename V, typename F>
class has_no_filter
{
    template<typename VV, typename FF>
    static auto test(int) -> decltype(std::declval<FF&>().match(std::declval<const VV&>()), std::true_type());

    template<typename, typename>
    static auto test(...) -> std::false_type;

public:

    static const bool value = !decltype(test<V, F>(0))::value;
};

template<typename K, typename V, typename F>
class FilterT<K, V, F, typename std::enable_if<has_no_filter<K, F>::value>::type> :
    public Filter, public AbstractElementT<typename F::FilterType>
{
public:

    using CachedType = typename F::FilterType;

    FilterT(typename F::FilterType&& v, long long int id) :
        AbstractElementT<typename F::FilterType>::AbstractElementT(std::forward<typename F::FilterType>(v), id),
        _filter(AbstractElementT<typename F::FilterType>::_value)
    {
    }

    virtual bool match(const std::shared_ptr<Key>& key) const override
    {
        return true;
    }

    virtual bool match(const std::shared_ptr<Sample>& sample) const override
    {
        return _filter.match(DataStorm::Sample<K, V>(sample));
    }

private:

    F _filter;
};

template<typename K, typename V, typename F>
class FilterT<K, V, F, typename std::enable_if<has_no_filter<DataStorm::Sample<K, V>, F>::value>::type> :
    public Filter, public AbstractElementT<typename F::FilterType>
{
public:

    using CachedType = typename F::FilterType;

    FilterT(typename F::FilterType&& v, long long int id) :
        AbstractElementT<typename F::FilterType>::AbstractElementT(std::forward<typename F::FilterType>(v), id),
        _filter(AbstractElementT<typename F::FilterType>::_value)
    {
    }

    virtual bool match(const std::shared_ptr<Key>& key) const override
    {
        return _filter.match(std::static_pointer_cast<KeyT<K>>(key)->get());
    }

    virtual bool match(const std::shared_ptr<Sample>& sample) const override
    {
        return true;
    }

private:

    F _filter;
};

template<typename K, typename V, typename F> class FilterFactoryT : public FilterFactory,
                                                                    public AbstractFactoryT<FilterT<K, V, F>>
{
public:

    using AbstractFactoryT<FilterT<K, V, F>>::AbstractFactoryT;

    virtual std::shared_ptr<Filter>
    decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<FilterT<K, V, F>>::create(DataStorm::Encoder<typename F::FilterType>::decode(communicator, data));
    }

    static std::function<std::shared_ptr<FilterFactory>()> factory()
    {
        return [] {
            auto f = std::make_shared<FilterFactoryT<K, V, F>>();
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
Encoder<T>::encode(const std::shared_ptr<Ice::Communicator>& communicator, const T& value)
{
    std::vector<unsigned char> v;
    Ice::OutputStream stream(communicator);
    stream.write(value);
    stream.finished(v);
    return v;
}

template<typename T> T
Encoder<T>::decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& value)
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
Stringifier<T>::toString(const T& value)
{
    return DataStormInternal::Stringifier<T>::toString(value);
}

//
// Sample template implementation
//
template<typename Key, typename Value> SampleType
Sample<Key, Value>::getType() const
{
    return _impl->type;
}

template<typename Key, typename Value> Key
Sample<Key, Value>::getKey() const
{
    if(_impl->key)
    {
        return std::static_pointer_cast<DataStormInternal::KeyT<Key>>(_impl->key)->get();
    }
    return Sample<Key, Value>::KeyType();
}

template<typename Key, typename Value> Value
Sample<Key, Value>::getValue() const
{
    return Encoder<typename Sample<Key, Value>::ValueType>::decode(_impl->communicator, _impl->value);
}

template<typename Key, typename Value> IceUtil::Time
Sample<Key, Value>::getTimestamp() const
{
    return _impl->timestamp;
}

template<typename Key, typename Value>
Sample<Key, Value>::Sample(const std::shared_ptr<DataStormInternal::Sample>& impl) : _impl(std::move(impl))
{
}

//
// DataReader template implementation
//
template<typename Key, typename Value>
DataReader<Key, Value>::~DataReader()
{
    _impl->destroy();
}

template<typename Key, typename Value> bool
DataReader<Key, Value>::hasWriters() const
{
    return _impl->hasWriters();
}

template<typename Key, typename Value> void
DataReader<Key, Value>::waitForWriters(unsigned int count) const
{
    _impl->waitForWriters(count);
}

template<typename Key, typename Value> void
DataReader<Key, Value>::waitForNoWriters() const
{
    _impl->waitForWriters(-1);
}

template<typename Key, typename Value> int
DataReader<Key, Value>::getInstanceCount() const
{
    return _impl->getInstanceCount();
}

template<typename Key, typename Value> std::vector<Sample<Key, Value>>
DataReader<Key, Value>::getAll() const
{
    auto all = _impl->getAll();
    std::vector<Sample<Key, Value>> samples;
    samples.reserve(all.size());
    for(const auto& sample : all)
    {
        samples.emplace_back(sample);
    }
    return samples;
}

template<typename Key, typename Value> std::vector<Sample<Key, Value>>
DataReader<Key, Value>::getAllUnread()
{
    auto unread = _impl->getAllUnread();
    std::vector<Sample<Key, Value>> samples;
    samples.reserve(unread.size());
    for(auto sample : unread)
    {
        samples.emplace_back(sample);
    }
    return samples;
}

template<typename Key, typename Value> void
DataReader<Key, Value>::waitForUnread(unsigned int count) const
{
    _impl->waitForUnread(count);
}

template<typename Key, typename Value> bool
DataReader<Key, Value>::hasUnread() const
{
    return _impl->hasUnread();
}

template<typename Key, typename Value> Sample<Key, Value>
DataReader<Key, Value>::getNextUnread()
{
    return Sample<Key, Value>(_impl->getNextUnread());
}

template<typename Key, typename Value> template<typename Filter>
KeyDataReader<Key, Value>::KeyDataReader(TopicReader<Key, Value, Filter>& topic, Key key) :
    DataReader<Key, Value>(topic._impl->getDataReader(topic._keyFactory->create(std::move(key))))
{
}

template<typename Key, typename Value> template<typename Filter>
FilteredDataReader<Key, Value>::FilteredDataReader(TopicReader<Key, Value, Filter>& topic,
                                                   typename Filter::FilterType filter) :
    DataReader<Key, Value>(topic._impl->getFilteredDataReader(topic._filterFactory->create(std::move(filter))))
{
}

//
// DataWriter template implementation
//
template<typename Key, typename Value>
DataWriter<Key, Value>::~DataWriter()
{
    _impl->destroy();
}

template<typename Key, typename Value> bool
DataWriter<Key, Value>::hasReaders() const
{
    return _impl->hasReaders();
}

template<typename Key, typename Value> void
DataWriter<Key, Value>::waitForReaders(unsigned int count) const
{
    return _impl->waitForReaders(count);
}

template<typename Key, typename Value> void
DataWriter<Key, Value>::waitForNoReaders() const
{
    return _impl->waitForReaders(-1);
}

template<typename Key, typename Value> void
DataWriter<Key, Value>::add(const Value& value)
{
    _impl->add(Encoder<Value>::encode(_impl->getCommunicator(), value));
}

template<typename Key, typename Value> void
DataWriter<Key, Value>::update(const Value& value)
{
    _impl->update(Encoder<Value>::encode(_impl->getCommunicator(), value));
}

template<typename Key, typename Value> void
DataWriter<Key, Value>::remove()
{
    _impl->remove();
}

template<typename Key, typename Value> template<typename Filter>
KeyDataWriter<Key, Value>::KeyDataWriter(TopicWriter<Key, Value, Filter>& topic, Key key) :
    DataWriter<Key, Value>(topic._impl->getDataWriter(topic._keyFactory->create(std::move(key))))
{
}

template<typename Key, typename Value> template<typename Filter>
FilteredDataWriter<Key, Value>::FilteredDataWriter(TopicWriter<Key, Value, Filter>& topic,
                                                   typename Filter::FilterType filter) :
    DataWriter<Key, Value>(topic._impl->getFilteredDataWriter(topic._filterFactory->create(std::move(filter))))
{
}

//
// TopicReader template implementation
//
template<typename Key, typename Value, typename Filter>
TopicReader<Key, Value, Filter>::TopicReader(Node& node, const std::string& name) :
    _impl(node._impl->getTopicReader(name,
                                     DataStormInternal::KeyFactoryT<Key>::factory(),
                                     DataStormInternal::FilterFactoryT<Key, Value, Filter>::factory())),
    _keyFactory(std::static_pointer_cast<DataStormInternal::KeyFactoryT<Key>>(_impl->getKeyFactory())),
    _filterFactory(std::static_pointer_cast<DataStormInternal::FilterFactoryT<Key, Value, Filter>>(_impl->getFilterFactory()))
{
}

template<typename Key, typename Value, typename Filter>
TopicReader<Key, Value, Filter>::~TopicReader()
{
    _impl->destroy();
}

//
// TopicWriter template implementation
//
template<typename Key, typename Value, typename Filter>
TopicWriter<Key, Value, Filter>::TopicWriter(Node& node, const std::string& name) :
    _impl(node._impl->getTopicWriter(name,
                                     DataStormInternal::KeyFactoryT<Key>::factory(),
                                     DataStormInternal::FilterFactoryT<Key, Value, Filter>::factory())),
    _keyFactory(std::static_pointer_cast<DataStormInternal::KeyFactoryT<Key>>(_impl->getKeyFactory())),
    _filterFactory(std::static_pointer_cast<DataStormInternal::FilterFactoryT<Key, Value, Filter>>(_impl->getFilterFactory()))
{
}

template<typename Key, typename Value, typename Filter>
TopicWriter<Key, Value, Filter>::~TopicWriter()
{
    _impl->destroy();
}


}