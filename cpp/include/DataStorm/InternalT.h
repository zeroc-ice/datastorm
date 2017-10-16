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
#include <DataStorm/SampleType.h>
#include <DataStorm/Filter.h>
#include <DataStorm/InternalI.h>

namespace DataStorm
{

template<typename K, typename V> class Sample;
template<typename T> struct Encoder;
template<typename T> struct Stringifier;

}

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

template<typename T> class AbstractElementT : virtual public Element
{
public:

    template<typename TT>
    AbstractElementT(TT&& v, long long int id) : _value(std::forward<TT>(v)), _id(id)
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

    template<typename F> std::shared_ptr<typename V::ClassType>
    create(F&& value)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return createImpl(std::forward<F>(value));
    }

    std::vector<std::shared_ptr<typename V::ClassType>>
    create(const std::vector<K>& values)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<std::shared_ptr<typename V::ClassType>> seq;
        for(const auto& v : values)
        {
            seq.push_back(createImpl(v));
        }
        return seq;
    }

    template<typename F> std::shared_ptr<V>
    createImpl(F&& value)
    {
        auto p = _elements.find(value);
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

        auto k = std::shared_ptr<V>(new V(std::forward<F>(value), _nextId++), _deleter);
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

template<typename K> class KeyT : public Key, public AbstractElementT<K>
{
public:

    using CachedType = K;
    using AbstractElementT<K>::AbstractElementT;
    using ClassType = Key;
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

    static std::shared_ptr<KeyFactoryT<K>> createFactory()
    {
        auto f = std::make_shared<KeyFactoryT<K>>();
        f->init();
        return f;
    }
};

template<typename Key, typename Value> class SampleT : public Sample
{
public:

    SampleT(DataStorm::SampleType type, Value value) : Sample(0, type, nullptr, {}, 0), _value(std::move(value))
    {
    }

    using Sample::Sample;

    static FactoryType factory()
    {
        return [](long long int id,
                  DataStorm::SampleType type,
                  const std::shared_ptr<DataStormInternal::Key>& key,
                  std::vector<unsigned char> value,
                  long long int timestamp)
        {
            return std::shared_ptr<Sample>(std::make_shared<SampleT<Key, Value>>(id,
                                                                                 type,
                                                                                 key,
                                                                                 std::move(value),
                                                                                 timestamp));
        };
    }

    Key getKey()
    {
        if(key)
        {
            return std::static_pointer_cast<KeyT<Key>>(key)->get();
        }
        return Key();
    }

    Value getValue() const
    {
        return _value;
    }

    virtual void decode(const std::shared_ptr<Ice::Communicator>& communicator) override
    {
        if(!_encodedValue.empty())
        {
            _value = DataStorm::Encoder<Value>::decode(communicator, _encodedValue);
            _encodedValue.clear();
        }
    }

    virtual const std::vector<unsigned char>& encode(const std::shared_ptr<Ice::Communicator>& communicator) override
    {
        if(_encodedValue.empty())
        {
            _encodedValue = DataStorm::Encoder<Value>::encode(communicator, _value);
        }
        return _encodedValue;
    }

private:

    Value _value;
};

template<typename V, typename F>
class has_key_filter
{
    template<typename VV, typename FF>
    static auto test(int) -> decltype(std::declval<FF&>().match(std::declval<const VV&>()), std::true_type());

    template<typename, typename>
    static auto test(...) -> std::false_type;

public:

    static const bool value = decltype(test<V, F>(0))::value;
};

template<typename K, typename V, typename F>
class has_rsample_filter
{
    template<typename VV, typename FF>
    static auto test(int) -> decltype(std::declval<FF&>().readerMatch(std::declval<const VV&>()), std::true_type());

    template<typename, typename>
    static auto test(...) -> std::false_type;

public:

    static const bool value = decltype(test<DataStorm::Sample<K, V>, F>(0))::value;
};

template<typename K, typename V, typename F>
class has_wsample_filter
{
    template<typename VV, typename FF>
    static auto test(int) -> decltype(std::declval<FF&>().writerMatch(std::declval<const VV&>()), std::true_type());

    template<typename, typename>
    static auto test(...) -> std::false_type;

public:

    static const bool value = decltype(test<DataStorm::Sample<K, V>, F>(0))::value;
};

template<bool> struct FilterMatch;

template<> struct FilterMatch<false>
{
    template<typename F, typename T>
    static bool match(const F& f, T v)
    {
        return true;
    }

    template<typename F, typename T>
    static bool readerMatch(const F& f, T v)
    {
        return true;
    }

    template<typename F, typename T>
    static bool writerMatch(const F& f, T v)
    {
        return true;
    }
};

template<> struct FilterMatch<true>
{
    template<typename F, typename T>
    static bool match(const F& f, T v)
    {
        return f.match(v);
    }

    template<typename F, typename T>
    static bool readerMatch(const F& f, T v)
    {
        return f.readerMatch(v);
    }

    template<typename F, typename T>
    static bool writerMatch(const F& f, T v)
    {
        return f.writerMatch(v);
    }
};

template<typename K, typename V, typename F> class FilterT :
    public Filter, public AbstractElementT<typename F::FilterType>
{
public:

    using CachedType = typename F::FilterType;
    using ClassType = Filter;

    template<typename FF>
    FilterT(FF&& v, long long int id) :
        AbstractElementT<typename F::FilterType>::AbstractElementT(std::forward<FF>(v), id),
        _filter(AbstractElementT<typename F::FilterType>::_value)
    {
    }

    virtual bool match(const std::shared_ptr<Key>& key) const override
    {
        return FilterMatch<has_key_filter<K, F>::value>::match(_filter, std::static_pointer_cast<KeyT<K>>(key)->get());
    }

    virtual bool hasReaderMatch() const override
    {
        return has_rsample_filter<K, V, F>::value;
    }

    virtual bool readerMatch(const std::shared_ptr<Sample>& sample) const override
    {
        return FilterMatch<has_rsample_filter<K, V, F>::value>::readerMatch(_filter, DataStorm::Sample<K, V>(sample));
    }

    virtual bool hasWriterMatch() const override
    {
        return has_wsample_filter<K, V, F>::value;
    }

    virtual bool writerMatch(const std::shared_ptr<Sample>& sample) const override
    {
        return FilterMatch<has_wsample_filter<K, V, F>::value>::writerMatch(_filter, DataStorm::Sample<K, V>(sample));
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

    static std::shared_ptr<FilterFactoryT<K, V, F>> createFactory()
    {
        auto f = std::make_shared<FilterFactoryT<K, V, F>>();
        f->init();
        return f;
    }
};

}
