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
#include <DataStorm/Types.h>
#include <DataStorm/InternalI.h>

namespace DataStorm
{

template<typename K, typename V, typename U> class Sample;

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

template<typename T, typename Enabler=void> struct Stringifier
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

    virtual std::string toString() const override
    {
        std::ostringstream os;
        os << _id << ':' << Stringifier<T>::toString(_value);
        return os.str();
    }

    virtual std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>& communicator) const override
    {
        return DataStorm::Encoder<T>::encode(communicator, _value);
    }

    virtual long long int getId() const override
    {
        return _id;
    }

    const T& get() const
    {
        return _value;
    }

protected:

    const T _value;
    const long long int _id;
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

    AbstractFactoryT() : _nextId(1)
    {
    }

    void
    init()
    {
        _deleter = { std::enable_shared_from_this<AbstractFactoryT<K, V>>::shared_from_this() };
    }

    template<typename F> std::shared_ptr<typename V::ClassType>
    create(F&& value)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return createImpl(std::forward<F>(value));
    }

    std::vector<std::shared_ptr<typename V::ClassType>>
    create(std::vector<K> values)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<std::shared_ptr<typename V::ClassType>> seq;
        for(auto& v : values)
        {
            seq.push_back(createImpl(std::move(v)));
        }
        return seq;
    }

protected:

    friend struct Deleter;

    std::shared_ptr<typename V::ClassType>
    getImpl(long long id) const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto p = _elementsById.find(id);
        if(p != _elementsById.end())
        {
            auto k = p->second.lock();
            if(k)
            {
                return k;
            }
        }
        return nullptr;
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

        auto k = std::shared_ptr<V>(new V(std::forward<F>(value), ++_nextId), _deleter);
        _elements[k->get()] = k;
        _elementsById[k->getId()] = k;
        return k;
    }

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
        _elementsById.erase(v->getId());
    }

    mutable std::mutex _mutex;
    std::map<K, std::weak_ptr<V>> _elements;
    std::map<long long int, std::weak_ptr<V>> _elementsById;
    long long int _nextId;
};

template<typename K> class KeyT : public Key, public AbstractElementT<K>
{
public:

    virtual std::string toString() const override
    {
        return "k" + AbstractElementT<K>::toString();
    }

    using AbstractElementT<K>::AbstractElementT;
    using ClassType = Key;
};

template<typename K> class KeyFactoryT : public KeyFactory, public AbstractFactoryT<K, KeyT<K>>
{
public:

    using AbstractFactoryT<K, KeyT<K>>::AbstractFactoryT;

    virtual std::shared_ptr<Key>
    get(long long int id) const override
    {
        return AbstractFactoryT<K, KeyT<K>>::getImpl(id);
    }

    virtual std::shared_ptr<Key>
    decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<K, KeyT<K>>::create(DataStorm::Decoder<K>::decode(communicator, data));
    }

    static std::shared_ptr<KeyFactoryT<K>> createFactory()
    {
        auto f = std::make_shared<KeyFactoryT<K>>();
        f->init();
        return f;
    }
};

template<typename T> class TagT : public Tag, public AbstractElementT<T>
{
public:

    virtual std::string toString() const override
    {
        return "t" + AbstractElementT<T>::toString();
    }

    using AbstractElementT<T>::AbstractElementT;
    using ClassType = Tag;
};

template<typename T> class TagFactoryT : public TagFactory, public AbstractFactoryT<T, TagT<T>>
{
public:

    using AbstractFactoryT<T, TagT<T>>::AbstractFactoryT;

    virtual std::shared_ptr<Tag>
    get(long long int id) const override
    {
        return AbstractFactoryT<T, TagT<T>>::getImpl(id);
    }

    virtual std::shared_ptr<Tag>
    decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<T, TagT<T>>::create(DataStorm::Decoder<T>::decode(communicator, data));
    }

    static std::shared_ptr<TagFactoryT<T>> createFactory()
    {
        auto f = std::make_shared<TagFactoryT<T>>();
        f->init();
        return f;
    }
};

template<typename Key, typename Value, typename UpdateTag> class SampleT :
    public Sample, public std::enable_shared_from_this<SampleT<Key, Value, UpdateTag>>
{
public:

    SampleT(const std::string& session,
            long long int topic,
            long long int element,
            long long int id,
            DataStorm::SampleEvent event,
            const std::shared_ptr<DataStormInternal::Key>& key,
            const std::shared_ptr<DataStormInternal::Tag>& tag,
            std::vector<unsigned char> value,
            long long int timestamp) :
        Sample(session, topic, element, id, event, key, tag, value, timestamp), _hasValue(false)
    {
    }

    SampleT(DataStorm::SampleEvent event) : Sample(event), _hasValue(false)
    {
    }

    SampleT(DataStorm::SampleEvent event, Value value) : Sample(event), _hasValue(true), _value(std::move(value))
    {
    }

    SampleT(std::vector<unsigned char> value, const std::shared_ptr<Tag>& tag) :
        Sample(DataStorm::SampleEvent::PartialUpdate, tag),
        _hasValue(false)
    {
        _encodedValue = std::move(value);
    }

    DataStorm::Sample<Key, Value, UpdateTag>
    get()
    {
        auto impl = std::enable_shared_from_this<SampleT<Key, Value, UpdateTag>>::shared_from_this();
        return DataStorm::Sample<Key, Value, UpdateTag>(impl);
    }

    const Key& getKey()
    {
        assert(key);
        return std::static_pointer_cast<KeyT<Key>>(key)->get();
    }

    const Value& getValue() const
    {
        assert(_hasValue);
        return _value;
    }

    UpdateTag getTag() const
    {
        return tag ? std::static_pointer_cast<TagT<UpdateTag>>(tag)->get() : UpdateTag();
    }

    void setValue(Value value)
    {
        _value = std::move(value);
        _hasValue = true;
    }

    virtual bool hasValue() const override
    {
        return _hasValue;
    }

    virtual void setValue(const std::shared_ptr<Sample>& sample) override
    {
        if(sample)
        {
            _value = DataStorm::Cloner<Value>::clone(
                std::static_pointer_cast<DataStormInternal::SampleT<Key, Value, UpdateTag>>(sample)->getValue());
        }
        else
        {
            _value = Value();
        }
        _hasValue = true;
    }

    virtual const std::vector<unsigned char>& encode(const std::shared_ptr<Ice::Communicator>& communicator) override
    {
        if(_encodedValue.empty())
        {
            assert(_hasValue || event == DataStorm::SampleEvent::Remove);
            _encodedValue = DataStorm::Encoder<Value>::encode(communicator, _value);
        }
        return _encodedValue;
    }

    virtual std::vector<unsigned char> encodeValue(const std::shared_ptr<Ice::Communicator>& communicator) override
    {
        assert(_hasValue);
        return DataStorm::Encoder<Value>::encode(communicator, _value);
    }

    virtual void decode(const std::shared_ptr<Ice::Communicator>& communicator) override
    {
        if(!_encodedValue.empty())
        {
            _hasValue = true;
            _value = DataStorm::Decoder<Value>::decode(communicator, _encodedValue);
            _encodedValue.clear();
        }
    }

private:

    bool _hasValue;
    Value _value;
};

template<typename Key, typename Value, typename UpdateTag> class SampleFactoryT : public SampleFactory
{
public:

    virtual std::shared_ptr<Sample> create(const std::string& session,
                                           long long int topic,
                                           long long int element,
                                           long long int id,
                                           DataStorm::SampleEvent type,
                                           const std::shared_ptr<DataStormInternal::Key>& key,
                                           const std::shared_ptr<DataStormInternal::Tag>& tag,
                                           std::vector<unsigned char> value,
                                           long long int timestamp)
    {
        return std::make_shared<SampleT<Key, Value, UpdateTag>>(session,
                                                                topic,
                                                                element,
                                                                id,
                                                                type,
                                                                key,
                                                                tag,
                                                                std::move(value),
                                                                timestamp);
    }
};

template<typename F, typename C, typename V> class FilterT : public Filter, public AbstractElementT<C>
{
public:

    using ClassType = Filter;

    template<typename FF>
    FilterT(FF&& v, long long int id) :
        AbstractElementT<C>::AbstractElementT(std::forward<FF>(v), id),
        _filter(AbstractElementT<C>::_value)
    {
    }

    virtual std::string toString() const override
    {
        return "f" + AbstractElementT<C>::toString();
    }

    virtual bool match(const std::shared_ptr<Filterable>& value) const override
    {
        return _filter.match(std::static_pointer_cast<V>(value)->get());
    }

private:

    F _filter;
};

template<typename F, typename C, typename V> class FilterFactoryT : public FilterFactory,
                                                                    public AbstractFactoryT<C, FilterT<F, C, V>>
{
public:

    FilterFactoryT()
    {
    }

    virtual std::shared_ptr<Filter>
    get(long long int id) const override
    {
        return AbstractFactoryT<C, FilterT<F, C, V>>::getImpl(id);
    }

    virtual std::shared_ptr<Filter>
    decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data) override
    {
        return AbstractFactoryT<C, FilterT<F, C, V>>::create(DataStorm::Decoder<C>::decode(communicator, data));
    }

    static std::shared_ptr<FilterFactoryT<F, C, V>> createFactory()
    {
        auto f = std::make_shared<FilterFactoryT<F, C, V>>();
        f->init();
        return f;
    }
};

template<typename V> class FilterFactoryT<void, void, V> : public FilterFactory
{
public:

    virtual std::shared_ptr<Filter>
    decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data) override
    {
        assert(false);
        return nullptr;
    }

    static std::shared_ptr<FilterFactoryT<void, void, V>> createFactory()
    {
        return nullptr;
    }
};

}
