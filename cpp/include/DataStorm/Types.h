// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Optional.h>

namespace DataStorm
{

/**
 * The discard policy to specify how samples are discarded by receivers upon receival.
 */
enum struct DiscardPolicy
{
    /** Samples are never discarded. */
    None,

    /**
     * Samples are discared based on the sample timestamp. If the received sample
     * timestamp is older than that last received sample, the sample is discarded.
     * This ensures that readers will eventually always end up with the same
     * view of the data if multiple writers are sending samples.
     **/
    SendTime,

    /**
     * Samples are discarded based on the writer priority. Only samples from the
     * highest priority connected writers are kept, others are discarded.
     */
    Priority
};

/**
 * The clear history policy specifies when the history is cleared. The history
 * can be never cleared or cleared when a Add or Remove sample is received.
 **/
enum struct ClearHistoryPolicy
{
    /** Clear the sample history when a Add sample is received. */
    Add,

    /** Clear the sample history when a Remove sample is received. */
    Remove,

    /** Clear the sample history when a Add or Remove sample is received. */
    AddOrRemove,

    /** Never clear the sample history. */
    Never
};

/**
 * The configuration base class holds configuration options common to readers and
 * writers.
 */
class Config
{
public:

    Config(Ice::optional<int> sampleCount = Ice::nullopt,
           Ice::optional<int> sampleLifetime = Ice::nullopt,
           Ice::optional<ClearHistoryPolicy> clearHistory = Ice::nullopt) :
        sampleCount(std::move(sampleCount)),
        sampleLifetime(std::move(sampleLifetime)),
        clearHistory(std::move(clearHistory))
    {
    }

    /**
     * The sampleCount configuration specifies how many samples are kept by the
     * reader or writer in its sample history. By default, only a single sample
     * is kept in the history.
     */
    Ice::optional<int> sampleCount;

    /**
     * The sampleLifetime configuration specifies samples to keep in the writer
     * or reader history based on their age. Samples with a timestamp older than
     * the sampleLifetime value (in milliseconds) are discarded from the history.
     * By default, the samples are kept for an unlimited amount of time.
     */
    Ice::optional<int> sampleLifetime;

    /**
     * The clear history policy specifies when samples are removed from the
     * sample history. By default, samples are removed when a add or remove
     * sample is received.
     */
    Ice::optional<ClearHistoryPolicy> clearHistory;
};

class ReaderConfig : public Config
{
public:

    ReaderConfig(Ice::optional<int> sampleCount = Ice::nullopt,
                 Ice::optional<int> sampleLifetime = Ice::nullopt,
                 Ice::optional<ClearHistoryPolicy> clearHistory = Ice::nullopt,
                 Ice::optional<DiscardPolicy> discardPolicy = Ice::nullopt) :
        Config(std::move(sampleCount), std::move(sampleLifetime), std::move(clearHistory)),
        discardPolicy(std::move(discardPolicy))
    {
    }

    /**
     * Specifies if and how samples are discarded after being received by a
     * reader.
     */
    Ice::optional<DiscardPolicy> discardPolicy;
};

class WriterConfig : public Config
{
public:

    WriterConfig(Ice::optional<int> sampleCount = Ice::nullopt,
                 Ice::optional<int> sampleLifetime  = Ice::nullopt,
                 Ice::optional<ClearHistoryPolicy> clearHistory = Ice::nullopt,
                 Ice::optional<int> priority = Ice::nullopt) :
        Config(std::move(sampleCount), std::move(sampleLifetime), std::move(clearHistory)),
        priority(std::move(priority))
    {
    }

    /**
     * Specifies the writer priority. The priority is used by readers using
     * the priority discard policy.
     */
    Ice::optional<int> priority;
};

/**
 * The Encoder template provides a method to encode decode user types.
 *
 * The encoder template can be specialized to provide encoding for types that don't
 * support being encoded with Ice. By default, the Ice encoding is used if no
 * Encoder template specialization is provided for the type.
 */
template<typename T, typename Enabler=void>
struct Encoder
{
    /**
     * Encode the given value. This method encodes the given value and returns the
     * resulting byte sequence. The communicator parameter is provided to allow the
     * implementation to eventually use the Ice encoding.
     *
     * @see decode
     *
     * @param communicator The communicator associated with the node
     * @param value The value to encode
     * @return The resulting byte sequence
     */
    static std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>&, const T&);
};

/**
 * The Decoder template provides a method to decode user types.
 *
 * The decoder template can be specialized to provide decoding for types that don't
 * support being decoded with Ice. By default, the Ice decoding is used if no
 * Decoder template specialization is provided for the type.
 */
template<typename T, typename Enabler=void>
struct Decoder
{
    /**
     * Unencodes a value. This method decodes the given byte sequence and returns the
     * resulting value. The communicator parameter is provided to allow the
     * implementation to eventually use the Ice encoding.
     *
     * @see encode
     *
     * @param communicator The communicator associated with the node
     * @param value The byte sequence to decode
     * @return The resulting value
     */
    static T decode(const std::shared_ptr<Ice::Communicator>&, const std::vector<unsigned char>&);
};

/**
 * The Cloner template provides a method to clone user types.
 *
 * The cloner template can be specialized to provide cloning for types that
 * require special cloning. By defaut, the template uses plain C++ copy.
 */
template<typename T, typename Enabler=void>
struct Cloner
{
    /**
     * Encode the given value. This method encodes the given value and returns the
     * resulting byte sequence. The communicator parameter is provided to allow the
     * implementation to eventually use the Ice encoding.
     *
     * @see decode
     *
     * @param communicator The communicator associated with the node
     * @param value The value to encode
     * @return The resulting byte sequence
     */
    static T clone(const T& value)
    {
        return value;
    }
};

/**
 * Encoder template specilization to encode Ice::Value instances
 **/
template<typename T>
struct Encoder<T, typename std::enable_if<std::is_base_of<::Ice::Value, T>::value>::type>
{
    static std::vector<unsigned char> encode(const std::shared_ptr<Ice::Communicator>& communicator, const T& value)
    {
        return Encoder<std::shared_ptr<T>>::encode(communicator, std::make_shared<T>(value));
    }
};

/**
 * Decoder template specilization to decode Ice::Value instances
 **/
template<typename T>
struct Decoder<T, typename std::enable_if<std::is_base_of<::Ice::Value, T>::value>::type>
{
    static T decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& data)
    {
        return *Decoder<std::shared_ptr<T>>::decode(communicator, data);
    }
};

/**
 * Cloner template specialization to clone shared Ice values using ice_clone.
 */
template<typename T>
struct Cloner<std::shared_ptr<T>, typename std::enable_if<std::is_base_of<::Ice::Value, T>::value>::type>
{
    static std::shared_ptr<T> clone(const std::shared_ptr<T>& value)
    {
        return value->ice_clone();
    }
};

//
// Encoder template implementation
//
template<typename T, typename E> std::vector<unsigned char>
Encoder<T, E>::encode(const std::shared_ptr<Ice::Communicator>& communicator, const T& value)
{
    std::vector<unsigned char> v;
    Ice::OutputStream stream(communicator);
    stream.write(value);
    stream.finished(v);
    return v;
}

//
// Decoder template implementation
//
template<typename T, typename E> T
Decoder<T, E>::decode(const std::shared_ptr<Ice::Communicator>& communicator, const std::vector<unsigned char>& value)
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

}