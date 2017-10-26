// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/optional.h>

namespace DataStorm
{

/**
 * The discard policy to specify how samples are discarded by receivers under certain
 * conditions.
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
 * The configuration base class holds configuration options common to readers and
 * writers.
 */
class Config
{
public:

    /**
     * The sampleCount configuration specifies how many samples are kept by the
     * reader or writer in its sample history.
     */
    Ice::optional<int> sampleCount;

    /**
     * The sampleLifetime configuration specifies samples to keep in the writer
     * or reader history based on their age. Samples with a timestamp older than
     * the sampleLifetime are discared from the history.
     */
    Ice::optional<int> sampleLifetime;
};

class ReaderConfig : public Config
{
public:

    /**
     * Specifies if and how samples are discarded after being received by a
     * reader.
     */
    Ice::optional<DiscardPolicy> discardPolicy;
};

class WriterConfig : public Config
{
public:

    /**
     * Specifies the writer priority. The priority is used by readers using
     * the priority discard policy.
     */
    Ice::optional<int> priority;
};

};
