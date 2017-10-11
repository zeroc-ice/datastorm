// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/SampleType.ice>

module DataStormContract
{

/** A sequence of bytes use to hold the encoded key or value */
sequence<byte> ByteSeq;

/** A sequence of long */
sequence<long> LongSeq;

/** The quality of service struct exchanged between nodes on data reader/writer creation. */
class QoS
{
    /** Use a datagram connection to exchange messages between the writer and reader. */
    bool datagram;

    /** Use a secure connection to exchange messages between the writer and reader. */
    bool secure;

    /**
     * Buffers the messages on the data writer or reader and flush the buffer after
     * a configurable sleep time.
     */
    bool buffered;

    /** Specifies how often the buffered messages are flushed in ms. */
    int flushTime;
}

struct DataSample
{
    long id;
    long timestamp;
    DataStorm::SampleType type;
    ByteSeq value;
}
sequence<DataSample> DataSampleSeq;

struct DataSamples
{
    long key;
    DataSampleSeq samples;
}
sequence<DataSamples> DataSamplesSeq;

struct TopicInfo
{
    long id;
    string name;
}
sequence<TopicInfo> TopicInfoSeq;

struct KeyInfo
{
    long id;
    ByteSeq key;
}
sequence<KeyInfo> KeyInfoSeq;

struct KeyInfoAndSamples
{
    KeyInfo info;
    DataSampleSeq samples;
}
sequence<KeyInfoAndSamples> KeyInfoAndSamplesSeq;

struct FilterInfo
{
    long id;
    ByteSeq filter;
}
sequence<FilterInfo> FilterInfoSeq;

struct TopicInfoAndContent
{
    long id;
    string name;
    long lastId;
    KeyInfoSeq keys;
    FilterInfoSeq filters;
}
sequence<TopicInfoAndContent> TopicInfoAndContentSeq;

interface Session
{
    void announceTopics(TopicInfoSeq topics);
    void attachTopics(TopicInfoAndContentSeq topics);
    void detachTopic(long topic);

    void announceKeys(long topic, KeyInfoSeq keys);
    void announceFilter(long topic, FilterInfo filter);
    void attachKeysAndFilters(long topic, long lastId, KeyInfoAndSamplesSeq keys, FilterInfoSeq filters);
    void detachKeys(long topic, LongSeq keys);
    void detachFilter(long topic, long filter);

    void destroy();
}

interface PublisherSession extends Session
{
}

interface SubscriberSession extends Session
{
    void i(long topic, DataSamplesSeq samples);
    void s(long topic, long key, DataSample sample);
    void f(long topic, long filter, DataSample sample);
}

interface Node
{
    ["amd"] SubscriberSession* createSubscriberSession(Node* publisher, PublisherSession* session);
    ["amd"] PublisherSession* createPublisherSession(Node* subscriber, SubscriberSession* session);
}

interface TopicLookup
{
    idempotent void announceTopicReader(string topic, Node* node);

    idempotent void announceTopicWriter(string topic, Node* node);
}

}

