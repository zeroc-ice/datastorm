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

sequence<byte> ByteSeq;
sequence<ByteSeq> ByteSeqSeq;
sequence<long> LongSeq;

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

sequence<string> StringSeq;

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

interface Publisher;

interface Peer
{
}

interface Subscriber extends Peer
{
    ["amd"] SubscriberSession* createSession(Publisher* publisher, PublisherSession* session);
}

interface Publisher extends Peer
{
    ["amd"] PublisherSession* createSession(Subscriber* subscriber, SubscriberSession* session);
}

interface TopicLookup
{
    idempotent void announceTopicPublisher(string topic, Publisher* publisher);

    idempotent void announceTopicSubscriber(string topic, Subscriber* subscriber);
}

}

