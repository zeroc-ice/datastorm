// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/Sample.ice>

module DataStormContract
{

enum ClearHistoryPolicy
{
    Add,
    Remove,
    AddOrRemove,
    Never
};

/** A sequence of bytes use to hold the encoded key or value */
sequence<byte> ByteSeq;

/** A sequence of long */
sequence<long> LongSeq;

struct DataSample
{
    long id;
    long timestamp;
    long tag;
    DataStorm::SampleEvent event;
    ByteSeq value;
}
["cpp:type:std::deque<DataSample>"] sequence<DataSample> DataSampleSeq;

struct DataSamples
{
    long id;
    DataSampleSeq samples;
}
sequence<DataSamples> DataSamplesSeq;

struct ElementInfo
{
    long valueId;
    ByteSeq value;
}
sequence<ElementInfo> ElementInfoSeq;

struct TopicInfo
{
    string name;
    LongSeq ids;
}
sequence<TopicInfo> TopicInfoSeq;

struct TopicSpec
{
    long id;
    string name;
    ElementInfoSeq elements;
    ElementInfoSeq tags;
};

class ElementConfig(1)
{
    optional(1) string facet;
    optional(2) ByteSeq sampleFilter;
    optional(10) int sampleCount;
    optional(11) int sampleLifetime;
    optional(12) ClearHistoryPolicy clearHistory;
};

struct ElementData
{
    long id;
    ElementConfig config;
}
sequence<ElementData> ElementDataSeq;

struct ElementSpec
{
    ElementDataSeq elements;
    long valueId;
    ByteSeq value;
    long peerValueId;
}
sequence<ElementSpec> ElementSpecSeq;

struct ElementDataAck
{
    long id;
    ElementConfig config;
    DataSampleSeq samples;
    long peerId;
}
sequence<ElementDataAck> ElementDataAckSeq;

struct ElementSpecAck
{
    ElementDataAckSeq elements;
    long valueId;
    ByteSeq value;
    long peerValueId;
}
sequence<ElementSpecAck> ElementSpecAckSeq;

interface Session
{
    void announceTopics(TopicInfoSeq topics);
    void attachTopic(TopicSpec topic);
    void detachTopic(long topic);

    void attachTags(long topic, ElementInfoSeq tags);
    void detachTags(long topic, LongSeq tags);

    void announceElements(long topic, ElementInfoSeq keys);
    void attachElements(long topic, ElementSpecSeq elements);
    void attachElementsAck(long topic, ElementSpecAckSeq elements);
    void detachElements(long topic, LongSeq keys);

    void initSamples(long topic, DataSamplesSeq samples);

    void destroy();
}

interface PublisherSession extends Session
{
}

interface SubscriberSession extends Session
{
    void s(long topic, long element, DataSample sample);
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
