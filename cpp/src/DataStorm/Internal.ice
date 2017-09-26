

module DataStormContract
{

enum SampleType
{
    Add,
    Update,
    Remove
};

sequence<byte> Key;
sequence<Key> KeySeq;

sequence<byte> ByteSeq;

class DataSample
{
    long id;
    long timestamp;
    SampleType type;
    ByteSeq value;
};
sequence<DataSample> DataSampleSeq;

class DataSamples
{
    Key key;
    DataSampleSeq samples;
};
sequence<DataSamples> DataSamplesSeq;

sequence<string> StringSeq;

struct KeyInfo
{
    Key key;
    int priority;
};
sequence<KeyInfo> KeyInfoSeq;

interface Session
{
    void initTopics(StringSeq topics);
    void addTopic(string topic);
    void removeTopic(string topic);

    void initKeysAndFilters(string topic, long lastId, KeyInfoSeq keys, StringSeq filters);

    void attachKey(string topic, long lastId, KeyInfo key);
    void detachKey(string topic, Key key);

    void attachFilter(string topic, long lastId, string filter);
    void detachFilter(string topic, string filter);

    void destroy();
};

interface PublisherSession extends Session
{
};

interface SubscriberSession extends Session
{
    void i(string topic, DataSamplesSeq samples);
    void s(string topic, Key key, DataSample sample);
    void f(string topic, string filter, DataSample sample);
};

interface Publisher;

interface Peer
{
};

interface Subscriber extends Peer
{
    ["amd"] SubscriberSession* createSession(Publisher* publisher, PublisherSession* session);
};

interface Publisher extends Peer
{
    ["amd"] PublisherSession* createSession(Subscriber* subscriber, SubscriberSession* session);
};

interface TopicLookup
{
    idempotent void announceTopicPublisher(string topic, Publisher* publisher);

    idempotent void announceTopicSubscriber(string topic, Subscriber* subscriber);
};

};

