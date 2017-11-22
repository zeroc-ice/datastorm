// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

#include <Test.h>
#include <TestCommon.h>

using namespace DataStorm;
using namespace std;

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    ReaderConfig config;
    config.sampleCount = -1; // Unlimited sample count
    config.clearHistory = ClearHistoryPolicy::Never;

    {
        Topic<string, string> topic(node, "string");
        {
            auto reader = makeSingleKeyReader(topic, "elem1", config);

            reader.waitForWriters(1);
            test(reader.hasWriters());

            auto testSample = [&reader](SampleEvent event, string key, string value = "")
            {
                reader.waitForUnread(1);
                auto sample = reader.getNextUnread();
                test(sample.getKey() == key);
                test(sample.getEvent() == event);
                if(event != SampleEvent::Remove)
                {
                    test(sample.getValue() == value);
                }
            };

            testSample(SampleEvent::Add, "elem1", "value1");
            testSample(SampleEvent::Update, "elem1", "value2");
            testSample(SampleEvent::Remove, "elem1");

            auto samples = reader.getAll();
            test(samples.size() == 3);

            samples = reader.getAllUnread();
            test(samples.empty());
        }
        {
            auto reader1 = makeSingleKeyReader(topic, "elem2", config);
            auto reader2 = makeSingleKeyReader(topic, "elem2", config);
            reader1.waitForWriters(1);
            reader2.waitForWriters(1);
            reader1.waitForUnread();
            reader2.waitForUnread();
        }
    }

    {
        Topic<int, Test::StructValue> topic(node, "struct");
        auto reader = makeSingleKeyReader(topic, 10, config);

        reader.waitForWriters(1);
        test(reader.hasWriters());

        auto testSample = [&reader](SampleEvent event, Test::StructValue value = Test::StructValue())
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == 10);
            test(sample.getEvent() == event);
            if(event != SampleEvent::Remove)
            {
                test(sample.getValue() == value);
            }
        };

        testSample(SampleEvent::Add, Test::StructValue({"firstName", "lastName", 10}));
        testSample(SampleEvent::Update, Test::StructValue({"firstName", "lastName", 11}));
        testSample(SampleEvent::Remove);
    }

    {
        Topic<string, shared_ptr<Test::Base>> topic(node, "baseclass");
        auto reader = makeSingleKeyReader(topic, "elem1", config);

        reader.waitForWriters(1);
        test(reader.hasWriters());

        auto testSample = [&reader](SampleEvent event, string value = "")
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == "elem1");
            test(sample.getEvent() == event);
            if(event != SampleEvent::Remove)
            {
                test(sample.getValue()->b == value);
            }
        };

        testSample(SampleEvent::Add, "value1");
        testSample(SampleEvent::Update, "value2");
        testSample(SampleEvent::Remove);
    }

    {
        Topic<string, shared_ptr<Test::Base>> topic(node, "baseclass2");

        auto testSample = [&topic](typename decltype(topic)::ReaderType& reader, SampleEvent event,
                                   string key, string value = "")
        {
            reader.waitForWriters(1);
            test(reader.hasWriters());

            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == key);
            test(sample.getEvent() == event);
            if(event != SampleEvent::Remove)
            {
                test(sample.getValue()->b == value);
            }
        };

        {
            auto reader = makeSingleKeyReader(topic, "elem1", config);
            testSample(reader, SampleEvent::Add, "elem1", "value1");
            testSample(reader, SampleEvent::Update, "elem1", "value2");
            testSample(reader, SampleEvent::Remove, "elem1");
        }
        {
            auto reader = makeSingleKeyReader(topic, "elem2", config);
            testSample(reader, SampleEvent::Update, "elem2", "value1");
        }
        {
            auto reader = makeSingleKeyReader(topic, "elem3", config);
            testSample(reader, SampleEvent::Remove, "elem3");
        }
        {
            auto reader = makeSingleKeyReader(topic, "elem4", config);
            testSample(reader, SampleEvent::Add, "elem4", "value1");
        }
    }

    {
        Topic<string, string> topic(node, "multikey1");

        auto reader = makeMultiKeyReader(topic, { "elem1", "elem2" }, config);
        reader.waitForWriters(2);
        reader.waitForUnread(6);
        test(reader.getAll().size() == 6);
    }

    {
        Topic<string, string> topic(node, "anykey");

        auto reader = makeAnyKeyReader(topic, config);
        reader.waitForWriters(2);
        reader.waitForUnread(6);
        test(reader.getAll().size() == 6);
    }

    {
        Topic<string, shared_ptr<Test::Base>, RegexFilter<string>, string> topic(node, "baseclass3");

        auto reader = makeFilteredReader(topic, "elem[0-4]", config);

        reader.waitForWriters(1);
        test(reader.hasWriters());

        auto testSample = [&reader](SampleEvent event, string key, string value = "")
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == key);
            test(sample.getEvent() == event);
            if(event != SampleEvent::Remove)
            {
                test(sample.getValue()->b == value);
            }
        };

        testSample(SampleEvent::Add, "elem1", "value1");
        testSample(SampleEvent::Update, "elem1", "value2");
        testSample(SampleEvent::Remove, "elem1");

        testSample(SampleEvent::Update, "elem2", "value1");
        testSample(SampleEvent::Remove, "elem3");
        testSample(SampleEvent::Add, "elem4", "value1");
    }
    {
        Topic<string, string, RegexFilter<string>, string> topic(node, "filtered reader key/value filter");
        {
            auto testSample = [](typename decltype(topic)::ReaderType& reader,
                                 SampleEvent event,
                                 string key,
                                 string value = "")
            {
                reader.waitForUnread(1);
                auto sample = reader.getNextUnread();
                test(sample.getKey() == key);
                test(sample.getEvent() == event);
                if(event != SampleEvent::Remove)
                {
                    test(sample.getValue() == value);
                }
            };

            auto reader11 = makeFilteredReader(topic, "elem[1]", vector<SampleEvent> { SampleEvent::Add }, config);
            auto reader12 = makeFilteredReader(topic, "elem[1]", vector<SampleEvent> { SampleEvent::Update }, config);
            auto reader13 = makeFilteredReader(topic, "elem[1]", vector<SampleEvent> { SampleEvent::Remove }, config);
            testSample(reader11, SampleEvent::Add, "elem1", "value1");
            testSample(reader12, SampleEvent::Update, "elem1", "value2");
            testSample(reader13, SampleEvent::Remove, "elem1");
        }
        {
            auto testSample = [](typename decltype(topic)::ReaderType& reader,
                                 SampleEvent event,
                                 string key,
                                 string value = "")
            {
                reader.waitForUnread(1);
                auto sample = reader.getNextUnread();
                test(sample.getKey() == key);
                test(sample.getEvent() == event);
                if(event != SampleEvent::Remove)
                {
                    test(sample.getValue() == value);
                }
            };

            auto reader2 = makeFilteredReader(topic, "elem[2]", "value[2-4]", config);
            testSample(reader2, SampleEvent::Update, "elem2", "value2");
            testSample(reader2, SampleEvent::Update, "elem2", "value3");
            testSample(reader2, SampleEvent::Update, "elem2", "value4");
        }
     }

     {
        Topic<string, string> t1(node, "topic");
        Topic<string, string> t2(node, "topic");
        t1.hasWriters(); // Required to create the underlying topic reader
        t2.hasWriters(); // Required to create the underlying topic reader
        t1.waitForWriters(2);
        t2.waitForWriters(2);

        auto reader = makeSingleKeyReader(t1, "shutdown", config);
        reader.waitForUnread();
    }
    return 0;
}
