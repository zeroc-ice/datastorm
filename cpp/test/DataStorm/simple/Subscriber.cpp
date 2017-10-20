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

    {
        Topic<string, string> topic(node, "string");
        {
            KeyReader<string, string> reader(topic, "elem1");

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
            KeyReader<string, string> reader1(topic, "elem2");
            KeyReader<string, string> reader2(topic, "elem2");
            reader1.waitForWriters(1);
            reader2.waitForWriters(1);
            reader1.waitForUnread();
            reader2.waitForUnread();
        }
    }

    {
        Topic<int, Test::StructValue> topic(node, "struct");
        KeyReader<int, Test::StructValue> reader(topic, 10);

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
        KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elem1");

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

        auto testSample = [&topic](auto& reader, SampleEvent event, string key, string value = "")
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
            KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elem1");
            testSample(reader, SampleEvent::Add, "elem1", "value1");
            testSample(reader, SampleEvent::Update, "elem1", "value2");
            testSample(reader, SampleEvent::Remove, "elem1");
        }
        {
            KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elem2");
            testSample(reader, SampleEvent::Update, "elem2", "value1");
        }
        {
            KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elem3");
            testSample(reader, SampleEvent::Remove, "elem3");
        }
        {
            KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elem4");
            testSample(reader, SampleEvent::Add, "elem4", "value1");
        }
    }

    {
        Topic<string, string> topic(node, "multikey1");

        KeyReader<string, string> reader(topic, vector<string> { "elem1", "elem2" });
        reader.waitForWriters(2);
        reader.waitForUnread(6);
        test(reader.getAll().size() == 6);
    }

    {
        Topic<string, shared_ptr<Test::Base>, RegexFilter<string>, string> topic(node, "baseclass3");

        FilteredReader<string, shared_ptr<Test::Base>> reader(topic, "elem[0-9]");

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

        auto testSample = [](auto& reader, SampleEvent event, string key, string value = "")
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

        FilteredReader<string, string, SampleEventSeq> reader11(topic, "elem[1]", { SampleEvent::Add });
        FilteredReader<string, string, SampleEventSeq> reader12(topic, "elem[1]", { SampleEvent::Update });
        FilteredReader<string, string, SampleEventSeq> reader13(topic, "elem[1]", { SampleEvent::Remove });
        testSample(reader11, SampleEvent::Add, "elem1", "value1");
        testSample(reader12, SampleEvent::Update, "elem1", "value2");
        testSample(reader13, SampleEvent::Remove, "elem1");

        FilteredReader<string, string, string> reader2(topic, "elem[2]", "value[2-4]");
        testSample(reader2, SampleEvent::Update, "elem2", "value2");
        testSample(reader2, SampleEvent::Update, "elem2", "value3");
        testSample(reader2, SampleEvent::Update, "elem2", "value4");
     }

    // {
    //     Topic<string, shared_ptr<Test::Base>> topic(node, "baseclass4");

    //     auto testSample = [&topic](SampleEvent event, auto& reader, string key, string value = "")
    //     {
    //         reader.waitForUnread(1);
    //         auto sample = reader.getNextUnread();
    //         //test(sample.getKey() == key); No key set for filtered writer
    //         test(sample.getKey().empty());
    //         test(sample.getEvent() == event);
    //         if(event != SampleEvent::Remove)
    //         {
    //             test(sample.getValue()->b == value);
    //         }
    //     };

    //     {
    //         KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elema1");
    //         reader.waitForWriters(1);
    //         test(reader.hasWriters());
    //         testSample(SampleEvent::Add, reader, "elema1", "value1");
    //         testSample(SampleEvent::Update, reader, "elema1", "value2");
    //         testSample(SampleEvent::Remove, reader, "elema1");
    //     }
    //     {
    //         KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elemb2");
    //         reader.waitForWriters(1);
    //         test(reader.hasWriters());
    //         testSample(SampleEvent::Update, reader, "elemb2", "value1");
    //     }
    //     {
    //         KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elemc3");
    //         reader.waitForWriters(1);
    //         test(reader.hasWriters());
    //         testSample(SampleEvent::Remove, reader, "elemc3");
    //     }
    //     {
    //         KeyReader<string, shared_ptr<Test::Base>> reader(topic, "elemd4");
    //         reader.waitForWriters(1);
    //         test(reader.hasWriters());
    //         testSample(SampleEvent::Add, reader, "elemd4", "value1");
    //     }

    //     {
    //         vector<KeyReader<string, shared_ptr<Test::Base>>> readers;
    //         for(int i = 0; i < 5; ++i)
    //         {
    //             ostringstream os;
    //             os << "elem" << i;
    //             readers.push_back(KeyReader<string, shared_ptr<Test::Base>>(topic, os.str()));
    //             readers.back().waitForWriters(1);
    //         }
    //         for(int i = 0; i < 5; ++i)
    //         {
    //             ostringstream os;
    //             os << "elem" << i;
    //             testSample(SampleEvent::Update, readers[i], os.str(), "value1");
    //         }
    //     }

    //     topic.waitForNoWriters();
    // }

    {
        Topic<string, string> t1(node, "topic");
        Topic<string, string> t2(node, "topic");
        t1.hasWriters(); // Required to create the underlying topic reader
        t2.hasWriters(); // Required to create the underlying topic reader
        t1.waitForWriters(2);
        t2.waitForWriters(2);

        KeyReader<string, string> reader(t1, "shutdown");
        reader.waitForUnread();
    }
    return 0;
}
