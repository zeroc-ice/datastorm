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
        KeyReader<string, string> reader(topic, "elem1");

        reader.waitForWriters(1);
        test(reader.hasWriters());

        auto testSample = [&reader](SampleType type, string key, string value = "")
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == key);
            test(sample.getType() == type);
            if(type != SampleType::Remove)
            {
                test(sample.getValue() == value);
            }
        };

        testSample(SampleType::Add, "elem1", "value1");
        testSample(SampleType::Update, "elem1", "value2");
        testSample(SampleType::Remove, "elem1");

        auto samples = reader.getAll();
        test(samples.size() == 3);

        samples = reader.getAllUnread();
        test(samples.empty());
    }

    {
        Topic<int, Test::StructValue> topic(node, "struct");
        KeyReader<int, Test::StructValue> reader(topic, 10);

        reader.waitForWriters(1);
        test(reader.hasWriters());

        auto testSample = [&reader](SampleType type, Test::StructValue value = Test::StructValue())
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == 10);
            test(sample.getType() == type);
            if(type != SampleType::Remove)
            {
                test(sample.getValue() == value);
            }
        };

        testSample(SampleType::Add, Test::StructValue({"firstName", "lastName", 10}));
        testSample(SampleType::Update, Test::StructValue({"firstName", "lastName", 11}));
        testSample(SampleType::Remove);
    }

    {
        Topic<string, Test::BasePtr> topic(node, "baseclass");
        KeyReader<string, Test::BasePtr> reader(topic, "elem1");

        reader.waitForWriters(1);
        test(reader.hasWriters());

        auto testSample = [&reader](SampleType type, string value = "")
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == "elem1");
            test(sample.getType() == type);
            if(type != SampleType::Remove)
            {
                test(sample.getValue()->b == value);
            }
        };

        testSample(SampleType::Add, "value1");
        testSample(SampleType::Update, "value2");
        testSample(SampleType::Remove);
    }

    {
        Topic<string, Test::BasePtr> topic(node, "baseclass2");

        auto testSample = [&topic](auto& reader, SampleType type, string key, string value = "")
        {
            reader.waitForWriters(1);
            test(reader.hasWriters());

            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == key);
            test(sample.getType() == type);
            if(type != SampleType::Remove)
            {
                test(sample.getValue()->b == value);
            }
        };

        {
            KeyReader<string, Test::BasePtr> reader(topic, "elem1");
            testSample(reader, SampleType::Add, "elem1", "value1");
            testSample(reader, SampleType::Update, "elem1", "value2");
            testSample(reader, SampleType::Remove, "elem1");
        }
        {
            KeyReader<string, Test::BasePtr> reader(topic, "elem2");
            testSample(reader, SampleType::Update, "elem2", "value1");
        }
        {
            KeyReader<string, Test::BasePtr> reader(topic, "elem3");
            testSample(reader, SampleType::Remove, "elem3");
        }
        {
            KeyReader<string, Test::BasePtr> reader(topic, "elem4");
            testSample(reader, SampleType::Add, "elem4", "value1");
        }
    }

    {
        Topic<string, string> topic(node, "multikey1");
        {
            KeyReader<string, string> reader1(topic, "elem1");
            KeyReader<string, string> reader2(topic, "elem2");
            reader1.waitForUnread(3);
            reader2.waitForUnread(3);
            test(reader1.getAll().size() == 3);
            test(reader2.getAll().size() == 3);
        }
        {
            KeyReader<string, string> reader(topic, vector<string> { "elem1", "elem2" });
            reader.waitForUnread(6);
            test(reader.getAll().size() == 6);
        }
    }

    {
        Topic<string, Test::BasePtr> topic(node, "baseclass3");

        FilteredReader<string, Test::BasePtr> reader(topic, "elem[0-9]");

        reader.waitForWriters(1);
        test(reader.hasWriters());

        auto testSample = [&reader](SampleType type, string key, string value = "")
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == key);
            test(sample.getType() == type);
            if(type != SampleType::Remove)
            {
                test(sample.getValue()->b == value);
            }
        };

        testSample(SampleType::Add, "elem1", "value1");
        testSample(SampleType::Update, "elem1", "value2");
        testSample(SampleType::Remove, "elem1");

        testSample(SampleType::Update, "elem2", "value1");
        testSample(SampleType::Remove, "elem3");
        testSample(SampleType::Add, "elem4", "value1");
    }
    {
        Topic<string, string, RegexKeyValueFilter<string, string>> topic(node, "filtered reader key/value filter");

        auto testSample = [](auto& reader, SampleType type, string key, string value = "")
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            test(sample.getKey() == key);
            test(sample.getType() == type);
            if(type != SampleType::Remove)
            {
                test(sample.getValue() == value);
            }
        };

        FilteredReader<string, string> reader11(topic, { "elem[1]", "value[0-9]", { SampleType::Add }});
        FilteredReader<string, string> reader12(topic, { "elem[1]", "value[0-9]", { SampleType::Update }});
        FilteredReader<string, string> reader13(topic, { "elem[1]", "", { SampleType::Remove }});
        testSample(reader11, SampleType::Add, "elem1", "value1");
        testSample(reader12, SampleType::Update, "elem1", "value2");
        testSample(reader13, SampleType::Remove, "elem1");

        FilteredReader<string, string> reader2(topic, { "elem2", "value[2-4]"});
        testSample(reader2, SampleType::Update, "elem2", "value2");
        testSample(reader2, SampleType::Update, "elem2", "value3");
        testSample(reader2, SampleType::Update, "elem2", "value4");
     }

    // {
    //     Topic<string, Test::BasePtr> topic(node, "baseclass4");

    //     auto testSample = [&topic](SampleType type, auto& reader, string key, string value = "")
    //     {
    //         reader.waitForUnread(1);
    //         auto sample = reader.getNextUnread();
    //         //test(sample.getKey() == key); No key set for filtered writer
    //         test(sample.getKey().empty());
    //         test(sample.getType() == type);
    //         if(type != SampleType::Remove)
    //         {
    //             test(sample.getValue()->b == value);
    //         }
    //     };

    //     {
    //         KeyReader<string, Test::BasePtr> reader(topic, "elema1");
    //         reader.waitForWriters(1);
    //         test(reader.hasWriters());
    //         testSample(SampleType::Add, reader, "elema1", "value1");
    //         testSample(SampleType::Update, reader, "elema1", "value2");
    //         testSample(SampleType::Remove, reader, "elema1");
    //     }
    //     {
    //         KeyReader<string, Test::BasePtr> reader(topic, "elemb2");
    //         reader.waitForWriters(1);
    //         test(reader.hasWriters());
    //         testSample(SampleType::Update, reader, "elemb2", "value1");
    //     }
    //     {
    //         KeyReader<string, Test::BasePtr> reader(topic, "elemc3");
    //         reader.waitForWriters(1);
    //         test(reader.hasWriters());
    //         testSample(SampleType::Remove, reader, "elemc3");
    //     }
    //     {
    //         KeyReader<string, Test::BasePtr> reader(topic, "elemd4");
    //         reader.waitForWriters(1);
    //         test(reader.hasWriters());
    //         testSample(SampleType::Add, reader, "elemd4", "value1");
    //     }

    //     {
    //         vector<KeyReader<string, Test::BasePtr>> readers;
    //         for(int i = 0; i < 5; ++i)
    //         {
    //             ostringstream os;
    //             os << "elem" << i;
    //             readers.push_back(KeyReader<string, Test::BasePtr>(topic, os.str()));
    //             readers.back().waitForWriters(1);
    //         }
    //         for(int i = 0; i < 5; ++i)
    //         {
    //             ostringstream os;
    //             os << "elem" << i;
    //             testSample(SampleType::Update, readers[i], os.str(), "value1");
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
