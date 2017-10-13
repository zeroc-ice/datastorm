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
        TopicReader<string, string> topic(node, "string");
        KeyDataReader<string, string> reader(topic, "elem1");

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
        TopicReader<int, Test::StructValue> topic(node, "struct");
        KeyDataReader<int, Test::StructValue> reader(topic, 10);

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
        TopicReader<string, Test::BasePtr> topic(node, "baseclass");
        KeyDataReader<string, Test::BasePtr> reader(topic, "elem1");

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
        TopicReader<string, Test::BasePtr> topic(node, "baseclass2");

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
            KeyDataReader<string, Test::BasePtr> reader(topic, "elem1");
            testSample(reader, SampleType::Add, "elem1", "value1");
            testSample(reader, SampleType::Update, "elem1", "value2");
            testSample(reader, SampleType::Remove, "elem1");
        }
        {
            KeyDataReader<string, Test::BasePtr> reader(topic, "elem2");
            testSample(reader, SampleType::Update, "elem2", "value1");
        }
        {
            KeyDataReader<string, Test::BasePtr> reader(topic, "elem3");
            testSample(reader, SampleType::Remove, "elem3");
        }
        {
            KeyDataReader<string, Test::BasePtr> reader(topic, "elem4");
            testSample(reader, SampleType::Add, "elem4", "value1");
        }
    }

    {
        TopicReader<string, Test::BasePtr> topic(node, "baseclass3");

        FilteredDataReader<string, Test::BasePtr> reader(topic, "elem[0-9]");

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
        TopicReader<string, string, RegexKeyValueFilter<string, string>> topic(node, "filtered reader key/value filter");

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

        FilteredDataReader<string, string> reader11(topic, { "elem[1]", "value[0-9]", { SampleType::Add }});
        FilteredDataReader<string, string> reader12(topic, { "elem[1]", "value[0-9]", { SampleType::Update }});
        FilteredDataReader<string, string> reader13(topic, { "elem[1]", "", { SampleType::Remove }});
        testSample(reader11, SampleType::Add, "elem1", "value1");
        testSample(reader12, SampleType::Update, "elem1", "value2");
        testSample(reader13, SampleType::Remove, "elem1");

        FilteredDataReader<string, string> reader2(topic, { "elem2", "value[2-4]"});
        testSample(reader2, SampleType::Update, "elem2", "value2");
        testSample(reader2, SampleType::Update, "elem2", "value3");
        testSample(reader2, SampleType::Update, "elem2", "value4");
     }

    {
        TopicReader<string, Test::BasePtr> topic(node, "baseclass4");

        auto testSample = [&topic](SampleType type, auto& reader, string key, string value = "")
        {
            reader.waitForUnread(1);
            auto sample = reader.getNextUnread();
            //test(sample.getKey() == key); No key set for filtered writer
            test(sample.getKey().empty());
            test(sample.getType() == type);
            if(type != SampleType::Remove)
            {
                test(sample.getValue()->b == value);
            }
        };

        {
            KeyDataReader<string, Test::BasePtr> reader(topic, "elema1");
            reader.waitForWriters(1);
            test(reader.hasWriters());
            testSample(SampleType::Add, reader, "elema1", "value1");
            testSample(SampleType::Update, reader, "elema1", "value2");
            testSample(SampleType::Remove, reader, "elema1");
        }
        {
            KeyDataReader<string, Test::BasePtr> reader(topic, "elemb2");
            reader.waitForWriters(1);
            test(reader.hasWriters());
            testSample(SampleType::Update, reader, "elemb2", "value1");
        }
        {
            KeyDataReader<string, Test::BasePtr> reader(topic, "elemc3");
            reader.waitForWriters(1);
            test(reader.hasWriters());
            testSample(SampleType::Remove, reader, "elemc3");
        }
        {
            KeyDataReader<string, Test::BasePtr> reader(topic, "elemd4");
            reader.waitForWriters(1);
            test(reader.hasWriters());
            testSample(SampleType::Add, reader, "elemd4", "value1");
        }

        {
            vector<KeyDataReader<string, Test::BasePtr>> readers;
            for(int i = 0; i < 5; ++i)
            {
                ostringstream os;
                os << "elem" << i;
                readers.push_back(KeyDataReader<string, Test::BasePtr>(topic, os.str()));
                readers.back().waitForWriters(1);
            }
            for(int i = 0; i < 5; ++i)
            {
                ostringstream os;
                os << "elem" << i;
                testSample(SampleType::Update, readers[i], os.str(), "value1");
            }
        }

        topic.waitForNoWriters();
    }

    return 0;
}
