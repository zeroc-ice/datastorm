// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
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

namespace DataStorm
{

template<> struct DataTraits<Test::StructValue>
{
    using KeyType = int;
    using ValueType = Test::StructValue;
    using FilterType = std::string;
};

}

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    {
        TopicReader<string> topic(node, "string");
        KeyDataReader<string> reader(topic, "elem1");

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
        TopicReader<Test::StructValue> topic(node, "struct");
        KeyDataReader<Test::StructValue> reader(topic, 10);

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
        TopicReader<Test::BasePtr> topic(node, "baseclass");
        KeyDataReader<Test::BasePtr> reader(topic, "elem1");

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
        TopicReader<Test::BasePtr> topic(node, "baseclass2");

        auto testSample = [&topic](SampleType type, string key, string value = "")
        {
            KeyDataReader<Test::BasePtr> reader(topic, key);

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

        testSample(SampleType::Add, "elem1", "value1");
        testSample(SampleType::Update, "elem1", "value2");
        testSample(SampleType::Remove, "elem1");

        testSample(SampleType::Update, "elem2", "value1");
        testSample(SampleType::Remove, "elem3");
        testSample(SampleType::Add, "elem4", "value1");
    }

    {
        TopicReader<Test::BasePtr> topic(node, "baseclass3");

        FilteredDataReader<Test::BasePtr> reader(topic, "elem[0-9]");

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
        TopicReader<Test::BasePtr> topic(node, "baseclass4");

        auto testSample = [&topic](SampleType type,
                                   DataReader<Test::BasePtr>& reader,
                                   string key,
                                   string value = "")
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
            KeyDataReader<Test::BasePtr> reader(topic, "elema1");
            reader.waitForWriters(1);
            test(reader.hasWriters());
            testSample(SampleType::Add, reader, "elema1", "value1");
            testSample(SampleType::Update, reader, "elema1", "value2");
            testSample(SampleType::Remove, reader, "elema1");
        }
        {
            KeyDataReader<Test::BasePtr> reader(topic, "elemb2");
            reader.waitForWriters(1);
            test(reader.hasWriters());
            testSample(SampleType::Update, reader, "elemb2", "value1");
        }
        {
            KeyDataReader<Test::BasePtr> reader(topic, "elemc3");
            reader.waitForWriters(1);
            test(reader.hasWriters());
            testSample(SampleType::Remove, reader, "elemc3");
        }
        {
            KeyDataReader<Test::BasePtr> reader(topic, "elemd4");
            reader.waitForWriters(1);
            test(reader.hasWriters());
            testSample(SampleType::Add, reader, "elemd4", "value1");
        }

        // vector<KeyDataReader<Test::BasePtr>> readers;
        // for(int i = 0; i < 5; ++i)
        // {
        //     ostringstream os;
        //     os << "elem" << i;
        //     readers.push_back(KeyDataReader<Test::BasePtr>(topic, os.str()));
        //     readers.back().waitForWriters(1);
        // }
        // for(int i = 0; i < 5; ++i)
        // {
        //     ostringstream os;
        //     os << "elem" << i;
        //     testSample(SampleType::Update, readers[i], os.str(), "value1");
        // }
    }

    return 0;
}
