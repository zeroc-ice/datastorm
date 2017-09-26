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

int
main(int argc, char* argv[])
{
    auto factory = DataStorm::createTopicFactory(argc, argv);

    {
        auto topic = factory->createTopicReader<string, string>("string");
        auto reader = topic->getDataReader("elem1");

        reader->waitForWriters(1);
        test(reader->hasWriters());

        auto testSample = [&reader](SampleType type, string key, string value = "")
        {
            reader->waitForUnread(1);
            auto sample = reader->getNextUnread();
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

        auto samples = reader->getAll();
        test(samples.size() == 3);

        samples = reader->getAllUnread();
        test(samples.empty());

        reader->destroy();
    }

    {
        auto topic = factory->createTopicReader<int, Test::StructValue>("struct");
        auto reader = topic->getDataReader(10);

        reader->waitForWriters(1);
        test(reader->hasWriters());

        auto testSample = [&reader](SampleType type, Test::StructValue value = Test::StructValue())
        {
            reader->waitForUnread(1);
            auto sample = reader->getNextUnread();
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

        reader->destroy();
    }

    {
        auto topic = factory->createTopicReader<string, Test::BasePtr>("baseclass");
        auto reader = topic->getDataReader("elem1");

        reader->waitForWriters(1);
        test(reader->hasWriters());

        auto testSample = [&reader](SampleType type, string value = "")
        {
            reader->waitForUnread(1);
            auto sample = reader->getNextUnread();
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

        reader->destroy();
    }

    {
        auto topic = factory->createTopicReader<string, Test::BasePtr>("baseclass2");

        auto testSample = [&topic](SampleType type, string key, string value = "")
        {
            auto reader = topic->getDataReader(key);

            reader->waitForWriters(1);
            test(reader->hasWriters());

            reader->waitForUnread(1);
            auto sample = reader->getNextUnread();
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

        topic->getDataReader("elem1")->destroy();
        topic->getDataReader("elem2")->destroy();
        topic->getDataReader("elem3")->destroy();
        topic->getDataReader("elem4")->destroy();
    }

    {
        auto topic = factory->createTopicReader<string, Test::BasePtr>("baseclass3");

        auto reader = topic->getFilteredDataReader("elem[0-9]");

        reader->waitForWriters(1);
        test(reader->hasWriters());

        auto testSample = [&reader](SampleType type, string key, string value = "")
        {
            reader->waitForUnread(1);
            auto sample = reader->getNextUnread();
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

        reader->destroy();
    }

    {
        auto topic = factory->createTopicReader<string, Test::BasePtr>("baseclass4");

        auto testSample = [&topic](SampleType type,
                                   shared_ptr<DataReader<pair<string, Test::BasePtr>>> reader,
                                   string key,
                                   string value = "")
        {
            reader->waitForUnread(1);
            auto sample = reader->getNextUnread();
            test(sample.getKey() == key);
            test(sample.getType() == type);
            if(type != SampleType::Remove)
            {
                test(sample.getValue()->b == value);
            }
        };

        auto reader = topic->getDataReader("elema1");
        reader->waitForWriters(1);
        test(reader->hasWriters());
        testSample(SampleType::Add, reader, "elema1", "value1");
        testSample(SampleType::Update, reader, "elema1", "value2");
        testSample(SampleType::Remove, reader, "elema1");
        reader->destroy();

        reader = topic->getDataReader("elemb2");
        reader->waitForWriters(1);
        test(reader->hasWriters());
        testSample(SampleType::Update, reader, "elemb2", "value1");
        reader->destroy();

        reader = topic->getDataReader("elemc3");
        reader->waitForWriters(1);
        test(reader->hasWriters());
        testSample(SampleType::Remove, reader, "elemc3");
        reader->destroy();

        reader = topic->getDataReader("elemd4");
        reader->waitForWriters(1);
        test(reader->hasWriters());
        testSample(SampleType::Add, reader, "elemd4", "value1");
        reader->destroy();

        for(int i = 0; i < 5; ++i)
        {
            ostringstream os;
            os << "elem" << i;
            reader = topic->getDataReader(os.str());
            reader->waitForWriters(1);
        }
        for(int i = 0; i < 5; ++i)
        {
            ostringstream os;
            os << "elem" << i;
            testSample(SampleType::Update, topic->getDataReader(os.str()), os.str(), "value1");
        }
        for(int i = 0; i < 5; ++i)
        {
            ostringstream os;
            os << "elem" << i;
            topic->getDataReader(os.str())->destroy();
        }
    }

    factory->destroy();
    return 0;
}
