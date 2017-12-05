// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

#include <TestCommon.h>

using namespace DataStorm;
using namespace std;

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    Topic<string, string> topic(node, "topic");
    Topic<string, bool> controller(node, "controller");

    auto readers = makeSingleKeyWriter(controller, "readers");
    auto writers = makeSingleKeyReader(controller, "writers");

    {
        ReaderConfig config;
        config.sampleCount = -1;
        config.clearHistory = ClearHistoryPolicy::Never;
        topic.setReaderDefaultConfig(config);
    }

    writers.waitForWriters();

    // Writer sample count
    {
        auto read = [&topic, &writers, &readers](int count)
        {
            while(!writers.getNextUnread().getValue()); // Wait for writer to write the samples before reading

            readers.update(false);

            auto reader = makeSingleKeyReader(topic, "elem1");
            test(count < 6 || reader.getNextUnread().getValue() == "value1");
            test(count < 5 || reader.getNextUnread().getValue() == "value2");
            test(count < 4 || reader.getNextUnread().getEvent() == SampleEvent::Remove);
            test(count < 3 || reader.getNextUnread().getValue() == "value3");
            test(count < 2 || reader.getNextUnread().getValue() == "value4");
            test(count < 1 || reader.getNextUnread().getEvent() == SampleEvent::Remove);
            readers.update(true); // Reader is done
        };

        read(6); // Writer keeps all the samples
        read(4); // Writer keeps 4 samples
        read(3); // Writer keeps last instance samples
    }

    // Reader sample count
    {
        auto read = [&topic, &writers, &readers](int count, ReaderConfig config)
        {
            while(!writers.getNextUnread().getValue()); // Wait for writer to write the samples before reading

            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem1", config);
            test(count < 6 || reader.getNextUnread().getValue() == "value1");
            test(count < 5 || reader.getNextUnread().getValue() == "value2");
            test(count < 4 || reader.getNextUnread().getEvent() == SampleEvent::Remove);
            test(count < 3 || reader.getNextUnread().getValue() == "value3");
            test(count < 2 || reader.getNextUnread().getValue() == "value4");
            test(count < 1 || reader.getNextUnread().getEvent() == SampleEvent::Remove);
            readers.update(true); // Reader is done
        };

        // Keep all the samples in the history.
        {
            read(6, ReaderConfig());
        }

        // Keep 4 samples in the history
        {
            ReaderConfig config;
            config.sampleCount = 4;
            read(4, config);
        }

        // Keep last instance samples in the history
        {
            ReaderConfig config;
            config.clearHistory = ClearHistoryPolicy::Add;
            read(3, config);
        }
    }

    // Writer sample lifetime
    {
        while(!writers.getNextUnread().getValue()); // Wait for writer to write the samples before reading

        // Writer keeps 3ms worth of samples
        readers.update(false);
        auto reader = makeSingleKeyReader(topic, "elem1");
        test(reader.getNextUnread().getValue() == "value3");
        test(reader.getNextUnread().getValue() == "value4");
        test(reader.getNextUnread().getEvent() == SampleEvent::Remove);
        readers.update(true); // Reader is done
    }

    // Reader sample lifetime
    {
        while(!writers.getNextUnread().getValue()); // Wait for writer to write the samples before reading

        ReaderConfig config;
        config.sampleLifetime = 150;

        auto now = chrono::system_clock::now();

        // Reader wants 150ms worth of samples
        readers.update(false);
        auto reader = makeSingleKeyReader(topic, "elem1", config);
        reader.waitForUnread(3);
        auto samples = reader.getAllUnread();
        test(samples[0].getValue() == "value3");
        test(samples[1].getValue() == "value4");
        test(samples[2].getEvent() == SampleEvent::Remove);
        readers.update(true); // Reader is done

        for(const auto& s : samples)
        {
            test(s.getTimeStamp() >= (now - chrono::milliseconds(150)));
        }
    }

    return 0;
}
