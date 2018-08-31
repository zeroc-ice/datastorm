// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
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

    ReaderConfig config;
    config.sampleCount = -1; // Unlimited sample count
    config.clearHistory = ClearHistoryPolicy::Never;

    Topic<string, bool> controller(node, "controller");
    auto writers = makeSingleKeyReader(controller, "writers");
    auto readers = makeSingleKeyWriter(controller, "readers");

    Topic<string, string> topic(node, "string");

    // onSamples
    {
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem1", config);
            while(!writers.getNextUnread().getValue());
            promise<void> p;
            reader.onSamples([&p](const vector<Sample<string, string>>& samples)
            {
                test(samples.size() == 1);
                p.set_value();
            });
            p.get_future().wait();
            readers.update(true);
        }
        {
            auto reader = makeSingleKeyReader(topic, "elem2", config);
            promise<void> p;
            reader.onSamples([&p](const vector<Sample<string, string>>& samples)
            {
                test(samples.size() == 1);
                p.set_value();
            });
            p.get_future().wait();
        }
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem3", config);
            while(!writers.getNextUnread().getValue());
            promise<void> p;
            reader.onSamples([&p](const vector<Sample<string, string>>& samples)
            {
                test(samples.size() == 3);
                p.set_value();
            });
            p.get_future().wait();
            readers.update(true);
        }
    }

    return 0;
}
