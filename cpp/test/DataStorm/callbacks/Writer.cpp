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

    WriterConfig config;
    config.sampleCount = -1; // Unlimited sample count
    config.clearHistory = ClearHistoryPolicy::Never;

    Topic<string, bool> controller(node, "controller");
    auto writers = makeSingleKeyWriter(controller, "writers");
    auto readers = makeSingleKeyReader(controller, "readers");

    Topic<string, string> topic(node, "string");

    cout << "testing onSamples... " << flush;
    {
        {
            auto writer = makeSingleKeyWriter(topic, "elem1", config);
            writer.add("value1");
            writers.update(true);
            writer.waitForReaders();
            writer.waitForNoReaders();
            writers.update(false);
        }
        {
            auto writer = makeSingleKeyWriter(topic, "elem2", config);
            writer.waitForReaders();
            writer.add("value1");
            writer.waitForNoReaders();
        }
        {
            auto writer = makeSingleKeyWriter(topic, "elem3", config);
            writer.add("value1");
            writer.update("value2");
            writer.remove();
            writers.update(true);
            writer.waitForReaders();
            writer.waitForNoReaders();
            writers.update(false);
        }
    }
    cout << "ok" << endl;

    return 0;
}
