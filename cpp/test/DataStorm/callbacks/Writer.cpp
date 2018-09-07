// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
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
            writers.update(false);
            auto writer = makeSingleKeyWriter(topic, "elem1", config);
            writer.add("value1");
            writers.update(true);
            while(!readers.getNextUnread().getValue());
        }
        {
            auto writer = makeSingleKeyWriter(topic, "elem2", config);
            writer.waitForReaders();
            writer.add("value1");
            writer.waitForNoReaders();
        }
        {
            writers.update(false);
            auto writer = makeSingleKeyWriter(topic, "elem3", config);
            writer.add("value1");
            writer.update("value2");
            writer.remove();
            writers.update(true);
            while(!readers.getNextUnread().getValue());
        }
    }
    cout << "ok" << endl;

    return 0;
}
