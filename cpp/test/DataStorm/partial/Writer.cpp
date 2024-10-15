//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#if defined(_MSC_VER)
#    pragma warning(4 : 4244)
#endif

#include <DataStorm/DataStorm.h>

#include <Test.h>
#include <TestCommon.h>

using namespace DataStorm;
using namespace std;
using namespace Test;

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    Topic<string, Stock> topic(node, "topic");

    WriterConfig config;
    config.sampleCount = -1;
    config.clearHistory = ClearHistoryPolicy::Never;
    topic.setWriterDefaultConfig(config);

    topic.setUpdater<float>("price", [](Stock& stock, float price) { stock.price = price; });

    cout << "testing partial update... " << flush;
    {
        auto writer = makeSingleKeyWriter(topic, "AAPL");
        writer.waitForReaders();
        writer.add(Stock{12.0f, 13.0f, 14.0f});
        writer.partialUpdate<float>("price")(15.0f);
        writer.partialUpdate<float>("price")(18);
        writer.waitForNoReaders();
    }
    cout << "ok" << endl;

    return 0;
}
