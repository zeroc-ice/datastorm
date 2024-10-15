// **********************************************************************
//
// Copyright (c) ZeroC, Inc. All rights reserved.
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
    config.clearHistory = ClearHistoryPolicy::Never;

    cout << "testing writer connection closure... " << flush;
    {
        Topic<string, string> topic(node, "string");
        auto writer = makeSingleKeyWriter(topic, "element", "", config);
        writer.add("add");
        writer.update("update1");
        auto barrier = makeSingleKeyReader(topic, "barrier");
        auto sample = barrier.getNextUnread();
        writer.waitForReaders();
        auto connection = node.getSessionConnection(sample.getSession());
        test(connection);
        connection->close().get();
        writer.update("update2");
        barrier.getNextUnread();
        writer.update("update3");
        barrier.getNextUnread();
    }
    cout << "ok" << endl;

    cout << "testing reader connection closure... " << flush;
    {
        Topic<string, int> topic(node, "int");
        auto writer = makeSingleKeyWriter(topic, "element", "", config);
        writer.waitForReaders();
        for (int i = 0; i < 1000; ++i)
        {
            writer.update(i);
        }
        makeSingleKeyReader(topic, "barrier").getNextUnread();
    }
    cout << "ok" << endl;

    return 0;
}
