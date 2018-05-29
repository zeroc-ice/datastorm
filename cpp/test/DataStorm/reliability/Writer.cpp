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

    WriterConfig config;
    config.clearHistory = ClearHistoryPolicy::Never;

    cout << "testing writer connection closure... " << flush;
    {
        Topic<string, string> topic(node, "string");
        auto writer = makeSingleKeyWriter(topic, "element", config);
        writer.add("add");
        writer.update("update1");
        std::promise<shared_ptr<Ice::Connection>> promise;
        writer.onKeyConnect([&node, &promise, &writer](decltype(writer)::ReaderId reader, string key)
        {
            promise.set_value(node.getSessionConnection(get<0>(reader)));
            writer.onKeyConnect(nullptr);
        });
        auto barrier = makeSingleKeyReader(topic, "barrier");
        writer.waitForReaders();
        auto connection = promise.get_future().get();
        test(connection);
        connection->close(Ice::ConnectionClose::Gracefully);
        writer.update("update2");
        barrier.getNextUnread();
        writer.update("update3");
        barrier.getNextUnread();
    }
    cout << "ok" << endl;

    cout << "testing reader connection closure... " << flush;
    {
        Topic<string, int> topic(node, "int");
        auto writer = makeSingleKeyWriter(topic, "element", config);
        writer.waitForReaders();
        for(int i = 0; i < 1000; ++i)
        {
            writer.update(i);
        }
        makeSingleKeyReader(topic, "barrier").getNextUnread();
    }
    cout << "ok" << endl;

    return 0;
}
