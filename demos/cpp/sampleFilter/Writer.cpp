// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

using namespace std;

int
main(int argc, char* argv[])
{
    //
    // Instantiates DataStorm node.
    //
    DataStorm::Node node(argc, argv);

    //
    // Instantiates the "hello" topic. The topic uses strings for keys and values.
    //
    DataStorm::Topic<string, string> topic(node, "hello");

    //
    // Configure writers to not clear the history. We want the readers to receive
    // all the writer samples.
    //
    topic.setWriterDefaultConfig({ Ice::nullopt, Ice::nullopt, DataStorm::ClearHistoryPolicy::Never });

    //
    // Instantiate the foo writer.
    //
    auto writer = DataStorm::makeSingleKeyWriter(topic, "foo");

    //
    // Publish samples
    //
    writer.update("hi");
    writer.update("greetings");
    writer.update("good morning");
    writer.update("hello");
    writer.update("good afternoon");

    //
    // Wait for a reader to connect and then disconnect.
    //
    topic.waitForReaders();
    topic.waitForNoReaders();

    return 0;
}
