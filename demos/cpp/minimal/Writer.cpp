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
    // Instantiate a writer with the key "foo".
    //
    auto writer = DataStorm::makeSingleKeyWriter(topic, "foo");

    //
    // Publish a sample.
    //
    writer.update("hello");

    //
    // Wait for a reader to connect and then disconnect.
    //
    writer.waitForReaders();
    writer.waitForNoReaders();

    return 0;
}
