// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
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
    // Setup the regex filter to allow filtering element keys based on a regular
    // expression. Key filters must be set both on the topic reader and writer.
    //
    topic.setKeyFilter("regex", makeKeyRegexFilter(topic));

    //
    // Instantiate writers
    //
    auto writera = DataStorm::makeSingleKeyWriter(topic, "fooa");
    auto writerb = DataStorm::makeSingleKeyWriter(topic, "foob");
    auto writerc = DataStorm::makeSingleKeyWriter(topic, "fooc");
    auto writerd = DataStorm::makeSingleKeyWriter(topic, "food");
    auto writere = DataStorm::makeSingleKeyWriter(topic, "fooe");

    //
    // Publish a sample on each writer.
    //
    writera.update("hello");
    writerb.update("hello");
    writerc.update("hello");
    writerd.update("hello");
    writere.update("hello");

    //
    // Wait for a reader to connect and then disconnect.
    //
    topic.waitForReaders();
    topic.waitForNoReaders();

    return 0;
}
