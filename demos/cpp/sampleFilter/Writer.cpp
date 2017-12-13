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
    topic.setWriterDefaultConfig(DataStorm::WriterConfig(-1)); // Keeps all the samples in the history.
    topic.setSampleFilter("regex", makeSampleRegexFilter(topic));

    //
    // Instantiate writer, the writer sample filter criteria type must match the
    // criteria type specified for the reader.
    //
    // Here, the criteria is a string and we use the DataStorm::RegexFilter filter
    // to filter the sample with a regular expression initiliazed from the criteria.
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
