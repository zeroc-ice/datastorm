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
    // Wait for a writer to connect.
    //
    topic.waitForWriters();

    //
    // Instantiate reader, the reader sample filter criteria type must match the
    // criteria type specified for the writer.
    //
    // Here, the criteria is the string "good.*". This string is provided to writers
    // to perform the sample filtering.
    //
    auto reader = DataStorm::makeSingleKeyReader<string>(topic, "foo", "good.*");

    //
    // Get the 2 samples published by the writer which starts with good
    //
    auto sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    return 0;
}
