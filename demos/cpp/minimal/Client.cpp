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
    // Instantiate a reader with the key "foo".
    //
    auto reader = DataStorm::makeKeyReader(topic, "foo");

    //
    // Get sample.
    //
    auto sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    return 0;
}
