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
    // Configure readers to never clear the history. We want to receive all the
    // samples written by the writers.
    //
    topic.setReaderDefaultConfig({ Ice::nullopt, Ice::nullopt, DataStorm::ClearHistoryPolicy::Never });

    //
    // Instantiate a filtered reader for keys matching the foo[ace] regular expression.
    //
    auto reader = DataStorm::makeFilteredReader<string>(topic, "_regex", "foo[ace]");

    //
    // Get the samples published by the writers fooa, fooc and fooe.
    //
    auto sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    return 0;
}
