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
    // Instantiate the reader for the key "foo". The reader uses the predefined
    // _regex sample filter and the "good.*"" regular expression as the criteria.
    //
    auto reader = DataStorm::makeSingleKeyReader<string>(topic, "foo", "_regex", "good.*");

    //
    // Get the 2 samples starting with good published by the writer.
    //
    auto sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    return 0;
}
