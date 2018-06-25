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
    // Instantiates the "hello" topic. The topic uses strings for keys and values
    // and also supports key filtering with the DataStorm::RegexFilter regular
    // expression filter.
    //
    DataStorm::Topic<string, string> topic(node, "hello");

    //
    // Setup the regex filter to allow filtering element keys based on a regular
    // expression. Key filters must be set both on the topic reader and writer.
    //
    topic.setKeyFilter("regex", makeKeyRegexFilter(topic));

    //
    // Wait for a writer to connect.
    //
    topic.waitForWriters();

    //
    // Instantiate a filtered reader that matches the writer key using the foo[ace]
    // regular expression. We keep at most 10 samples in the history.
    //
    auto reader = DataStorm::makeFilteredReader<string>(topic, "regex", "foo[ace]",
                                                        { 10, 0, DataStorm::ClearHistoryPolicy::Never });

    //
    // Get the 3 samples published by the writers fooa, fooc and fooe.
    //
    auto sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    sample = reader.getNextUnread();
    cout << sample.getKey() << " says " << sample.getValue() << "!" << endl;

    return 0;
}
