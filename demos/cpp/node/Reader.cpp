//
// Copyright (c) ZeroC, Inc. All rights reserved.
//

#include <DataStorm/DataStorm.h>

using namespace std;

int
main(int argc, char* argv[])
{
    try
    {
        //
        // CtrlCHandler::maskSignals() must be called before the node is created or any other threads are started.
        //
        DataStorm::CtrlCHandler::maskSignals();

        //
        // Instantiates node.
        //
        DataStorm::Node node(argc, argv, "config.reader");

        //
        // Shutdown the node on Ctrl-C.
        //
        DataStorm::CtrlCHandler ctrlCHandler([&node](int) { node.shutdown(); });

        //
        // Instantiates the "time" topic.
        //
        DataStorm::Topic<int, string> topic(node, "time");

        //
        // Instantiate a reader to read the time from all the topic writers.
        //
        auto reader = DataStorm::makeAnyKeyReader(topic);

        //
        // Wait for at least on writer to connect.
        //
        reader.waitForWriters();

        //
        // Prints out the received samples.
        //
        reader.onSamples(nullptr, [](const DataStorm::Sample<int, string>& sample)
        {
            cout << "[" << sample.getKey() << "] received time: " << sample.getValue() << endl;
        });

        //
        // Exit once the user hits Ctrl-C to shutdown the node.
        //
        node.waitForShutdown();
    }
    catch(const std::exception& ex)
    {
        cerr << ex.what() << endl;
        return 1;
    }
    return 0;
}
