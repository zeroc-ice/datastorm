// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

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
        DataStorm::Node node(argc, argv);

        //
        // Shutdown the node on Ctrl-C.
        //
        DataStorm::CtrlCHandler ctrlCHandler([&node](int) { node.shutdown(); });

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
