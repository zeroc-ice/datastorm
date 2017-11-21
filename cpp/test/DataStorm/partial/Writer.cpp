// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

#include <TestCommon.h>
#include <Test.h>

using namespace DataStorm;
using namespace std;
using namespace Test;

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    Topic<string, shared_ptr<Stock>> topic(node, "topic");
    topic.setUpdater<float>("price", [](shared_ptr<Stock> stock, float price)
                            {
                                stock = stock->ice_clone();
                                stock->price = price;
                                return stock;
                            });

    cout << "testing partial update... " << flush;
    {
        auto writer = makeSingleKeyWriter(topic, "AAPL");
        writer.add(make_shared<Stock>(12.0f, 13.0f, 14.0f));
        writer.update("price", 15.0f);
        writer.update("price", 18.0f);
        writer.waitForReaders();
        writer.waitForNoReaders();
    }
    cout << "ok" << endl;

    return 0;
}
