// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

#include <Stock.h>

using namespace std;
using namespace DataStorm;
using namespace Demo;

int
main(int argc, char* argv[])
{
    //
    // Instantiates node.
    //
    Node node(argc, argv);

    //
    // Instantiates the "stocks" topic.
    //
    Topic<string, Demo::Stock> topic(node, "stocks");

    //
    // Setup partial update updaters. The updater is responsiable for updating the
    // element value when a partial update is received. Updaters must be set on
    // both the topic reader and writer.
    //
    topic.setUpdater<float>("price", [](Stock& stock, float price) { stock.price = price; });
    topic.setUpdater<int>("volume", [](Stock& stock, int volume) { stock.volume = volume; });

    //
    // Create a reader that connects to all the keys but doesn't receive any samples
    // (we use the `event' sample filter with an empty set of sample events to discard
    // events on the writer).
    //
    auto stocks = makeAnyKeyReader<SampleEventSeq>(topic, "event", SampleEventSeq {});
    stocks.waitForWriters();

    //
    // Get the set of stocks connected with the any reader and display their ticker.
    //
    cout << "Available stocks: " << endl;
    vector<string> tickers = stocks.getConnectedKeys();
    for(auto ticker : tickers)
    {
        cout << ticker << endl;
    }

    string stock;
    cout << "Please enter the stock to follow: ";
    cin >> stock;

    //
    // Read values for the given stock using a key reader.
    //
    auto reader = makeSingleKeyReader(topic, stock);
    auto value = reader.getNextUnread().getValue();

    cout << "Stock: " <<  value.name << " (" << stock << ")" << endl;
    cout << "Price: " << value.price << endl;
    cout << "Best bid/ask: " << value.bestBid << '/' << value.bestAsk << endl;
    cout << "Market Cap: " << value.marketCap << endl;
    cout << "Previous close: " << value.previousClose << endl;
    cout << "Volume: " << value.volume << endl;

    //
    // Prints out the received samples.
    //
    reader.onSample([](const Sample<string, Stock>& sample)
    {
        if(sample.getEvent() == SampleEvent::PartialUpdate)
        {
            if(sample.getUpdateTag() == "price")
            {
                cout << "received price update: " << sample.getValue().price << endl;
            }
            else if(sample.getUpdateTag() == "volume")
            {
                cout << "received volume update: " << sample.getValue().volume << endl;
            }
        }
    });

    //
    // Exit once no more writers are online
    //
    topic.waitForNoWriters();
    return 0;
}
