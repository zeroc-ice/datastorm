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
    topic.setUpdater<float>("price", [](Stock& stock, float price) { stock.price = price; });
    topic.setUpdater<int>("volume", [](Stock& stock, int volume) { stock.volume = volume; });

    //
    // Create a reader that connects to all the writer but doesn't receive any samples.
    //
    auto stocks = makeMultiKeyReader<vector<SampleEvent>>(topic, {}, {});
    stocks.waitForWriters();

    cout << "Available stocks: " << endl;
    vector<string> tickers = stocks.getConnectedKeys();
    for(auto ticker : tickers)
    {
        cout << ticker << endl;
    }

    string stock;
    cout << "Please enter the stock to follow: ";
    cin >> stock;

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
