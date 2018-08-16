// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

#include <Stock.h>

using namespace std;
using namespace Demo;

int
main(int argc, char* argv[])
{
    //
    // Instantiates node.
    //
    DataStorm::Node node(argc, argv);

    //
    // Instantiates the "stocks" topic.
    //
    DataStorm::Topic<string, Demo::Stock> topic(node, "stocks");

    //
    // Setup partial update updaters. The updater is responsiable for updating the
    // element value when a partial update is received. Updaters must be set on
    // the topic from both the reader and writer.
    //
    topic.setUpdater<float>("price", [](Stock& stock, float price) { stock.price = price; });
    topic.setUpdater<int>("volume", [](Stock& stock, int volume) { stock.volume = volume; });

    //
    // Create a reader that connects to all the keys but doesn't receive any samples
    // (we use the `_event' predefined sample filter with an empty set of sample
    // events to discard events on the writer).
    //
    auto stocks = DataStorm::makeAnyKeyReader<DataStorm::SampleEventSeq>(topic, "_event", DataStorm::SampleEventSeq {});
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
    stocks.onKeyConnect([](DataStorm::Topic<string, Stock>::WriterId, string stock)
    {
        cout << "New stock available: " << stock << endl;
    });

    string stock;
    cout << "Please enter the stock to follow (default = all):\n";
    getline(cin, stock);
    stocks.onKeyConnect(nullptr); // Clear the listener, we no longer need it

    //
    // Read values for the given stock using a key reader.
    //
    shared_ptr<DataStorm::Topic<string, Demo::Stock>::ReaderType> reader;
    if(stock.empty() || stock == "all")
    {
        reader = makeSharedAnyKeyReader(topic); // Returns a shared_ptr
    }
    else
    {
        tickers = stocks.getConnectedKeys();
        if(find(tickers.begin(), tickers.end(), stock) == tickers.end())
        {
            cout << "unknown stock `" << stock << "'" << endl;
            return 1;
        }
        reader = makeSharedSingleKeyReader(topic, stock); // Returns a shared_ptr
    }

    //
    // Print out the initial sample value.
    //
    reader->onInit([](const vector<DataStorm::Sample<string, Stock>>& samples)
    {
        assert(samples.size() == 1);
        auto value = samples[0].getValue();
        cout << "Stock: " <<  value.name << " (" << samples[0].getKey() << ")" << endl;
        cout << "Price: " << value.price << endl;
        cout << "Best bid/ask: " << value.bestBid << '/' << value.bestAsk << endl;
        cout << "Market Cap: " << value.marketCap << endl;
        cout << "Previous close: " << value.previousClose << endl;
        cout << "Volume: " << value.volume << endl;
        cout << endl;
    });

    //
    // Print out the received partial update samples.
    //
    reader->onSample([](const DataStorm::Sample<string, Stock>& sample)
    {
        if(sample.getEvent() == DataStorm::SampleEvent::PartialUpdate)
        {
            if(sample.getUpdateTag() == "price")
            {
                cout << "received price update for " << sample.getKey() << ": " << sample.getValue().price << endl;
            }
            else if(sample.getUpdateTag() == "volume")
            {
                cout << "received volume update for " << sample.getKey() << ": " << sample.getValue().volume << endl;
            }
        }
    });

    //
    // Exit once no more writers are online.
    //
    topic.waitForNoWriters();
    return 0;
}
