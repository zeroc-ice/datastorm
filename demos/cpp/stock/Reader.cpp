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
    try
    {
        //
        // Instantiates node.
        //
        DataStorm::Node node(argc, argv, "config.reader");

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

        shared_ptr<DataStorm::Topic<string, Demo::Stock>::ReaderType> reader;
        {
            //
            // Create a reader that connects to all the keys but doesn't receive any samples
            // (we use the `_event' predefined sample filter with an empty set of sample
            // events to discard events on the writer).
            //
            auto stocks = DataStorm::makeAnyKeyReader<DataStorm::SampleEventSeq>(topic,
                                                                                 "_event",
                                                                                 DataStorm::SampleEventSeq {});
            stocks.waitForWriters();

            //
            // Get the set of stocks connected with the any reader and display their ticker.
            //
            cout << "Available stocks: " << endl;
            for(auto ticker : stocks.getConnectedKeys())
            {
                cout << ticker << endl;
            }

            string stock;
            cout << "Please enter the stock to follow (default = all):\n";
            getline(cin, stock);

            //
            // Read values for the given stock using a key reader.
            //
            if(stock.empty() || stock == "all")
            {
                reader = makeSharedAnyKeyReader(topic); // Returns a shared_ptr
            }
            else
            {
                auto tickers = stocks.getConnectedKeys();
                if(find(tickers.begin(), tickers.end(), stock) == tickers.end())
                {
                    cout << "unknown stock `" << stock << "'" << endl;
                    return 1;
                }
                reader = makeSharedSingleKeyReader(topic, stock); // Returns a shared_ptr
            }
        }

        //
        // Wait for the reader to be connected.
        //
        reader->waitForWriters();

        //
        // Print out the sample values queued with this reader.
        //
        reader->onSamples([](const vector<DataStorm::Sample<string, Stock>>& samples)
        {
            for(auto& s : samples)
            {
                if(s.getEvent() == DataStorm::SampleEvent::Add || s.getEvent() == DataStorm::SampleEvent::Update)
                {
                    auto value = s.getValue();
                    cout << "Stock: " <<  value.name << " (" << s.getKey() << ")" << endl;
                    cout << "Price: " << value.price << endl;
                    cout << "Best bid/ask: " << value.bestBid << '/' << value.bestAsk << endl;
                    cout << "Market Cap: " << value.marketCap << endl;
                    cout << "Previous close: " << value.previousClose << endl;
                    cout << "Volume: " << value.volume << endl;
                    cout << endl;
                }
                if(s.getEvent() == DataStorm::SampleEvent::PartialUpdate)
                {
                    if(s.getUpdateTag() == "price")
                    {
                        cout << "received price update for " << s.getKey() << ": " << s.getValue().price << endl;
                    }
                    else if(s.getUpdateTag() == "volume")
                    {
                        cout << "received volume update for " << s.getKey() << ": " << s.getValue().volume << endl;
                    }
                }
            }
        });

        //
        // Exit once no more writers are online.
        //
        reader->waitForNoWriters();
    }
    catch(const std::exception& ex)
    {
        cerr << ex.what() << endl;
        return 1;
    }
    return 0;
}
