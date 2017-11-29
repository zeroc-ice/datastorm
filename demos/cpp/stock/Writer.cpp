// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

#include <Stock.h>

#include <random>

using namespace std;
using namespace DataStorm;
using namespace Demo;

namespace
{

std::random_device random;

KeyWriter<string, Stock, SampleEventFilter, vector<SampleEvent>>
makeStock(Topic<string, Stock>& topic, string ticker, Stock stock)
{
    auto writer = makeSingleKeyWriter<SampleEventFilter, vector<SampleEvent>>(topic, move(ticker));
    writer.add(move(stock));
    return writer;
}

void
updateStock(KeyWriter<string, Stock, SampleEventFilter, vector<SampleEvent>>& stock)
{
    if(uniform_int_distribution<int>(1, 10)(random) < 8)
    {
        auto price = stock.getLast().getValue().price;
        stock.update<float>("price", uniform_real_distribution<float>(price * 0.95f, price * 1.05f)(random));
    }
    else
    {
        auto volume = stock.getLast().getValue().volume;
        stock.update<int>("volume", uniform_int_distribution<int>(volume * 95 / 100, volume * 105 / 100)(random));
    }
}

}

int
main(int argc, char* argv[])
{
    //
    // Instantiates node.
    //
    Node node(argc, argv);

    //
    // Instantiates the "stock" topic.
    //
    Topic<string, Stock> topic(node, "stocks");
    topic.setUpdater<float>("price", [](Stock& stock, float price) { stock.price = price; });
    topic.setUpdater<int>("volume", [](Stock& stock, int volume) { stock.volume = volume; });

    //
    // Instantiate writers for few stocks.
    //
    vector<KeyWriter<string, Stock, SampleEventFilter, vector<SampleEvent>>> stocks;
    stocks.push_back(makeStock(topic, "GOOG", Stock("Google", 1040.61, 1035, 1043.178, 723018024728, 1035.96, 536996)));
    stocks.push_back(makeStock(topic, "AAPL", Stock("Apple", 174.74, 174.44, 175.50, 898350570640, 174.96, 14026673)));
    stocks.push_back(makeStock(topic, "FB", Stock("Facebook", 182.78, 180.29, 183.15,  531123722959, 180.87, 9426283)));
    stocks.push_back(makeStock(topic, "AMZN", Stock("Amazon", 1186.41, 1160.70, 1186.84, 571697967142, 1156.16, 3442959)));
    stocks.push_back(makeStock(topic, "MSFT", Stock("Microsoft", 83.27, 82.78, 83.43, 642393925538, 83.11, 6056186)));

    while(true)
    {
        this_thread::sleep_for(chrono::seconds(1));
        for(auto& w : stocks)
        {
            updateStock(w);
        }
    }

    return 0;
}
