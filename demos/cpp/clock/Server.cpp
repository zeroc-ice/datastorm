// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

using namespace std;

namespace DataStorm
{

template<> struct Encoder<chrono::system_clock::time_point>
{
    static vector<unsigned char>
    encode(const shared_ptr<Ice::Communicator>& com, const chrono::system_clock::time_point& time)
    {
        return Encoder<long long int>::encode(com, chrono::system_clock::to_time_t(time));
    }
};

};

int
main(int argc, char* argv[])
{
    //
    // Instantiates node.
    //
    DataStorm::Node node(argc, argv);

    string city;
    cout << "Please enter city name: ";
    cin >> city;

    //
    // Instantiates the "time" topic.
    //
    using KeyFilter = DataStorm::RegexFilter<string>;
    DataStorm::Topic<string, chrono::system_clock::time_point, KeyFilter, string> topic(node, "time");

    //
    // Instantiate a writer to writer the time from the given city.
    //
    auto writer = DataStorm::makeKeyWriter(topic, city);

    writer.onConnect([](tuple<string, long long int, long long int> reader)
    {
        cout << "reader connected " << endl;
    });
    writer.onDisconnect([](tuple<string, long long int, long long int> reader)
    {
        cout << "reader disconnected " << endl;
    });

    while(true)
    {
        writer.update(chrono::system_clock::now());
        this_thread::sleep_for(chrono::seconds(1));
    }

    return 0;
}
