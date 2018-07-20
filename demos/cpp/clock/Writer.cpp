// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
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
            auto value = chrono::time_point_cast<chrono::seconds>(time).time_since_epoch().count();
            return Encoder<long long int>::encode(com, value);
        }
    };

    template<> struct Decoder<chrono::system_clock::time_point>
    {
        static chrono::system_clock::time_point
        decode(const shared_ptr<Ice::Communicator>& com, const vector<unsigned char>& data)
        {
            auto value = Decoder<long long int>::decode(com, data);
            return std::chrono::time_point<std::chrono::system_clock>(std::chrono::seconds(value));
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
    DataStorm::Topic<string, chrono::system_clock::time_point> topic(node, "time");

    //
    // Instantiate a writer to writer the time from the given city.
    //
    DataStorm::WriterConfig config;
    config.sampleCount = 0; // Don't keep sample history
    auto writer = DataStorm::makeSingleKeyWriter(topic, city, config);

    //
    // Print message when reader connects / disconnects
    //
    writer.onFilterConnect([](decltype(topic)::ReaderId reader, string name)
    {
        cout << "filtered reader connected (filter = " << name << ")" << endl;
    });
    writer.onFilterDisconnect([](decltype(topic)::ReaderId reader, string name)
    {
        cout << "filtered reader disconnected (filter = " << name << ")" << endl;
    });

    while(true)
    {
        writer.update(chrono::system_clock::now());
        this_thread::sleep_for(chrono::seconds(1));
    }

    return 0;
}
