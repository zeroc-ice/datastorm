// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#if defined(_MSC_VER) && _MSC_VER == 1900 // Visual Studio 2015
#   pragma warning(disable:4503) // decorated name length exceeded, name was truncated
#endif

#include <DataStorm/DataStorm.h>

using namespace std;

namespace DataStorm
{
    template<> struct Encoder<chrono::system_clock::time_point>
    {
        static vector<unsigned char>
        encode(const chrono::system_clock::time_point& time)
        {
            //
            // Encode the number of seconds since epoch. The value is encoded in a way which
            // doesn't depend on the platform endianess.
            //
            vector<unsigned char> data;
            auto value = chrono::time_point_cast<chrono::seconds>(time).time_since_epoch().count();
            while(value)
            {
                data.push_back(static_cast<unsigned char>(value % 256));
                value = value / 256;
            }
            return data;
        }
    };

    template<> struct Decoder<chrono::system_clock::time_point>
    {
        static chrono::system_clock::time_point
        decode(const vector<unsigned char>& data)
        {
            assert(false); // Not used by the reader but it still needs to be declared.
            return chrono::system_clock::time_point();
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

    //
    // Asks for city name to publish updates
    //
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
    auto writer = DataStorm::makeSingleKeyWriter(topic, city);

    while(true)
    {
        writer.update(chrono::system_clock::now());
        this_thread::sleep_for(chrono::seconds(1));
    }

    return 0;
}
