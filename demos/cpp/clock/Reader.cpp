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

    //
    // Instantiates the "time" topic.
    //
    DataStorm::Topic<string, chrono::system_clock::time_point> topic(node, "time");

    //
    // Instantiate a reader to read the time from all the topic cities.
    //
    DataStorm::ReaderConfig config;
    config.sampleCount = 0; // Don't keep sample history
    auto reader = DataStorm::makeAnyKeyReader(topic, config);

    //
    // Wait for at least on writer to be online.
    //
    reader.waitForWriters();

    //
    // Prints out the received samples.
    //
    reader.onSample([](const DataStorm::Sample<string, chrono::system_clock::time_point>& sample)
    {
        auto time = chrono::system_clock::to_time_t(sample.getValue());
        char timeString[100];
        if(strftime(timeString, sizeof(timeString), "%x %X", localtime(&time)) == 0)
        {
            timeString[0] = '\0';
        }
        cout << "received time for `" << sample.getKey() << "': " << timeString << endl;
    });

    //
    // Exit once no more writers are online
    //
    reader.waitForNoWriters();
    return 0;
}
