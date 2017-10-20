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

    static chrono::system_clock::time_point
    decode(const shared_ptr<Ice::Communicator>& com, const vector<unsigned char>& data)
    {
        return chrono::system_clock::from_time_t(static_cast<time_t>(Encoder<long long int>::decode(com, data)));
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
    using KeyFilter = DataStorm::RegexFilter<string>;
    DataStorm::Topic<string, chrono::system_clock::time_point, KeyFilter, string> topic(node, "time");

    //
    // Instantiate a reader to read the time from all the topic cities.
    //
    auto reader = DataStorm::makeFilteredReader(topic, ".*");

    //
    // Wait for at least on writer to be online.
    //
    reader.waitForWriters();

    //
    // Prints out the received samples.
    //
    reader.onSample([](DataStorm::Sample<string, chrono::system_clock::time_point> sample)
    {
        auto time = chrono::system_clock::to_time_t(sample.getValue());
        cout << time << endl;
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
