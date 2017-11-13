// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

using namespace std;

namespace DataStorm
{

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
    using KeyFilter = DataStorm::RegexFilter<string>;
    DataStorm::Topic<string, chrono::system_clock::time_point, KeyFilter, string> topic(node, "time");

    //
    // Instantiate a reader to read the time from all the topic cities.
    //
    DataStorm::ReaderConfig config;
    config.sampleCount = 0; // Don't keep sample history
    auto reader = DataStorm::makeFilteredReader(topic, ".*", config);

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
