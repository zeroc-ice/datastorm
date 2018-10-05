// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

using namespace std;

namespace DataStorm
{
    template<> struct Encoder<chrono::system_clock::time_point>
    {
        static vector<unsigned char>
        encode(const chrono::system_clock::time_point& time)
        {
            assert(false); // Not used by the reader but it still needs to be declared.
            return vector<unsigned char>();
        }
    };

    template<> struct Decoder<chrono::system_clock::time_point>
    {
        static chrono::system_clock::time_point
        decode(const vector<unsigned char>& data)
        {
            //
            // Decode the number of seconds since epoch. The value is encoded in a way which
            // doesn't depend on the platform endianess.
            //
            long long int value = 0;
            for(auto p = data.rbegin(); p != data.rend(); ++p)
            {
                value = value * 256 + static_cast<long long int>(*p);
            }
            return std::chrono::time_point<std::chrono::system_clock>(std::chrono::seconds(value));
        }
    };
};

int
main(int argc, char* argv[])
{
    try
    {
        //
        // CtrlCHandler::maskSignals() must be called before the node is created or any other threads are started.
        //
        DataStorm::CtrlCHandler::maskSignals();

        //
        // Instantiates node.
        //
        DataStorm::Node node(argc, argv, "config.reader");

        //
        // Shutdown the node on Ctrl-C.
        //
        DataStorm::CtrlCHandler ctrlCHandler([&node](int) { node.shutdown(); });

        //
        // Instantiates the "time" topic.
        //
        DataStorm::Topic<string, chrono::system_clock::time_point> topic(node, "time");

        //
        // Instantiate a reader to read the time from all the topic cities.
        //
        auto reader = DataStorm::makeAnyKeyReader(topic);

        //
        // Wait for at least on writer to connect.
        //
        reader.waitForWriters();

        //
        // Prints out the received samples.
        //
        reader.onSamples(nullptr, [](const DataStorm::Sample<string, chrono::system_clock::time_point>& sample)
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
        // Exit once no more writers are connected.
        //
        reader.waitForNoWriters();
    }
    catch(const std::exception& ex)
    {
        cerr << ex.what() << endl;
        return 1;
    }
    return 0;
}
