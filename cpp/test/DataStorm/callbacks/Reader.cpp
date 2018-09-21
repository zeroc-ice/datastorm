// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

#include <TestCommon.h>

using namespace DataStorm;
using namespace std;

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    ReaderConfig config;
    config.sampleCount = -1; // Unlimited sample count
    config.clearHistory = ClearHistoryPolicy::Never;

    Topic<string, bool> controller(node, "controller");
    auto writers = makeSingleKeyReader(controller, "writers");
    auto readers = makeSingleKeyWriter(controller, "readers");

    Topic<string, string> topic(node, "string");

    // onSamples
    {
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem1", "", config);
            while(!writers.getNextUnread().getValue());
            promise<void> p;
            reader.onSamples([&p](const vector<Sample<string, string>>& samples)
            {
                test(samples.size() == 1);
                p.set_value();
            });
            p.get_future().wait();
            readers.update(true);
        }
        {
            auto reader = makeSingleKeyReader(topic, "elem2", "", config);
            promise<void> p;
            reader.onSamples([&p](const vector<Sample<string, string>>& samples)
            {
                test(samples.size() == 1);
                p.set_value();
            });
            p.get_future().wait();
        }
        {
            auto reader = makeSingleKeyReader(topic, "elem3", "", config);
            while(!writers.getNextUnread().getValue());
            promise<void> p;
            reader.onSamples([&p](const vector<Sample<string, string>>& samples)
            {
                test(samples.size() == 3);
                p.set_value();
            });
            p.get_future().wait();
        }
    }
    // onConnectedKeys
    {
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem1", "", config);
            reader.waitForWriters();
            while(!writers.getNextUnread().getValue());
            reader.getNextUnread();
            readers.update(true);
        }
        {
            auto reader = makeSingleKeyReader(topic, "elem2", "", config);
            reader.getNextUnread();
        }
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem3", "", config);
            promise<bool> p1, p2, p3;
            reader.onConnectedKeys([&p1, &p2, &p3](ConnectedAction action, vector<string> keys)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(keys.empty());
                }
                else if(action == ConnectedAction::Add)
                {
                    p2.set_value(!keys.empty() && keys[0] == "elem3");
                }
                else if(action == ConnectedAction::Remove)
                {
                    p3.set_value(!keys.empty() && keys[0] == "elem3");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
            while(!writers.getNextUnread().getValue());
            test(p3.get_future().get());
        }
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem4", "", config);
            reader.waitForWriters();
            promise<bool> p1, p2;
            reader.onConnectedKeys([&p1, &p2](ConnectedAction action, vector<string> keys)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(!keys.empty() && keys[0] == "elem4");
                }
                else if(action == ConnectedAction::Add)
                {
                    test(false);
                }
                else if(action == ConnectedAction::Remove)
                {
                    p2.set_value(!keys.empty() && keys[0] == "elem4");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
        }
        {
            readers.update(false);
            auto reader = makeFilteredKeyReader(topic, Filter<string>("_regex","elem1"), "", config);
            reader.waitForWriters();
            while(!writers.getNextUnread().getValue());
            reader.getNextUnread();
            readers.update(true);
        }
        {
            auto reader = makeFilteredKeyReader(topic, Filter<string>("_regex","elem2"), "", config);
            reader.getNextUnread();
        }
        {
            readers.update(false);
            auto reader = makeFilteredKeyReader(topic, Filter<string>("_regex","elem3"), "", config);
            promise<bool> p1, p2, p3;
            reader.onConnectedKeys([&p1, &p2, &p3](ConnectedAction action, vector<string> keys)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(keys.empty());
                }
                else if(action == ConnectedAction::Add)
                {
                    p2.set_value(!keys.empty() && keys[0] == "elem3");
                }
                else if(action == ConnectedAction::Remove)
                {
                    p3.set_value(!keys.empty() && keys[0] == "elem3");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
            while(!writers.getNextUnread().getValue());
            test(p3.get_future().get());
        }
        {
            readers.update(false);
            auto reader = makeFilteredKeyReader(topic, Filter<string>("_regex", "elem4"), "", config);
            reader.waitForWriters();
            promise<bool> p1, p2;
            reader.onConnectedKeys([&p1, &p2](ConnectedAction action, vector<string> keys)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(!keys.empty() && keys[0] == "elem4");
                }
                else if(action == ConnectedAction::Add)
                {
                    test(false);
                }
                else if(action == ConnectedAction::Remove)
                {
                    p2.set_value(!keys.empty() && keys[0] == "elem4");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
        }

        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "anyelem1", "", config);
            while(!writers.getNextUnread().getValue());
            reader.getNextUnread();
            readers.update(true);
        }
        {
            while(writers.getNextUnread().getValue());
            readers.update(false);
            auto reader = makeAnyKeyReader(topic, "", config);
            promise<bool> p1, p2, p3;
            reader.onConnectedKeys([&p1, &p2, &p3](ConnectedAction action, vector<string> keys)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(keys.empty());
                }
                else if(action == ConnectedAction::Add)
                {
                    p2.set_value(!keys.empty() && keys[0] == "anyelem3");
                }
                else if(action == ConnectedAction::Remove)
                {
                    p3.set_value(!keys.empty() && keys[0] == "anyelem3");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
            while(!writers.getNextUnread().getValue());
            test(p3.get_future().get());
        }
    }
    // onConnected
    {
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem1", "reader1", config);
            reader.waitForWriters();
            while(!writers.getNextUnread().getValue());
            reader.getNextUnread();
            readers.update(true);
        }
        {
            auto reader = makeSingleKeyReader(topic, "elem2", "reader2", config);
            reader.getNextUnread();
        }
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem3", "", config);
            promise<bool> p1, p2, p3;
            reader.onConnected([&p1, &p2, &p3](ConnectedAction action, vector<string> writers)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(writers.empty());
                }
                else if(action == ConnectedAction::Add)
                {
                    p2.set_value(!writers.empty() && writers[0] == "writer1");
                }
                else if(action == ConnectedAction::Remove)
                {
                    p3.set_value(!writers.empty() && writers[0] == "writer1");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
            while(!writers.getNextUnread().getValue());
            test(p3.get_future().get());
        }
        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "elem4", "", config);
            reader.waitForWriters();
            promise<bool> p1, p2;
            reader.onConnected([&p1, &p2](ConnectedAction action, vector<string> writers)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(!writers.empty() && writers[0] == "writer2");
                }
                else if(action == ConnectedAction::Add)
                {
                    test(false);
                }
                else if(action == ConnectedAction::Remove)
                {
                    p2.set_value(!writers.empty() && writers[0] == "writer2");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
        }
        {
            readers.update(false);
            auto reader = makeFilteredKeyReader(topic, Filter<string>("_regex", "elem1"), "reader1", config);
            reader.waitForWriters();
            while(!writers.getNextUnread().getValue());
            reader.getNextUnread();
            readers.update(true);
        }
        {
            auto reader = makeFilteredKeyReader(topic, Filter<string>("_regex", "elem2"), "reader2", config);
            reader.getNextUnread();
        }
        {
            readers.update(false);
            auto reader = makeFilteredKeyReader(topic, Filter<string>("_regex", "elem3"), "", config);
            promise<bool> p1, p2, p3;
            reader.onConnected([&p1, &p2, &p3](ConnectedAction action, vector<string> writers)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(writers.empty());
                }
                else if(action == ConnectedAction::Add)
                {
                    p2.set_value(!writers.empty() && writers[0] == "writer1");
                }
                else if(action == ConnectedAction::Remove)
                {
                    p3.set_value(!writers.empty() && writers[0] == "writer1");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
            while(!writers.getNextUnread().getValue());
            test(p3.get_future().get());
        }
        {
            readers.update(false);
            auto reader = makeFilteredKeyReader(topic, Filter<string>("_regex", "elem4"), "", config);
            reader.waitForWriters();
            promise<bool> p1, p2;
            reader.onConnected([&p1, &p2](ConnectedAction action, vector<string> writers)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(!writers.empty() && writers[0] == "writer2");
                }
                else if(action == ConnectedAction::Add)
                {
                    test(false);
                }
                else if(action == ConnectedAction::Remove)
                {
                    p2.set_value(!writers.empty() && writers[0] == "writer2");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
        }

        {
            readers.update(false);
            auto reader = makeSingleKeyReader(topic, "anyelem1", "reader1", config);
            while(!writers.getNextUnread().getValue());
            reader.getNextUnread();
            readers.update(true);
        }
        {
            while(writers.getNextUnread().getValue());
            readers.update(false);
            auto reader = makeAnyKeyReader(topic, "", config);
            promise<bool> p1, p2, p3;
            reader.onConnected([&p1, &p2, &p3](ConnectedAction action, vector<string> writers)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(writers.empty());
                }
                else if(action == ConnectedAction::Add)
                {
                    p2.set_value(!writers.empty() && writers[0] == "writer1");
                }
                else if(action == ConnectedAction::Remove)
                {
                    p3.set_value(!writers.empty() && writers[0] == "writer1");
                }
            });
            test(p1.get_future().get());
            readers.update(true);
            test(p2.get_future().get());
            while(!writers.getNextUnread().getValue());
            test(p3.get_future().get());
        }
    }
    return 0;
}
