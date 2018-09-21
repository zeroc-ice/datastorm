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

    WriterConfig config;
    config.sampleCount = -1; // Unlimited sample count
    config.clearHistory = ClearHistoryPolicy::Never;

    Topic<string, bool> controller(node, "controller");
    auto writers = makeSingleKeyWriter(controller, "writers");
    auto readers = makeSingleKeyReader(controller, "readers");

    Topic<string, string> topic(node, "string");

    cout << "testing onSamples... " << flush;
    {
        {
            writers.update(false);
            auto writer = makeSingleKeyWriter(topic, "elem1", "", config);
            writer.add("value1");
            writers.update(true);
            while(!readers.getNextUnread().getValue());
        }
        {
            auto writer = makeSingleKeyWriter(topic, "elem2", "", config);
            writer.waitForReaders();
            writer.add("value1");
            writer.waitForNoReaders();
        }
        {
            writers.update(false);
            auto writer = makeSingleKeyWriter(topic, "elem3", "", config);
            writer.add("value1");
            writer.update("value2");
            writer.remove();
            writers.update(true);
            writer.waitForReaders();
            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing onConnectedKeys... " << flush;
    {
        for(int i = 0; i < 2; ++i)
        {
            {
                while(readers.getNextUnread().getValue());
                writers.update(false);
                auto writer = makeSingleKeyWriter(topic, "elem1", "", config);
                promise<bool> p1, p2, p3;
                writer.onConnectedKeys([&p1, &p2, &p3](ConnectedAction action, vector<string> keys)
                {
                    if(action == ConnectedAction::Initialize)
                    {
                        p1.set_value(keys.empty());
                    }
                    else if(action == ConnectedAction::Add)
                    {
                        p2.set_value(!keys.empty() && keys[0] == "elem1");
                    }
                    else if(action == ConnectedAction::Remove)
                    {
                        p3.set_value(!keys.empty() && keys[0] == "elem1");
                    }
                });
                test(p1.get_future().get());
                writers.update(true);
                test(p2.get_future().get());
                writer.update("value2");
                while(!readers.getNextUnread().getValue());
                test(p3.get_future().get());
            }
            {
                auto writer = makeSingleKeyWriter(topic, "elem2", "", config);
                writer.waitForReaders();
                promise<bool> p1, p2;
                writer.onConnectedKeys([&p1, &p2](ConnectedAction action, vector<string> keys)
                {
                    if(action == ConnectedAction::Initialize)
                    {
                        p1.set_value(!keys.empty() && keys[0] == "elem2");
                    }
                    else if(action == ConnectedAction::Add)
                    {
                        test(false);
                    }
                    else if(action == ConnectedAction::Remove)
                    {
                        p2.set_value(!keys.empty() && keys[0] == "elem2");
                    }
                });
                test(p1.get_future().get());
                writer.update("value2");
                test(p2.get_future().get());
            }
            {
                writers.update(false);
                auto writer = makeSingleKeyWriter(topic, "elem3", "", config);
                writer.waitForReaders();
                while(!readers.getNextUnread().getValue());
                writers.update(true);
            }
            {
                auto writer = makeSingleKeyWriter(topic, "elem4", "", config);
                while(!readers.getNextUnread().getValue());
            }
        }

        {
            while(readers.getNextUnread().getValue());
            writers.update(false);
            auto writer = makeAnyKeyWriter(topic, "", config);
            promise<bool> p1, p2, p3;
            writer.onConnectedKeys([&p1, &p2, &p3](ConnectedAction action, vector<string> keys)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(keys.empty());
                }
                else if(action == ConnectedAction::Add)
                {
                    p2.set_value(!keys.empty() && keys[0] == "anyelem1");
                }
                else if(action == ConnectedAction::Remove)
                {
                    p3.set_value(!keys.empty() && keys[0] == "anyelem1");
                }
            });
            test(p1.get_future().get());
            writers.update(true);
            test(p2.get_future().get());
            writer.update("anyelem1", "value2");
            while(!readers.getNextUnread().getValue());
            test(p3.get_future().get());
        }
        {
            writers.update(false);
            auto writer = makeSingleKeyWriter(topic, "anyelem3", "", config);
            writer.waitForReaders();
            while(!readers.getNextUnread().getValue());
            writers.update(true);
        }
    }
    cout << "ok" << endl;

    cout << "testing onConnected... " << flush;
    {
        for(int i = 0; i < 2; ++i)
        {
            {
                while(readers.getNextUnread().getValue());
                writers.update(false);
                auto writer = makeSingleKeyWriter(topic, "elem1", "", config);
                promise<bool> p1, p2, p3;
                writer.onConnected([&p1, &p2, &p3](ConnectedAction action, vector<string> readers)
                {
                    if(action == ConnectedAction::Initialize)
                    {
                        p1.set_value(readers.empty());
                    }
                    else if(action == ConnectedAction::Add)
                    {
                        p2.set_value(!readers.empty() && readers[0] == "reader1");
                    }
                    else if(action == ConnectedAction::Remove)
                    {
                        p3.set_value(!readers.empty() && readers[0] == "reader1");
                    }
                });
                test(p1.get_future().get());
                writers.update(true);
                test(p2.get_future().get());
                writer.update("value2");
                while(!readers.getNextUnread().getValue());
                test(p3.get_future().get());
            }
            {
                auto writer = makeSingleKeyWriter(topic, "elem2", "", config);
                writer.waitForReaders();
                promise<bool> p1, p2;
                writer.onConnected([&p1, &p2](ConnectedAction action, vector<string> readers)
                {
                    if(action == ConnectedAction::Initialize)
                    {
                        p1.set_value(!readers.empty() && readers[0] == "reader2");
                    }
                    else if(action == ConnectedAction::Add)
                    {
                        test(false);
                    }
                    else if(action == ConnectedAction::Remove)
                    {
                        p2.set_value(!readers.empty() && readers[0] == "reader2");
                    }
                });
                test(p1.get_future().get());
                writer.update("value2");
                test(p2.get_future().get());
            }
            {
                writers.update(false);
                auto writer = makeSingleKeyWriter(topic, "elem3", "writer1", config);
                writer.waitForReaders();
                while(!readers.getNextUnread().getValue());
                writers.update(true);
            }
            {
                auto writer = makeSingleKeyWriter(topic, "elem4", "writer2", config);
                while(!readers.getNextUnread().getValue());
            }
        }

        {
            while(readers.getNextUnread().getValue());
            writers.update(false);
            auto writer = makeAnyKeyWriter(topic, "", config);
            promise<bool> p1, p2, p3;
            writer.onConnected([&p1, &p2, &p3](ConnectedAction action, vector<string> readers)
            {
                if(action == ConnectedAction::Initialize)
                {
                    p1.set_value(readers.empty());
                }
                else if(action == ConnectedAction::Add)
                {
                    p2.set_value(!readers.empty() && readers[0] == "reader1");
                }
                else if(action == ConnectedAction::Remove)
                {
                    p3.set_value(!readers.empty() && readers[0] == "reader1");
                }
            });
            test(p1.get_future().get());
            writers.update(true);
            test(p2.get_future().get());
            writer.update("anyelem1", "value2");
            while(!readers.getNextUnread().getValue());
            test(p3.get_future().get());
        }
        {
            writers.update(false);
            auto writer = makeSingleKeyWriter(topic, "anyelem3", "writer1", config);
            writer.waitForReaders();
            while(!readers.getNextUnread().getValue());
            writers.update(true);
        }
    }
    cout << "ok" << endl;

    return 0;
}
