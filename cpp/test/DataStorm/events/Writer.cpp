// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

#include <Test.h>
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

    cout << "testing single key reader/writer... " << flush;
    {
        {
            Topic<string, string> topic(node, "string");
            auto writer = makeSingleKeyWriter(topic, "elem1", config);

            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add("value1");
            writer.update("value2");
            writer.remove();

            writer.waitForNoReaders();

            auto writer2 = makeSingleKeyWriter(topic, "elem2", config);
            writer2.waitForReaders(2);
            writer2.add("value");
            writer2.waitForNoReaders();
        }
        {
            Topic<int, Test::StructValue> topic(node, "struct");
            auto writer = makeSingleKeyWriter(topic, 10, config);

            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add({"firstName", "lastName", 10});
            writer.update({"firstName", "lastName", 11});
            writer.remove();

            writer.waitForNoReaders();
        }
        {
            Topic<string, shared_ptr<Test::Base>> topic(node, "baseclass");
            auto writer = makeSingleKeyWriter(topic, "elem1", config);

            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add(make_shared<Test::Base>("value1"));
            writer.update(make_shared<Test::Base>("value2"));
            writer.remove();

            writer.waitForNoReaders();
        }
        {
            Topic<string, shared_ptr<Test::Base>> topic(node, "baseclass2");
            {
                auto writer = makeSingleKeyWriter(topic, "elem1", config);
                writer.waitForReaders(1);
                test(writer.hasReaders());

                writer.add(make_shared<Test::Base>("value1"));
                writer.update(make_shared<Test::Base>("value2"));
                writer.remove();
                writer.waitForNoReaders();
            }
            {
                auto writer = makeSingleKeyWriter(topic, "elem2", config);
                writer.waitForReaders(1);
                writer.update(make_shared<Test::Base>("value1"));
                writer.waitForNoReaders();
            }
            {
                auto writer = makeSingleKeyWriter(topic, "elem3", config);
                writer.waitForReaders(1);
                writer.remove();
                writer.waitForNoReaders();
            }
            {
                auto writer = makeSingleKeyWriter(topic, "elem4", config);
                writer.waitForReaders(1);
                writer.add(make_shared<Test::Base>("value1"));
                writer.waitForNoReaders();
            }
        }
    }
    cout << "ok" << endl;

    cout << "testing multi-key reader/writer... " << flush;
    {
        {
            Topic<string, string> topic(node, "multikey1");
            auto writer1 = makeSingleKeyWriter(topic, "elem1", config);
            auto writer2 = makeSingleKeyWriter(topic, "elem2", config);

            writer1.waitForReaders(1);
            writer2.waitForReaders(1);

            writer1.add("value1");
            writer1.update("value2");
            writer1.remove();

            writer2.add("value1");
            writer2.update("value2");
            writer2.remove();

            writer1.waitForNoReaders();
            writer2.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing any-key reader/writer... " << flush;
    {
        {
            Topic<string, string> topic(node, "anykey1");
            auto writer1 = makeSingleKeyWriter(topic, "elem1", config);
            auto writer2 = makeSingleKeyWriter(topic, "elem2", config);

            writer1.waitForReaders(1);
            writer2.waitForReaders(1);

            writer1.add("value1");
            writer1.update("value2");
            writer1.remove();

            writer2.add("value1");
            writer2.update("value2");
            writer2.remove();

            writer1.waitForNoReaders();
            writer2.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing reader/multi-key writer... " << flush;
    {
        {
            Topic<string, string> topic(node, "multikey2");
            auto writer = makeMultiKeyWriter(topic, {"elem1", "elem2"}, config);

            writer.waitForReaders(2);

            writer.add("elem1", "value1");
            writer.update("elem1", "value2");
            writer.remove("elem1");

            writer.add("elem2", "value1");
            writer.update("elem2", "value2");
            writer.remove("elem2");

            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing reader/any-key writer... " << flush;
    {
        {
            Topic<string, string> topic(node, "anykey2");
            auto writer = makeAnyKeyWriter(topic, config);

            writer.waitForReaders(1);

            writer.add("elem1", "value1");
            writer.update("elem1", "value2");
            writer.remove("elem1");

            writer.add("elem2", "value1");
            writer.update("elem2", "value2");
            writer.remove("elem2");

            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing multi-key reader/multi-key writer... " << flush;
    {
        {
            Topic<string, string> topic(node, "multikey3");
            auto writer = makeMultiKeyWriter(topic, {"elem1", "elem2"}, config);

            writer.waitForReaders(1);

            writer.add("elem1", "value1");
            writer.update("elem1", "value2");
            writer.remove("elem1");

            writer.add("elem2", "value1");
            writer.update("elem2", "value2");
            writer.remove("elem2");

            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing any-key reader/any-key writer... " << flush;
    {
        {
            Topic<string, string> topic(node, "anykey3");
            auto writer = makeAnyKeyWriter(topic, config);

            writer.waitForReaders(1);

            writer.add("elem1", "value1");
            writer.update("elem1", "value2");
            writer.remove("elem1");

            writer.add("elem2", "value1");
            writer.update("elem2", "value2");
            writer.remove("elem2");

            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing filtered reader/writer... " << flush;
    {
        Topic<string, shared_ptr<Test::Base>> topic(node, "filtered1");
        topic.setKeyFilter("regex", makeKeyRegexFilter(topic));
        auto writer5 = makeSingleKeyWriter(topic, "elem5", config);
        writer5.add(make_shared<Test::Base>("value1"));
        writer5.update(make_shared<Test::Base>("value2"));
        writer5.remove();

        auto writer1 = makeSingleKeyWriter(topic, "elem1", config);
        writer1.waitForReaders(1);
        test(writer1.hasReaders());
        writer1.add(make_shared<Test::Base>("value1"));
        writer1.update(make_shared<Test::Base>("value2"));
        writer1.remove();

        auto writer2 = makeSingleKeyWriter(topic, "elem2", config);
        writer2.waitForReaders(1);
        writer2.update(make_shared<Test::Base>("value1"));

        auto writer3 = makeSingleKeyWriter(topic, "elem3", config);
        writer3.waitForReaders(1);
        writer3.remove();

        auto writer4 = makeSingleKeyWriter(topic, "elem4", config);
        writer4.waitForReaders(1);
        writer4.add(make_shared<Test::Base>("value1"));
        writer4.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing filtered reader/multi-key writer... " << flush;
    {
        Topic<string, shared_ptr<Test::Base>> topic(node, "filtered2");
        topic.setKeyFilter("regex", makeKeyRegexFilter(topic));

        auto writer = makeMultiKeyWriter(topic, { "elem1", "elem2", "elem3", "elem4", "elem5" }, config);
        writer.waitForReaders(1);

        writer.add("elem5", make_shared<Test::Base>("value1"));
        writer.update("elem5", make_shared<Test::Base>("value2"));
        writer.remove("elem5");

        test(writer.hasReaders());
        writer.add("elem1", make_shared<Test::Base>("value1"));
        writer.update("elem1", make_shared<Test::Base>("value2"));
        writer.remove("elem1");

        writer.update("elem2", make_shared<Test::Base>("value1"));

        writer.remove("elem3");

        writer.add("elem4", make_shared<Test::Base>("value1"));
        writer.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing filtered reader/any-key writer... " << flush;
    {
        Topic<string, shared_ptr<Test::Base>> topic(node, "filtered3");
        topic.setKeyFilter("regex", makeKeyRegexFilter(topic));

        auto writer = makeAnyKeyWriter(topic, config);
        writer.waitForReaders(1);

        writer.add("elem5", make_shared<Test::Base>("value1"));
        writer.update("elem5", make_shared<Test::Base>("value2"));
        writer.remove("elem5");

        test(writer.hasReaders());
        writer.add("elem1", make_shared<Test::Base>("value1"));
        writer.update("elem1", make_shared<Test::Base>("value2"));
        writer.remove("elem1");

        writer.update("elem2", make_shared<Test::Base>("value1"));

        writer.remove("elem3");

        writer.add("elem4", make_shared<Test::Base>("value1"));
        writer.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing filtered sample reader... " << flush;
    {
        Topic<string, string> topic(node, "filtered reader key/value filter");
        topic.setKeyFilter("regex", makeKeyRegexFilter(topic));
        topic.setSampleFilter("regex", makeSampleRegexFilter(topic));
        topic.setSampleFilter("event", makeSampleEventFilter(topic));

        auto writer1 = makeSingleKeyWriter(topic, "elem1", config);
        writer1.waitForReaders(3);
        test(writer1.hasReaders());
        writer1.add("value1");
        writer1.update("value2");
        writer1.remove();

        auto writer2 = makeSingleKeyWriter(topic, "elem2", config);
        writer2.waitForReaders(1);
        writer2.update("value1");
        writer2.update("value2");
        writer2.update("value3");
        writer2.update("value4");
        writer2.update("value5");

        writer1.waitForNoReaders();
        writer2.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing topic reader/writer... " << flush;
    {
        Topic<string, string> t1(node, "topic");
        Topic<string, string> t2(node, "topic");
        t1.hasReaders(); // Required to create the underlying topic writer
        t2.hasReaders(); // Required to create the underlying topic writer
        t1.waitForReaders(2);
        t2.waitForReaders(2);

        auto writer = makeSingleKeyWriter(t1, "shutdown", config);
        writer.add("now");

        t1.waitForNoReaders();
        t2.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing topic collocated key reader and writer... " << flush;
    {
        Topic<string, string> topic(node, "topic");
        {
            auto writer = makeSingleKeyWriter(topic, "test");
            writer.add("add");

            auto reader = makeSingleKeyReader(topic, "test");
            test(reader.getNextUnread().getValue() == "add");
        }
        {
            auto reader = makeSingleKeyReader(topic, "test");

            auto writer = makeSingleKeyWriter(topic, "test");
            writer.update("update");

            test(reader.getNextUnread().getValue() == "update");
        }
    }
    cout << "ok" << endl;

    return 0;
}
