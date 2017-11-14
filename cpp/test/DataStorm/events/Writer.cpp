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

    cout << "testing single key reader/writer... " << flush;
    {
        {
            Topic<string, string> topic(node, "string");
            KeyWriter<string, string> writer(topic, "elem1");

            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add("value1");
            writer.update("value2");
            writer.remove();

            writer.waitForNoReaders();

            KeyWriter<string, string> writer2(topic, "elem2");
            writer2.waitForReaders(2);
            writer2.add("value");
            writer2.waitForNoReaders();
        }
        {
            Topic<int, Test::StructValue> topic(node, "struct");
            KeyWriter<int, Test::StructValue> writer(topic, 10);

            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add({"firstName", "lastName", 10});
            writer.update({"firstName", "lastName", 11});
            writer.remove();

            writer.waitForNoReaders();
        }
        {
            Topic<string, shared_ptr<Test::Base>> topic(node, "baseclass");
            KeyWriter<string, shared_ptr<Test::Base>> writer(topic, "elem1");

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
                KeyWriter<string, shared_ptr<Test::Base>> writer(topic, "elem1");
                writer.waitForReaders(1);
                test(writer.hasReaders());

                writer.add(make_shared<Test::Base>("value1"));
                writer.update(make_shared<Test::Base>("value2"));
                writer.remove();
                writer.waitForNoReaders();
            }
            {
                KeyWriter<string, shared_ptr<Test::Base>> writer(topic, "elem2");
                writer.waitForReaders(1);
                writer.update(make_shared<Test::Base>("value1"));
                writer.waitForNoReaders();
            }
            {
                KeyWriter<string, shared_ptr<Test::Base>> writer(topic, "elem3");
                writer.waitForReaders(1);
                writer.remove();
                writer.waitForNoReaders();
            }
            {
                KeyWriter<string, shared_ptr<Test::Base>> writer(topic, "elem4");
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
            KeyWriter<string, string> writer1(topic, "elem1");
            KeyWriter<string, string> writer2(topic, "elem2");

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
            Topic<string, string> topic(node, "anykey");
            KeyWriter<string, string> writer1(topic, "elem1");
            KeyWriter<string, string> writer2(topic, "elem2");

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

    cout << "testing filtered reader... " << flush;
    {
        Topic<string, shared_ptr<Test::Base>, RegexFilter<string>, string> topic(node, "baseclass3");

        KeyWriter<string, shared_ptr<Test::Base>> writer5(topic, "elem5");
        writer5.add(make_shared<Test::Base>("value1"));
        writer5.update(make_shared<Test::Base>("value2"));
        writer5.remove();

        KeyWriter<string, shared_ptr<Test::Base>> writer1(topic, "elem1");
        writer1.waitForReaders(1);
        test(writer1.hasReaders());
        writer1.add(make_shared<Test::Base>("value1"));
        writer1.update(make_shared<Test::Base>("value2"));
        writer1.remove();

        KeyWriter<string, shared_ptr<Test::Base>> writer2(topic, "elem2");
        writer2.waitForReaders(1);
        writer2.update(make_shared<Test::Base>("value1"));

        KeyWriter<string, shared_ptr<Test::Base>> writer3(topic, "elem3");
        writer3.waitForReaders(1);
        writer3.remove();

        KeyWriter<string, shared_ptr<Test::Base>> writer4(topic, "elem4");
        writer4.waitForReaders(1);
        writer4.add(make_shared<Test::Base>("value1"));
        writer4.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing filtered sample reader... " << flush;
    {
        Topic<string, string, RegexFilter<string>, string> topic(node, "filtered reader key/value filter");

        KeyWriter<string, string, SampleEventFilter<string, string>, SampleEventSeq> writer1(topic, "elem1");
        writer1.waitForReaders(3);
        test(writer1.hasReaders());
        writer1.add("value1");
        writer1.update("value2");
        writer1.remove();

        KeyWriter<string, string, RegexFilter<Sample<string, string>>, string> writer2(topic, "elem2");
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

        KeyWriter<string, string> writer(t1, "shutdown");
        writer.add("now");

        t1.waitForNoReaders();
        t2.waitForNoReaders();
    }
    cout << "ok" << endl;

    return 0;
}
