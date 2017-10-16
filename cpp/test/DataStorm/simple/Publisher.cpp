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

using namespace std;
using namespace DataStorm;

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    cout << "testing key data reader/writer... " << flush;
    {
        {
            TopicWriter<string, string> topic(node, "string");
            KeyDataWriter<string, string> writer(topic, "elem1");

            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add("value1");
            writer.update("value2");
            writer.remove();

            writer.waitForNoReaders();
        }
        {
            TopicWriter<int, Test::StructValue> topic(node, "struct");
            KeyDataWriter<int, Test::StructValue> writer(topic, 10);

            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add({"firstName", "lastName", 10});
            writer.update({"firstName", "lastName", 11});
            writer.remove();

            writer.waitForNoReaders();
        }
        {
            TopicWriter<string, shared_ptr<Test::Base>> topic(node, "baseclass");
            KeyDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elem1");

            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add(make_shared<Test::Base>("value1"));
            writer.update(make_shared<Test::Base>("value2"));
            writer.remove();

            writer.waitForNoReaders();
        }
        {
            TopicWriter<string, shared_ptr<Test::Base>> topic(node, "baseclass2");
            {
                KeyDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elem1");
                writer.waitForReaders(1);
                test(writer.hasReaders());

                writer.add(make_shared<Test::Base>("value1"));
                writer.update(make_shared<Test::Base>("value2"));
                writer.remove();
                writer.waitForNoReaders();
            }
            {
                KeyDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elem2");
                writer.waitForReaders(1);
                writer.update(make_shared<Test::Base>("value1"));
                writer.waitForNoReaders();
            }
            {
                KeyDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elem3");
                writer.waitForReaders(1);
                writer.remove();
                writer.waitForNoReaders();
            }
            {
                KeyDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elem4");
                writer.waitForReaders(1);
                writer.add(make_shared<Test::Base>("value1"));
                writer.waitForNoReaders();
            }
        }
    }
    cout << "ok" << endl;

    cout << "testing multi-key data reader/writer... " << flush;
    {
        {
            TopicWriter<string, string> topic(node, "multikey1");
            KeyDataWriter<string, string> writer(topic, vector<string> { "elem1", "elem2" });

            writer.add("value1");
            writer.update("value2");
            writer.remove();

            writer.waitForReaders(2);
            writer.waitForNoReaders();

            writer.waitForReaders(1);
            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing filtered reader... " << flush;
    {
        TopicWriter<string, shared_ptr<Test::Base>> topic(node, "baseclass3");

        KeyDataWriter<string, shared_ptr<Test::Base>> writer1(topic, "elem1");
        writer1.waitForReaders(1);
        test(writer1.hasReaders());
        writer1.add(make_shared<Test::Base>("value1"));
        writer1.update(make_shared<Test::Base>("value2"));
        writer1.remove();

        KeyDataWriter<string, shared_ptr<Test::Base>> writer2(topic, "elem2");
        writer2.waitForReaders(1);
        writer2.update(make_shared<Test::Base>("value1"));

        KeyDataWriter<string, shared_ptr<Test::Base>> writer3(topic, "elem3");
        writer3.waitForReaders(1);
        writer3.remove();

        KeyDataWriter<string, shared_ptr<Test::Base>> writer4(topic, "elem4");
        writer4.waitForReaders(1);
        writer4.add(make_shared<Test::Base>("value1"));
        writer4.waitForNoReaders();
    }
    {
        TopicWriter<string, string, RegexKeyValueFilter<string, string>> topic(node, "filtered reader key/value filter");

        KeyDataWriter<string, string> writer1(topic, "elem1");
        writer1.waitForReaders(3);
        test(writer1.hasReaders());
        writer1.add("value1");
        writer1.update("value2");
        writer1.remove();

        KeyDataWriter<string, string> writer2(topic, "elem2");
        writer2.waitForReaders(1);
        writer2.update("value1");
        writer2.update("value2");
        writer2.update("value3");
        writer2.update("value4");
        writer2.update("value5");
    }
    cout << "ok" << endl;

    cout << "testing filtered writer... " << flush;
    {
        TopicWriter<string, shared_ptr<Test::Base>> topic(node, "baseclass4");
        {
            FilteredDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elema[0-9]");
            writer.waitForReaders(1);
            test(writer.hasReaders());
            writer.add(make_shared<Test::Base>("value1"));
            writer.update(make_shared<Test::Base>("value2"));
            writer.remove();
            writer.waitForNoReaders();
        }
        {
            FilteredDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elemb[0-9]");
            writer.waitForReaders(1);
            writer.update(make_shared<Test::Base>("value1"));
            writer.waitForNoReaders();
        }
        {
            FilteredDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elemc[0-9]");
            writer.waitForReaders(1);
            writer.remove();
            writer.waitForNoReaders();
        }
        {
            FilteredDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elemd[0-9]");
            writer.waitForReaders(1);
            writer.add(make_shared<Test::Base>("value1"));
            writer.waitForNoReaders();
        }
        {
            FilteredDataWriter<string, shared_ptr<Test::Base>> writer(topic, "elem[0-9]");
            writer.waitForReaders(5);
            writer.update(make_shared<Test::Base>("value1"));
            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing topic reader/writer... " << flush;
    {
        TopicWriter<string, string> t1(node, "topic");
        TopicWriter<string, string> t2(node, "topic");
        t1.waitForReaders(2);
        t2.waitForReaders(2);
    }
    cout << "ok" << endl;

    return 0;
}
