// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
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

namespace DataStorm
{

template<> struct DataTraits<Test::StructValue>
{
    using KeyType = int;
    using ValueType = Test::StructValue;
    using FilterType = std::string;
};

}

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    cout << "testing string... " << flush;
    {
        TopicWriter<string> topic(node, "string");
        KeyDataWriter<string> writer(topic, "elem1");

        writer.waitForReaders(1);
        test(writer.hasReaders());

        writer.add("value1");
        writer.update("value2");
        writer.remove();

        writer.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing struct... " << flush;
    {
        TopicWriter<Test::StructValue> topic(node, "struct");
        KeyDataWriter<Test::StructValue> writer(topic, 10);

        writer.waitForReaders(1);
        test(writer.hasReaders());

        writer.add({"firstName", "lastName", 10});
        writer.update({"firstName", "lastName", 11});
        writer.remove();

        writer.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing class... " << flush;
    {
        TopicWriter<shared_ptr<Test::Base>> topic(node, "baseclass");
        KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem1");

        writer.waitForReaders(1);
        test(writer.hasReaders());

        writer.add(make_shared<Test::Base>("value1"));
        writer.update(make_shared<Test::Base>("value2"));
        writer.remove();

        writer.waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing topic updates... " << flush;
    {
        TopicWriter<shared_ptr<Test::Base>> topic(node, "baseclass2");
        {
            KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem1");
            writer.waitForReaders(1);
            test(writer.hasReaders());

            writer.add(make_shared<Test::Base>("value1"));
            writer.update(make_shared<Test::Base>("value2"));
            writer.remove();
        }
        {
            KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem2");
            writer.waitForReaders(1);
            writer.update(make_shared<Test::Base>("value1"));
        }
        {
            KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem3");
            writer.waitForReaders(1);
            writer.remove();
        }
        {
            KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem4");
            writer.waitForReaders(1);
            writer.add(make_shared<Test::Base>("value1"));
            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing filtered reader... " << flush;
    {
        TopicWriter<shared_ptr<Test::Base>> topic(node, "baseclass3");
        {
            KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem1");
            writer.waitForReaders(1);
            test(writer.hasReaders());
            writer.add(make_shared<Test::Base>("value1"));
            writer.update(make_shared<Test::Base>("value2"));
            writer.remove();
        }
        {
            KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem2");
            writer.waitForReaders(1);
            writer.update(make_shared<Test::Base>("value1"));
        }
        {
            KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem3");
            writer.waitForReaders(1);
            writer.remove();
        }
        {
            KeyDataWriter<shared_ptr<Test::Base>> writer(topic, "elem4");
            writer.waitForReaders(1);
            writer.add(make_shared<Test::Base>("value1"));
            writer.waitForNoReaders();
        }
    }
    cout << "ok" << endl;

    cout << "testing filtered writer... " << flush;
    {
        TopicWriter<shared_ptr<Test::Base>> topic(node, "baseclass4");
        {
            FilteredDataWriter<shared_ptr<Test::Base>> writer(topic, "elema[0-9]");
            writer.waitForReaders(1);
            test(writer.hasReaders());
            writer.add(make_shared<Test::Base>("value1"));
            writer.update(make_shared<Test::Base>("value2"));
            writer.remove();
            writer.waitForNoReaders();
        }
        {
            FilteredDataWriter<shared_ptr<Test::Base>> writer(topic, "elemb[0-9]");
            writer.waitForReaders(1);
            writer.update(make_shared<Test::Base>("value1"));
            writer.waitForNoReaders();
        }
        {
            FilteredDataWriter<shared_ptr<Test::Base>> writer(topic, "elemc[0-9]");
            writer.waitForReaders(1);
            writer.remove();
            writer.waitForNoReaders();
        }
        {
            FilteredDataWriter<shared_ptr<Test::Base>> writer(topic, "elemd[0-9]");
            writer.waitForReaders(1);
            writer.add(make_shared<Test::Base>("value1"));
            writer.waitForNoReaders();
        }
        // {
        //     FilteredDataWriter<shared_ptr<Test::Base>> writer(topic, "elem[0-9]");
        //     writer.waitForReaders(5);
        //     writer.update(make_shared<Test::Base>("value1"));
        //     writer.waitForNoReaders();
        // }
    }
    cout << "ok" << endl;

    return 0;
}
