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
using namespace Test;

int
main(int argc, char* argv[])
{
    Node node(argc, argv);

    cout << "testing topic... " << flush;
    {
        Topic<int, string> t1(node, "t1");
        Topic<int, string> t2(node, "t2");
        Topic<StructKey, string> t3(node, "t3");
        Topic<ClassKey, string> t4(node, "t4");

        Topic<int, string>::KeyType k1 = 5;
        Topic<int, string>::ValueType v1("string");
        Topic<int, string>::UpdateTagType tag1("string");

        Topic<int, string>::WriterType* writer = nullptr;
        if(writer != nullptr)
        {
            test(writer->getConnectedKeys()[0] == k1); // Use variables to prevent unused variable warnings
        }
        Topic<int, string>::ReaderType* reader = nullptr;
        if(reader != nullptr)
        {
            reader->getConnectedKeys();
        }

        auto tc1 = move(t1);

        test(!tc1.hasWriters());
        tc1.waitForWriters(0);
        tc1.waitForNoWriters();

        test(!tc1.hasReaders());
        tc1.waitForReaders(0);
        tc1.waitForNoReaders();

        tc1.setWriterDefaultConfig(WriterConfig());
        tc1.setReaderDefaultConfig(ReaderConfig());

        tc1.setUpdater<string>("test", [](string& value, string v) {});
    }
    cout << "ok" << endl;

    cout << "testing writer... " << flush;
    {
        Topic<string, string> topic(node, "topic");
        auto skw = makeSingleKeyWriter(topic, "key");
        skw = makeSingleKeyWriter(topic, "key", WriterConfig());

        auto mkw = makeMultiKeyWriter(topic, { "key" });
        mkw = makeMultiKeyWriter(topic, { "key" }, WriterConfig());

        auto akw = makeAnyKeyWriter(topic);
        akw = makeAnyKeyWriter(topic, WriterConfig());
    }
    cout << "ok" << endl;

    cout << "testing reader... " << flush;
    {
        Topic<string, string> topic(node, "topic");
        auto skw = makeSingleKeyReader(topic, "key");
        skw = makeSingleKeyReader(topic, "key", ReaderConfig());

        auto mkw = makeMultiKeyReader(topic, { "key" });
        mkw = makeMultiKeyReader(topic, { "key" }, ReaderConfig());

        auto akw = makeAnyKeyReader(topic);
        akw = makeAnyKeyReader(topic, ReaderConfig());
    }
    cout << "ok" << endl;

    return 0;
}
