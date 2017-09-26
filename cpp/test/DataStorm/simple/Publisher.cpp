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

int
main(int argc, char* argv[])
{
    auto factory = DataStorm::createTopicFactory(argc, argv);

    cout << "testing string... " << flush;
    {
        auto topic = factory->createTopicWriter<string, string>("string");
        auto writer = topic->getDataWriter("elem1");

        writer->waitForReaders(1);
        test(writer->hasReaders());

        writer->add("value1");
        writer->update("value2");
        writer->remove();

        writer->waitForNoReaders();
        writer->destroy();
    }
    cout << "ok" << endl;

    cout << "testing struct... " << flush;
    {
        auto topic = factory->createTopicWriter<int, Test::StructValue>("struct");
        auto writer = topic->getDataWriter(10);

        writer->waitForReaders(1);
        test(writer->hasReaders());

        writer->add({"firstName", "lastName", 10});
        writer->update({"firstName", "lastName", 11});
        writer->remove();

        writer->waitForNoReaders();
        writer->destroy();
    }
    cout << "ok" << endl;

    cout << "testing class... " << flush;
    {
        auto topic = factory->createTopicWriter<string, Test::BasePtr>("baseclass");
        auto writer = topic->getDataWriter("elem1");

        writer->waitForReaders(1);
        test(writer->hasReaders());

        writer->add(make_shared<Test::Base>("value1"));
        writer->update(make_shared<Test::Base>("value2"));
        writer->remove();

        writer->waitForNoReaders();
        writer->destroy();
    }
    cout << "ok" << endl;

    cout << "testing topic updates... " << flush;
    {
        auto topic = factory->createTopicWriter<string, Test::BasePtr>("baseclass2");

        auto writer = topic->getDataWriter("elem1");
        writer->waitForReaders(1);
        test(writer->hasReaders());

        writer->add(make_shared<Test::Base>("value1"));
        writer->update(make_shared<Test::Base>("value2"));
        writer->remove();

        writer = topic->getDataWriter("elem2");
        writer->waitForReaders(1);
        writer->update(make_shared<Test::Base>("value1"));

        writer = topic->getDataWriter("elem3");
        writer->waitForReaders(1);
        writer->remove();

        writer = topic->getDataWriter("elem4");
        writer->waitForReaders(1);
        writer->add(make_shared<Test::Base>("value1"));

        writer->waitForNoReaders();
    }
    cout << "ok" << endl;

    cout << "testing filtered reader... " << flush;
    {
        auto topic = factory->createTopicWriter<string, Test::BasePtr>("baseclass3");

        auto writer = topic->getDataWriter("elem1");
        writer->waitForReaders(1);
        test(writer->hasReaders());

        writer->add(make_shared<Test::Base>("value1"));
        writer->update(make_shared<Test::Base>("value2"));
        writer->remove();
        writer->destroy();

        writer = topic->getDataWriter("elem2");
        writer->waitForReaders(1);
        writer->update(make_shared<Test::Base>("value1"));
        writer->destroy();

        writer = topic->getDataWriter("elem3");
        writer->waitForReaders(1);
        writer->remove();
        writer->destroy();

        writer = topic->getDataWriter("elem4");
        writer->waitForReaders(1);
        writer->add(make_shared<Test::Base>("value1"));

        writer->waitForNoReaders();
        writer->destroy();
    }
    cout << "ok" << endl;

    cout << "testing filtered writer... " << flush;
    {
        auto topic = factory->createTopicWriter<string, Test::BasePtr>("baseclass4");

        auto writer = topic->getFilteredDataWriter("elema[0-9]");
        writer->waitForReaders(1);
        test(writer->hasReaders());
        writer->add(make_shared<Test::Base>("value1"));
        writer->update(make_shared<Test::Base>("value2"));
        writer->remove();

        writer = topic->getFilteredDataWriter("elemb[0-9]");
        writer->waitForReaders(1);
        writer->update(make_shared<Test::Base>("value1"));
        writer->destroy();

        writer = topic->getFilteredDataWriter("elemc[0-9]");
        writer->waitForReaders(1);
        writer->remove();
        writer->destroy();

        writer = topic->getFilteredDataWriter("elemd[0-9]");
        writer->waitForReaders(1);
        writer->add(make_shared<Test::Base>("value1"));
        writer->destroy();

        writer = topic->getFilteredDataWriter("elem[0-9]");
        writer->waitForReaders(5);
        writer->update(make_shared<Test::Base>("value1"));
        writer->destroy();
    }
    cout << "ok" << endl;

    factory->destroy();
    return 0;
}
