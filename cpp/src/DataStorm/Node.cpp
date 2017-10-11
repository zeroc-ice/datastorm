// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <Ice/Initialize.h>

#include <DataStorm/Node.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TopicFactoryI.h>

using namespace std;
using namespace DataStorm;

Node::Node(const std::shared_ptr<Ice::Communicator>& communicator) :
    _ownsCommunicator(communicator == nullptr)
{
    _instance = make_shared<DataStormInternal::Instance>(communicator);
    _instance->init();

    _factory = _instance->getTopicFactory();
}

Node::Node(int& argc, char* argv[]) :
    _ownsCommunicator(true)
{
    auto communicator = Ice::initialize(argc, argv);
    auto args = Ice::argsToStringSeq(argc, argv);
    communicator->getProperties()->parseCommandLineOptions("DataStorm", args);
    Ice::stringSeqToArgs(args, argc, argv);

    _instance = make_shared<DataStormInternal::Instance>(communicator);
    _instance->init();

    _factory = _instance->getTopicFactory();
}

Node::~Node()
{
    _instance->destroy(_ownsCommunicator);
}

std::shared_ptr<Ice::Communicator>
Node::getCommunicator() const
{
    return _instance->getCommunicator();
}
