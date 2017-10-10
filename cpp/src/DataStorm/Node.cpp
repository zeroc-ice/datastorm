// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/Node.h>
#include <Ice/Initialize.h>

using namespace DataStorm;

Node::Node(const std::shared_ptr<Ice::Communicator>& communicator) :
    _ownsCommunicator(communicator == nullptr)
{
    _impl = DataStormInternal::createTopicFactory(communicator ? communicator : Ice::initialize());
    _impl->init();
}

Node::Node(int& argc, char* argv[]) :
    _ownsCommunicator(true)
{
    auto communicator = Ice::initialize(argc, argv);
    auto args = Ice::argsToStringSeq(argc, argv);
    communicator->getProperties()->parseCommandLineOptions("DataStorm", args);
    Ice::stringSeqToArgs(args, argc, argv);

    _impl = DataStormInternal::createTopicFactory(communicator);
    _impl->init();
}

Node::~Node()
{
    _impl->destroy(_ownsCommunicator);
}

std::shared_ptr<Ice::Communicator>
Node::getCommunicator() const
{
    return _impl->getCommunicator();
}
