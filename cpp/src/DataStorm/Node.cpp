// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <Ice/Initialize.h>

#include <DataStorm/Node.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TopicFactoryI.h>
#include <DataStorm/NodeI.h>

using namespace std;
using namespace DataStorm;

Node::Node(const std::shared_ptr<Ice::Communicator>& communicator) :
    _ownsCommunicator(communicator == nullptr)
{
    _instance = make_shared<DataStormI::Instance>(communicator == nullptr ? Ice::initialize() : communicator);
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

    _instance = make_shared<DataStormI::Instance>(communicator);
    _instance->init();

    _factory = _instance->getTopicFactory();
}

Node::Node(Node&& node)
{
    _instance = move(node._instance);
    _factory = move(node._factory);
    _ownsCommunicator = move(node._ownsCommunicator);
}

Node::~Node()
{
    if(_instance)
    {
        _instance->destroy(_ownsCommunicator);
    }
}

Node&
Node::operator=(Node&& node)
{
    _instance = move(node._instance);
    _factory = move(node._factory);
    _ownsCommunicator = move(node._ownsCommunicator);
    return *this;
}

shared_ptr<Ice::Communicator>
Node::getCommunicator() const
{
    return _instance->getCommunicator();
}

shared_ptr<Ice::Connection>
Node::getSessionConnection(const string& ident) const
{
    return _instance->getNode()->getSessionConnection(ident);
}
