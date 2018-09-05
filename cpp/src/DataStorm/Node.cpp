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

Node::Node(std::shared_ptr<Ice::Communicator> communicator) :
    _ownsCommunicator(false)
{
    init(communicator);
}

void
Node::init(const std::shared_ptr<Ice::Communicator>& communicator)
{
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
