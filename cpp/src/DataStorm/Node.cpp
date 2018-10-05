// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
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

void
Node::shutdown()
{
    _instance->shutdown();
}

bool
Node::isShutdown()
{
    return _instance->isShutdown();
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
    return _instance ? _instance->getCommunicator() : nullptr;
}

shared_ptr<Ice::Connection>
Node::getSessionConnection(const string& ident) const
{
    return _instance ? _instance->getNode()->getSessionConnection(ident) : nullptr;
}
