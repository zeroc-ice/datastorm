// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>

#include <Ice/Ice.h>

namespace DataStormI
{

class Instance;

class Forwarder
{
public:

    virtual void forward(const Ice::ByteSeq&, const Ice::Current&) const = 0;
};

class ForwarderManager : public Ice::Blobject
{
public:

    ForwarderManager(const std::shared_ptr<Ice::ObjectAdapter>&);

    std::shared_ptr<Ice::ObjectPrx> add(const std::shared_ptr<Forwarder>&);
    void remove(const Ice::Identity&);

private:

    virtual bool ice_invoke(Ice::ByteSeq, Ice::ByteSeq&, const Ice::Current&);

    const std::shared_ptr<Ice::ObjectAdapter> _adapter;

    std::mutex _mutex;
    std::map<std::string, std::shared_ptr<Forwarder>> _forwarders;
    unsigned int _nextId;
};

}
