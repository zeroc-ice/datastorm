// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Ice.h>

namespace DataStormInternal
{

class SessionI;

class SessionManager
{
public:

    SessionManager();

    void add(SessionI*, std::shared_ptr<Ice::Connection>);
    void remove(SessionI*, std::shared_ptr<Ice::Connection>);
    void remove(std::shared_ptr<Ice::Connection>);
    void destroy();

private:

    std::mutex _mutex;
    std::map<std::shared_ptr<Ice::Connection>, std::set<SessionI*>> _connections;
};

}
