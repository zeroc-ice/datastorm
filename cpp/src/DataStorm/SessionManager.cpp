// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/SessionManager.h>
#include <DataStorm/SessionI.h>

using namespace std;
using namespace DataStormInternal;

SessionManager::SessionManager()
{
}

void
SessionManager::add(SessionI* session, shared_ptr<Ice::Connection> connection)
{
    lock_guard<mutex> lock(_mutex);
    auto& sessions = _connections[connection];
    if(sessions.empty())
    {
        connection->setCloseCallback([=](const shared_ptr<Ice::Connection>& con)
            {
                remove(con);
            });
    }
    sessions.insert(session);
}

void
SessionManager::remove(SessionI* session, shared_ptr<Ice::Connection> connection)
{
    lock_guard<mutex> lock(_mutex);
    auto& sessions = _connections[connection];
    if(sessions.empty())
    {
        _connections.erase(connection);
    }
    sessions.erase(session);
}

void
SessionManager::remove(shared_ptr<Ice::Connection> connection)
{
    set<SessionI*> sessions;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _connections.find(connection);
        sessions.swap(p->second);
        connection->setCloseCallback(nullptr);
        _connections.erase(p);
    }
    exception_ptr ex;
    try
    {
        connection->getInfo();
    }
    catch(const std::exception&)
    {
        ex = current_exception();
    }
    for(const auto& session : sessions)
    {
        session->disconnected(ex);
    }
}

void
SessionManager::destroy()
{
    lock_guard<mutex> lock(_mutex);
    for(const auto& connection : _connections)
    {
        connection.first->setCloseCallback(nullptr);
    }
    _connections.clear();
}
