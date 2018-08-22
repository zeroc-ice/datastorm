// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/SessionManager.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormI;

SessionManager::SessionManager(const shared_ptr<CallbackExecutor>& executor) : _executor(executor)
{
}

void
SessionManager::add(const shared_ptr<SessionI>& session, shared_ptr<Ice::Connection> connection)
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
SessionManager::remove(const shared_ptr<SessionI>& session, shared_ptr<Ice::Connection> connection)
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
    set<shared_ptr<SessionI>> sessions;
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
        session->disconnected(connection, ex);
    }
    _executor->flush();
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
