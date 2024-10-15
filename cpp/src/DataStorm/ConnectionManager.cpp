//
// Copyright (c) ZeroC, Inc. All rights reserved.
//

#include "ConnectionManager.h"
#include "CallbackExecutor.h"

#include <iostream>

using namespace std;
using namespace DataStormI;

ConnectionManager::ConnectionManager(const shared_ptr<CallbackExecutor>& executor) : _executor(executor) {}

void
ConnectionManager::add(
    const Ice::ConnectionPtr& connection,
    shared_ptr<void> object,
    function<void(const Ice::ConnectionPtr&, exception_ptr)> callback)
{
    lock_guard<mutex> lock(_mutex);
    auto& objects = _connections[connection];
    if (objects.empty())
    {
        connection->setCloseCallback([self = shared_from_this()](const Ice::ConnectionPtr& con) { self->remove(con); });
    }
    objects.emplace(std::move(object), std::move(callback));
}

void
ConnectionManager::remove(const shared_ptr<void>& object, const Ice::ConnectionPtr& connection)
{
    lock_guard<mutex> lock(_mutex);
    auto p = _connections.find(connection);
    if (p == _connections.end())
    {
        return;
    }
    auto& objects = p->second;
    objects.erase(object);
    if (objects.empty())
    {
        connection->setCloseCallback(nullptr);
        _connections.erase(p);
    }
}

void
ConnectionManager::remove(const Ice::ConnectionPtr& connection)
{
    map<shared_ptr<void>, Callback> objects;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _connections.find(connection);
        if (p == _connections.end())
        {
            return;
        }
        objects.swap(p->second);
        connection->setCloseCallback(nullptr);
        _connections.erase(p);
    }
    exception_ptr ex;
    try
    {
        connection->getInfo();
    }
    catch (const std::exception&)
    {
        ex = current_exception();
    }
    for (const auto& object : objects)
    {
        try
        {
            object.second(connection, ex);
        }
        catch (const std::exception& ex)
        {
            cerr << ex.what() << endl;
            assert(false);
            throw;
        }
    }
    _executor->flush();
}

void
ConnectionManager::destroy()
{
    lock_guard<mutex> lock(_mutex);
    for (const auto& connection : _connections)
    {
        connection.first->setCloseCallback(nullptr);
    }
    _connections.clear();
}
