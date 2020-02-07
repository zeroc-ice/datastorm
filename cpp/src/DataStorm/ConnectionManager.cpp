//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#include <DataStorm/ConnectionManager.h>
#include <DataStorm/CallbackExecutor.h>

using namespace std;
using namespace DataStormI;

ConnectionManager::ConnectionManager(const shared_ptr<CallbackExecutor>& executor) :
    _executor(executor)
{
}

void
ConnectionManager::add(const shared_ptr<void>& object,
                       const shared_ptr<Ice::Connection>& connection,
                       function<void(const shared_ptr<Ice::Connection>&, exception_ptr)> callback)
{
    lock_guard<mutex> lock(_mutex);
    auto& objects = _connections[connection];
    if(objects.empty())
    {
        connection->setCloseCallback([self=shared_from_this()](const shared_ptr<Ice::Connection>& con)
        {
            self->remove(con);
        });
    }
    objects.emplace(move(object), move(callback));
}

void
ConnectionManager::remove(const shared_ptr<void>& object, const shared_ptr<Ice::Connection>& connection)
{
    lock_guard<mutex> lock(_mutex);
    auto p = _connections.find(connection);
    if(p == _connections.end())
    {
        return;
    }
    auto& objects = p->second;
    objects.erase(object);
    if(objects.empty())
    {
        connection->setCloseCallback(nullptr);
        _connections.erase(p);
    }
}

void
ConnectionManager::remove(const shared_ptr<Ice::Connection>& connection)
{
    map<shared_ptr<void>, Callback> objects;
    {
        lock_guard<mutex> lock(_mutex);
        auto p = _connections.find(connection);
        if(p == _connections.end())
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
    catch(const std::exception&)
    {
        ex = current_exception();
    }
    for(const auto& object : objects)
    {
        try
        {
            object.second(connection, ex);
        }
        catch(const std::exception& ex)
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
    for(const auto& connection : _connections)
    {
        connection.first->setCloseCallback(nullptr);
    }
    _connections.clear();
}
