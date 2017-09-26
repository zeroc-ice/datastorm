
// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/DataStorm.h>

using namespace std;
using namespace DataStorm;

TopicFactoryHolder::TopicFactoryHolder()
{
}

TopicFactoryHolder::TopicFactoryHolder(shared_ptr<TopicFactory> factory) :
    _factory(std::move(factory))
{
}

TopicFactoryHolder&
TopicFactoryHolder::operator=(shared_ptr<TopicFactory> factory)
{
    if(_factory)
    {
        _factory->destroy();
    }
    _factory = std::move(factory);
    return *this;
}

TopicFactoryHolder&
TopicFactoryHolder::operator=(TopicFactoryHolder&& other)
{
    if(_factory)
    {
        _factory->destroy();
    }
    _factory = std::move(other._factory);
    return *this;
}

TopicFactoryHolder::~TopicFactoryHolder()
{
    if(_factory)
    {
        _factory->destroy();
    }
}

TopicFactoryHolder::operator bool() const
{
    return _factory != nullptr;
}

const std::shared_ptr<TopicFactory>&
TopicFactoryHolder::factory() const
{
    return _factory;
}

const std::shared_ptr<TopicFactory>&
TopicFactoryHolder::operator->() const
{
    return _factory;
}

std::shared_ptr<TopicFactory>
TopicFactoryHolder::release()
{
    return std::move(_factory);
}
