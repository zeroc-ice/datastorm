// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/CommunicatorF.h>
#include <Ice/LoggerF.h>
#include <Ice/LoggerUtil.h>

#include <DataStorm/DataStorm.h>
#include <DataStorm/SessionI.h>

namespace Ice
{

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const std::shared_ptr<DataStormInternal::Key>& p)
{
    return os << (p ? p->toString() : "");
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::StringSeq& p)
{
    if(!p.empty())
    {
        for(auto q = p.begin(); q != p.end(); ++q)
        {
            if(q != p.begin())
            {
                os << ", ";
            }
            os << *q;
        }
    }
    return os;
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const Ice::Identity& id)
{
    return os << (id.category.empty() ? "" : id.category + "/") << id.name;
}

template<typename T, typename ::std::enable_if<::std::is_base_of<DataStormInternal::Topic, T>::value>::type* = nullptr>
inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, T* topic)
{
    return os << (topic ? topic->getName() : "<null>");
}

template<typename T, typename ::std::enable_if<::std::is_base_of<DataStormInternal::SessionI, T>::value>::type* = nullptr>
inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, T* session)
{
    if(session)
    {
        Ice::Identity id = session->getPeer()->ice_getIdentity();
        os << id.name;
        if(!id.category.empty())
        {
            os << '/' << id.category;
        }
        return os;
    }
    else
    {
        return os << "<null>";
    }
}

}

namespace DataStormInternal
{

class TraceLevels
{
public:

    TraceLevels(std::shared_ptr<Ice::Communicator>);

    const int topic;
    const char* topicCat;

    const int data;
    const char* dataCat;

    const int session;
    const char* sessionCat;

    const std::shared_ptr<Ice::Logger> logger;
};

class Trace : public Ice::Trace
{
public:

    Trace(std::shared_ptr<TraceLevels> traceLevels, const std::string& category) :
        Ice::Trace(traceLevels->logger, category)
    {
    }
};

}
