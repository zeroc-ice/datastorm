// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/CommunicatorF.h>
#include <Ice/LoggerF.h>
#include <Ice/LoggerUtil.h>

#include <DataStorm/InternalI.h>
#include <DataStorm/SessionI.h>
#include <DataStorm/DataElementI.h>

namespace Ice
{

template<typename T>
inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const std::vector<T>& p)
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

template<typename K, typename V>
inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const std::map<K, V>& p)
{
    if(!p.empty())
    {
        for(auto q = p.begin(); q != p.end(); ++q)
        {
            if(q != p.begin())
            {
                os << ", ";
            }
            os << q->first << "=" << q->second;
        }
    }
    return os;
}

template<typename T, typename ::std::enable_if<::std::is_base_of<DataStormInternal::Element, T>::value>::type* = nullptr>
inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const std::shared_ptr<T>& p)
{
    return os << (p ? p->toString() : "");
}

template<typename T, typename ::std::enable_if<::std::is_base_of<DataStormInternal::Topic, T>::value>::type* = nullptr>
inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, T* topic)
{
    if(topic)
    {
        return os << topic->getName() << ":" << topic->getId();
    }
    else
    {
        return os << "<null>";
    }
}

template<typename T, typename ::std::enable_if<::std::is_base_of<DataStormInternal::DataElementI, T>::value>::type* = nullptr>
inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, T* element)
{
    return os << (element ? element->toString() : "<null>");
}

template<typename T, typename ::std::enable_if<::std::is_base_of<DataStormInternal::SessionI, T>::value>::type* = nullptr>
inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, T* session)
{
    if(session)
    {
        Ice::Identity id = session->getNode()->ice_getIdentity();
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

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const Ice::Identity& id)
{
    return os << (id.category.empty() ? "" : id.category + "/") << id.name;
}

inline std::string
valueIdToString(long long int valueId)
{
    std::ostringstream os;
    if(valueId < 0)
    {
        os << "f" << -valueId;
    }
    else
    {
        os << "k" << valueId;
    }
    return os.str();
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::ElementInfo& info)
{
    return os << valueIdToString(info.valueId);
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::ElementData& data)
{
    os << 'e' << data.id;
    if(!data.facet.empty())
    {
        os << ':' << data.facet;
    }
    return os;
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::ElementSpec& spec)
{
    for(auto q = spec.elements.begin(); q != spec.elements.end(); ++q)
    {
        if(q != spec.elements.begin())
        {
            os << ',';
        }
        os << *q << ':' << valueIdToString(spec.valueId) << ":pv" << valueIdToString(spec.peerValueId);
    }
    return os;
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::ElementDataAck& data)
{
    os << 'e' << data.id << ":pe" << data.peerId;
    if(!data.facet.empty())
    {
        os << ':' << data.facet;
    }
    if(!data.samples.empty())
    {
        os << ":sz" << data.samples.size();
    }
    return os;
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::ElementSpecAck& spec)
{
    for(auto q = spec.elements.begin(); q != spec.elements.end(); ++q)
    {
        if(q != spec.elements.begin())
        {
            os << ',';
        }
        os << *q << ':' << valueIdToString(spec.valueId) << ":pv" << valueIdToString(spec.peerValueId);
    }
    return os;
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::TopicInfo& info)
{
    os << "[" << info.ids << "]:" << info.name;
    return os;
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::TopicSpec& info)
{
    os << '[' << info.elements << "]@" << info.id << ':' << info.name;
    return os;
}

inline LoggerOutputBase&
operator<<(LoggerOutputBase& os, const DataStormContract::DataSamples& samples)
{
    return os << 'e' << samples.id << ":sz" << samples.samples.size();
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

class Warning : public Ice::Warning
{
public:

    Warning(std::shared_ptr<TraceLevels> traceLevels) :
        Ice::Warning(traceLevels->logger)
    {
    }
};

}
