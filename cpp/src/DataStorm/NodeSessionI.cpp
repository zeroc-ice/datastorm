//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#include <DataStorm/NodeSessionI.h>
#include <DataStorm/NodeSessionManager.h>
#include <DataStorm/ConnectionManager.h>
#include <DataStorm/Instance.h>
#include <DataStorm/TraceUtil.h>

using namespace std;
using namespace DataStormI;
using namespace DataStormContract;

namespace
{

class NodeForwarderI : public Node, public enable_shared_from_this<NodeForwarderI>
{
public:

    NodeForwarderI(shared_ptr<NodeSessionManager> nodeSessionManager,
                   shared_ptr<NodeSessionI> session,
                   shared_ptr<NodePrx> node) :
        _nodeSessionManager(move(nodeSessionManager)),
        _session(move(session)),
        _node(move(node))
    {
    }

    virtual void initiateCreateSession(shared_ptr<NodePrx> publisher, const Ice::Current& current) override
    {
        auto session = _session.lock();
        if(!session)
        {
            return;
        }

        try
        {
            shared_ptr<SessionPrx> session;
            updateNodeAndSessionProxy(publisher, session, current);
            _node->initiateCreateSessionAsync(publisher);
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
        }
        catch(const Ice::CommunicatorDestroyedException&)
        {
        }
    }

    virtual void createSession(shared_ptr<NodePrx> subscriber,
                               shared_ptr<SubscriberSessionPrx> subscriberSession,
                               bool /* fromRelay */,
                               const Ice::Current& current) override
    {
        auto session = _session.lock();
        if(!session)
        {
            return;
        }

        try
        {
            updateNodeAndSessionProxy(subscriber, subscriberSession, current);
            session->addSession(subscriberSession);
            _node->createSessionAsync(subscriber, subscriberSession, true);
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
        }
        catch(const Ice::CommunicatorDestroyedException&)
        {
        }
    }

    virtual void confirmCreateSession(shared_ptr<NodePrx> publisher,
                                      shared_ptr<PublisherSessionPrx> publisherSession,
                                      const Ice::Current& current) override
    {
        auto session = _session.lock();
        if(!session)
        {
            return;
        }
        try
        {
            updateNodeAndSessionProxy(publisher, publisherSession, current);
            session->addSession(publisherSession);
            _node->confirmCreateSessionAsync(publisher, publisherSession);
        }
        catch(const Ice::ObjectAdapterDeactivatedException&)
        {
        }
        catch(const Ice::CommunicatorDestroyedException&)
        {
        }
    }

private:

    template<typename T> void updateNodeAndSessionProxy(shared_ptr<NodePrx>& node,
                                                        shared_ptr<T>& session,
                                                        const Ice::Current& current)
    {
        if(node->ice_getEndpoints().empty() && node->ice_getAdapterId().empty())
        {
            auto peerSession = _nodeSessionManager->createOrGet(node, current.con, false);
            assert(peerSession);
            node = peerSession->getPublicNode();
            if(session)
            {
                session = peerSession->getSessionForwarder(session);
            }
        }
    }

    const shared_ptr<NodeSessionManager> _nodeSessionManager;
    const weak_ptr<NodeSessionI> _session;
    const shared_ptr<NodePrx> _node;
};

}

NodeSessionI::NodeSessionI(shared_ptr<Instance> instance,
                           shared_ptr<NodePrx> node,
                           shared_ptr<Ice::Connection> connection,
                           bool forwardAnnouncements) :
    _instance(move(instance)),
    _traceLevels(_instance->getTraceLevels()),
    _node(move(node)),
    _connection(move(connection))
{
    if(forwardAnnouncements)
    {
        _lookup = Ice::uncheckedCast<LookupPrx>(_connection->createProxy({ "Lookup", "DataStorm" }));
    }
}

void
NodeSessionI::init()
{
    if(_node->ice_getEndpoints().empty() && _node->ice_getAdapterId().empty())
    {
        auto bidirNode = _node->ice_fixed(_connection);
        auto fwd = make_shared<NodeForwarderI>(_instance->getNodeSessionManager(), shared_from_this(), bidirNode);
        _publicNode = Ice::uncheckedCast<NodePrx>(_instance->getObjectAdapter()->add(fwd, _node->ice_getIdentity()));
    }
    else
    {
        _publicNode = _node;
    }

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "created node session (peer = `" << _publicNode << "'):\n" << _connection->toString();
    }
}

void
NodeSessionI::destroy()
{
    lock_guard<mutex> lock(_mutex);
    _destroyed = true;

    try
    {
        if(_publicNode != _node)
        {
            _instance->getObjectAdapter()->remove(_publicNode->ice_getIdentity());
        }

        for(const auto& session : _sessions)
        {
            session.second->disconnectedAsync();
        }
    }
    catch(const Ice::ObjectAdapterDeactivatedException&)
    {
    }
    catch(const Ice::CommunicatorDestroyedException&)
    {
    }

    if(_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << "destroyed node session (peer = `" << _publicNode << "')";
    }
}

void
NodeSessionI::addSession(shared_ptr<SessionPrx> session)
{
    lock_guard<mutex> lock(_mutex);
    _sessions[session->ice_getIdentity()] = move(session);
}

shared_ptr<SessionPrx>
NodeSessionI::forwarder(const std::shared_ptr<SessionPrx>& session) const
{
    auto id = session->ice_getIdentity();
    auto proxy = _instance->getObjectAdapter()->createProxy({ id.name + '-' + _node->ice_getIdentity().name,
                                                              id.category + 'f' });
    return Ice::uncheckedCast<SessionPrx>(proxy->ice_oneway());
}
