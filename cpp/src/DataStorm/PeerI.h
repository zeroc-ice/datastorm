// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <Ice/Ice.h>
#include <DataStorm/DataStorm.h>

#include <Internal.h>

#include <set>

namespace DataStormInternal
{

class TraceLevels;
class TopicI;
class TopicWriterI;
class TopicReaderI;
class Instance;
class SessionI;
class PublisherI;
class SubscriberI;

class TopicPeer : public std::enable_shared_from_this<TopicPeer>
{
    class DataElementForwarderI : public Ice::Blobject
    {
    public:

        DataElementForwarderI(std::shared_ptr<TopicPeer>, const std::shared_ptr<Key>&);

        virtual bool ice_invoke(Ice::ByteSeq, Ice::ByteSeq&, const Ice::Current&);

    private:

        std::shared_ptr<TopicPeer> _topic;
        std::shared_ptr<Key> _key;
    };

    class FilteredDataElementForwarderI : public Ice::Blobject
    {
    public:

        FilteredDataElementForwarderI(std::shared_ptr<TopicPeer>, const std::string&);

        virtual bool ice_invoke(Ice::ByteSeq, Ice::ByteSeq&, const Ice::Current&);

    private:

        std::shared_ptr<TopicPeer> _topic;
        std::string _filter;
    };

    class ForwarderI : public Ice::Blobject
    {
    public:

        ForwarderI(std::shared_ptr<TopicPeer>);
        virtual bool ice_invoke(Ice::ByteSeq, Ice::ByteSeq&, const Ice::Current&);

    private:

        std::shared_ptr<TopicPeer> _topic;
    };

public:

    TopicPeer(TopicI*);
    virtual ~TopicPeer() {}

    void init();

    void waitForKeyListeners(const std::shared_ptr<Key>&, int) const;
    bool hasKeyListeners(const std::shared_ptr<Key>&) const;
    void waitForFilteredListeners(const std::string&, int) const;
    bool hasFilteredListeners(const std::string&) const;

    void addSession(SessionI*);
    void removeSession(SessionI*);

    bool addKeyListener(const std::shared_ptr<Key>&, SessionI*);
    void removeKeyListener(const std::shared_ptr<Key>&, SessionI*);
    void addFilteredListener(const std::string&, SessionI*);
    void removeFilteredListener(const std::string&, SessionI*);

    void forEachPeerKey(const std::function<void(const std::shared_ptr<Key>&)>&, const std::string& = "") const;

    void subscribe(const std::string&);
    void unsubscribe(const std::string&);
    void subscribe(const std::shared_ptr<Key>&);
    void unsubscribe(const std::shared_ptr<Key>&);

    std::shared_ptr<DataStormContract::SessionPrx> addForwarder(const std::shared_ptr<Key>&);
    void removeForwarder(const std::shared_ptr<Key>&);
    std::shared_ptr<DataStormContract::SessionPrx> addForwarder(const std::string&);
    void removeForwarder(const std::string&);

    void destroy();

    std::shared_ptr<DataStormContract::SessionPrx> getForwarder() const
    {
        return _forwarder;
    }

    TopicI* getTopic() const
    {
        return _topic;
    }

    class Filter
    {
    public:

        Filter(TopicPeer* parent, const std::string& filter) : _parent(parent), _filter(filter)
        {
            for(const auto& forwarder : parent->_forwarders)
            {
                if(forwarder.first->match(filter))
                {
                    _keys.insert(forwarder.first);
                }
            }
        }

        void
        addKey(const std::shared_ptr<Key>& key)
        {
            if(key->match(_filter))
            {
                _keys.insert(key);

                if(!_sessions.empty())
                {
                    auto p = _parent->_listeners.find(key);
                    if(p == _parent->_listeners.end())
                    {
                        p = _parent->_listeners.emplace(key, Listener { _parent, key }).first;
                    }
                    for(auto s : _sessions)
                    {
                        p->second.addSession(s.first);
                    }
                }
            }
        }

        void
        removeKey(const std::shared_ptr<Key>& key)
        {
            if(key->match(_filter))
            {
                if(!_sessions.empty())
                {
                    auto p = _parent->_listeners.find(key);
                    for(auto s : _sessions)
                    {
                        if(p->second.removeSession(s.first))
                        {
                            _parent->_listeners.erase(p);
                        }
                    }
                }
                _keys.erase(key);
            }
        }

        void
        addSession(SessionI* session)
        {
            for(const auto& key : _keys)
            {
                auto p = _parent->_listeners.find(key);
                if(p == _parent->_listeners.end())
                {
                    p = _parent->_listeners.emplace(key, Listener { _parent, key }).first;
                }
                p->second.addSession(session);
            }
            assert(_sessions.find(session) == _sessions.end());
            _sessions[session] += 1;
        }

        bool
        removeSession(SessionI* session)
        {
            for(const auto& key : _keys)
            {
                auto p = _parent->_listeners.find(key);
                if(p->second.removeSession(session))
                {
                    _parent->_listeners.erase(p);
                }
            }
            auto p = _sessions.find(session);
            p->second -= 1;
            if(p->second == 0)
            {
                _sessions.erase(p);
                return _sessions.empty();
            }
            return false;
        }

        int
        getSessionCount() const
        {
            return static_cast<int>(_sessions.size());
        }

        const std::string&
        getFilter() const
        {
            return _filter;
        }

        void forward(const Ice::ByteSeq&, const Ice::Current&);

    private:

        friend class TopicPeer;

        TopicPeer* _parent;
        const std::string _filter;
        std::set<std::shared_ptr<Key>> _keys;
        std::map<SessionI*, int> _sessions;
    };

    class Listener
    {
    public:

        Listener(TopicPeer* parent, const std::shared_ptr<Key>& key) : _parent(parent), _key(key)
        {
            for(const auto& forwarder : parent->_filterForwarders)
            {
                if(key->match(forwarder.first))
                {
                    _filters.insert(forwarder.first);
                }
            }
        }

        void
        addFilter(const std::string& filter)
        {
            if(_key->match(filter))
            {
                _filters.insert(filter);

                if(!_sessions.empty())
                {
                    auto p = _parent->_filters.find(filter);
                    if(p == _parent->_filters.end())
                    {
                        p = _parent->_filters.emplace(filter, Filter { _parent, filter }).first;
                    }
                    for(auto s : _sessions)
                    {
                        p->second.addSession(s.first);
                    }
                }
            }
        }

        void
        removeFilter(const std::string& filter)
        {
            if(_key->match(filter))
            {
                if(!_sessions.empty())
                {
                    auto p = _parent->_filters.find(filter);
                    for(auto s : _sessions)
                    {
                        if(p->second.removeSession(s.first))
                        {
                            _parent->_filters.erase(p);
                        }
                    }
                }
                _filters.erase(filter);
            }
        }

        void
        addSession(SessionI* session)
        {
            for(const auto& filter : _filters)
            {
                auto p = _parent->_filters.find(filter);
                if(p == _parent->_filters.end())
                {
                    p = _parent->_filters.emplace(filter, Filter { _parent, filter }).first;
                }
                p->second.addSession(session);
            }
            assert(_sessions.find(session) == _sessions.end());
            _sessions[session] += 1;
        }

        bool
        removeSession(SessionI* session)
        {
            for(const auto& filter : _filters)
            {
                auto p = _parent->_filters.find(filter);
                if(p->second.removeSession(session))
                {
                    _parent->_filters.erase(p);
                }
            }
            auto p = _sessions.find(session);
            p->second -= 1;
            if(p->second == 0)
            {
                _sessions.erase(p);
                return _sessions.empty();
            }
            return false;
        }

        const std::shared_ptr<Key>&
        getKey() const
        {
            return _key;
        }

        int
        getSessionCount() const
        {
            return static_cast<int>(_sessions.size());
        }

        void forward(const Ice::ByteSeq& encaps, const Ice::Current& current);

    private:

        friend class TopicPeer;

        TopicPeer* _parent;
        const std::shared_ptr<Key> _key;
        std::set<std::string> _filters;
        std::map<SessionI*, int> _sessions;
    };

protected:

    friend class ForwarderI;
    friend class DataElementForwarderI;
    friend class FilteredDataElementForwarderI;

    void forward(const Ice::ByteSeq&, const Ice::Current&);
    void forward(const std::shared_ptr<Key>&, const Ice::ByteSeq&, const Ice::Current&);
    void forward(const std::string&, const Ice::ByteSeq&, const Ice::Current&);

    mutable std::mutex _mutex;
    mutable std::condition_variable _cond;
    TopicI* _topic;
    std::shared_ptr<TraceLevels> _traceLevels;
    std::shared_ptr<DataStormContract::SessionPrx> _forwarder;

    std::set<SessionI*> _sessions;
    std::map<std::shared_ptr<Key>, Listener> _listeners;
    std::map<std::string, Filter> _filters;
    std::map<std::shared_ptr<Key>, std::shared_ptr<DataStormContract::SessionPrx>> _forwarders;
    std::map<std::string, std::shared_ptr<DataStormContract::SessionPrx>> _filterForwarders;
};

class TopicPublisher : public TopicPeer
{
public:

    TopicPublisher(PublisherI*, TopicWriterI*);

    TopicWriterI* getWriter() const
    {
        return _writer;
    }

private:

    TopicWriterI* _writer;
};

class TopicSubscriber : public TopicPeer
{
public:

    TopicSubscriber(SubscriberI*, TopicReaderI*);

    TopicReaderI* getReader() const
    {
        return _reader;
    }

private:

    TopicReaderI* _reader;
};

class PeerI : virtual public DataStormContract::Peer, public std::enable_shared_from_this<PeerI>
{
    class ForwarderI : public Ice::Blobject
    {
    public:

        ForwarderI(std::shared_ptr<PeerI>);
        virtual bool ice_invoke(Ice::ByteSeq, Ice::ByteSeq&, const Ice::Current&);

    private:

        std::shared_ptr<PeerI> _peer;
    };

public:

    PeerI(std::shared_ptr<Instance>);
    void init();

    virtual bool createSession(std::shared_ptr<DataStormContract::PeerPrx>) = 0;
    void removeSession(SessionI*);

    std::shared_ptr<Instance> getInstance() const
    {
        return _instance;
    }

    std::shared_ptr<DataStormContract::SessionPrx> getForwarder() const
    {
        return _forwarder;
    }

    void checkSessions(std::shared_ptr<TopicI>);

protected:

    std::shared_ptr<SessionI> createSessionServant(std::shared_ptr<DataStormContract::PeerPrx>);
    virtual std::shared_ptr<SessionI> makeSessionServant(std::shared_ptr<DataStormContract::PeerPrx>) = 0;

    friend class ForwarderI;
    void forward(const Ice::ByteSeq&, const Ice::Current&);

    mutable std::mutex _mutex;
    mutable std::condition_variable _cond;
    std::shared_ptr<Instance> _instance;
    std::shared_ptr<DataStormContract::PeerPrx> _proxy;
    std::shared_ptr<DataStormContract::SessionPrx> _forwarder;
    std::map<Ice::Identity, std::shared_ptr<SessionI>> _sessions;
};

class PublisherI : public PeerI, public DataStormContract::Publisher
{
public:

    PublisherI(std::shared_ptr<Instance>);

    virtual bool createSession(std::shared_ptr<DataStormContract::PeerPrx>);
    virtual void createSessionAsync(std::shared_ptr<DataStormContract::SubscriberPrx>,
                                    std::shared_ptr<DataStormContract::SubscriberSessionPrx>,
                                    std::function<void (const std::shared_ptr<DataStormContract::PublisherSessionPrx>&)>,
                                    std::function<void (std::exception_ptr)>,
                                    const Ice::Current&);

    void sessionConnected(std::shared_ptr<SessionI>,
                          std::shared_ptr<DataStormContract::SubscriberSessionPrx>,
                          std::shared_ptr<DataStormContract::PeerPrx>);

    std::shared_ptr<TopicPublisher> createTopicPublisher(TopicWriterI*);

    std::shared_ptr<DataStormContract::PublisherPrx> getProxy() const
    {
        return Ice::uncheckedCast<DataStormContract::PublisherPrx>(_proxy);
    }

private:

    virtual std::shared_ptr<SessionI> makeSessionServant(std::shared_ptr<DataStormContract::PeerPrx>);
};

class SubscriberI : public PeerI, public DataStormContract::Subscriber
{
public:

    SubscriberI(std::shared_ptr<Instance>);

    virtual bool createSession(std::shared_ptr<DataStormContract::PeerPrx>);
    virtual void createSessionAsync(std::shared_ptr<DataStormContract::PublisherPrx>,
                                    std::shared_ptr<DataStormContract::PublisherSessionPrx>,
                                    std::function<void (const std::shared_ptr<DataStormContract::SubscriberSessionPrx>&)>,
                                    std::function<void (std::exception_ptr)>,
                                    const Ice::Current&);

    void sessionConnected(std::shared_ptr<SessionI>,
                          std::shared_ptr<DataStormContract::PublisherSessionPrx>,
                          std::shared_ptr<DataStormContract::PeerPrx>);

    std::shared_ptr<TopicSubscriber> createTopicSubscriber(TopicReaderI*);

    std::shared_ptr<DataStormContract::SubscriberPrx> getProxy() const
    {
        return Ice::uncheckedCast<DataStormContract::SubscriberPrx>(_proxy);
    }

private:

    virtual std::shared_ptr<SessionI> makeSessionServant(std::shared_ptr<DataStormContract::PeerPrx>);
};

}