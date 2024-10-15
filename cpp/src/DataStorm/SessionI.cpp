//
// Copyright (c) ZeroC, Inc. All rights reserved.
//

#include "SessionI.h"
#include "CallbackExecutor.h"
#include "ConnectionManager.h"
#include "Instance.h"
#include "NodeI.h"
#include "TimerTaskI.h"
#include "TopicFactoryI.h"
#include "TopicI.h"
#include "TraceUtil.h"

using namespace std;
using namespace DataStormI;
using namespace DataStormContract;

namespace
{

    class DispatchInterceptorI : public Ice::Object
    {
    public:
        DispatchInterceptorI(Ice::ObjectPtr servant, shared_ptr<CallbackExecutor> executor)
            : _servant(std::move(servant)),
              _executor(std::move(executor))
        {
        }

        void dispatch(Ice::IncomingRequest& request, std::function<void(Ice::OutgoingResponse)> sendResponse) final
        {
            _servant->dispatch(request, sendResponse);
            _executor->flush();
        }

    private:
        Ice::ObjectPtr _servant;
        shared_ptr<CallbackExecutor> _executor;
    };

}

SessionI::SessionI(const std::shared_ptr<NodeI>& parent, optional<NodePrx> node)
    : _instance(parent->getInstance()),
      _traceLevels(_instance->getTraceLevels()),
      _parent(parent),
      _node(node),
      _destroyed(false),
      _sessionInstanceId(0),
      _retryCount(0)
{
}

void
SessionI::init(optional<SessionPrx> prx)
{
    assert(_node);
    _proxy = prx;
    _id = Ice::identityToString(prx->ice_getIdentity());

    //
    // Even though the node register a default servant for sessions, we still need to
    // register the session servant explicitly here to ensure collocation works. The
    // default servant from the node is used for facet calls.
    //
    _instance->getObjectAdapter()->add(
        make_shared<DispatchInterceptorI>(shared_from_this(), _instance->getCallbackExecutor()),
        _proxy->ice_getIdentity());

    if (_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": created session (peer = `" << _node << "')";
    }
}

void
SessionI::announceTopics(TopicInfoSeq topics, bool, const Ice::Current&)
{
    //
    // Retain topics outside the synchronization. This is necessary to ensure the topic destructor
    // doesn't get called within the synchronization. The topic destructor can callback on the
    // session to disconnect.
    //
    vector<shared_ptr<TopicI>> retained;
    {
        lock_guard<mutex> lock(_mutex);
        if (!_session)
        {
            return;
        }

        if (_traceLevels->session > 2)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": announcing topics `" << topics << "'";
        }

        for (const auto& info : topics)
        {
            runWithTopics(
                info.name,
                retained,
                [&](const shared_ptr<TopicI>& topic)
                {
                    for (auto id : info.ids)
                    {
                        topic->attach(id, shared_from_this(), _session);
                    }
                    // TODO check the return value?
                    auto _ = _session->attachTopicAsync(topic->getTopicSpec());
                });
        }

        // Reap un-visited topics
        auto p = _topics.begin();
        while (p != _topics.end())
        {
            if (p->second.reap(_sessionInstanceId))
            {
                _topics.erase(p++);
            }
            else
            {
                ++p;
            }
        }
    }
}

void
SessionI::attachTopic(TopicSpec spec, const Ice::Current&)
{
    //
    // Retain topics outside the synchronization. This is necessary to ensure the topic destructor
    // doesn't get called within the synchronization. The topic destructor can callback on the
    // session to disconnect.
    //
    vector<shared_ptr<TopicI>> retained;
    {
        lock_guard<mutex> lock(_mutex);
        if (!_session)
        {
            return;
        }

        runWithTopics(
            spec.name,
            retained,
            [&](const shared_ptr<TopicI>& topic)
            {
                if (_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": attaching topic `" << spec << "' to `" << topic << "'";
                }

                topic->attach(spec.id, shared_from_this(), _session);

                if (!spec.tags.empty())
                {
                    auto& subscriber = _topics.at(spec.id).getSubscriber(topic.get());
                    for (const auto& tag : spec.tags)
                    {
                        subscriber.tags[tag.id] =
                            topic->getTagFactory()->decode(_instance->getCommunicator(), tag.value);
                    }
                }

                auto tags = topic->getTags();
                if (!tags.empty())
                {
                    // TODO check the return value?
                    auto _ = _session->attachTagsAsync(topic->getId(), tags, true);
                }

                auto specs = topic->getElementSpecs(spec.id, spec.elements, shared_from_this());
                if (!specs.empty())
                {
                    if (_traceLevels->session > 2)
                    {
                        Trace out(_traceLevels, _traceLevels->sessionCat);
                        out << _id << ": matched elements `" << spec << "' on `" << topic << "'";
                    }
                    // TODO check the return value?
                    auto _ = _session->attachElementsAsync(topic->getId(), specs, true);
                }
            });
    }
}

void
SessionI::detachTopic(int64_t id, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session)
    {
        return;
    }

    runWithTopics(
        id,
        [&](TopicI* topic, TopicSubscriber&)
        {
            if (_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": detaching topic `" << id << "' from `" << topic << "'";
            }
            topic->detach(id, shared_from_this());
        });

    // Erase the topic
    _topics.erase(id);
}

void
SessionI::attachTags(int64_t topicId, ElementInfoSeq tags, bool initialize, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session)
    {
        return;
    }

    runWithTopics(
        topicId,
        [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers&)
        {
            if (_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": attaching tags `[" << tags << "]@" << topicId << "' on topic `" << topic << "'";
            }

            if (initialize)
            {
                subscriber.tags.clear();
            }
            for (const auto& tag : tags)
            {
                subscriber.tags[tag.id] = topic->getTagFactory()->decode(_instance->getCommunicator(), tag.value);
            }
        });
}

void
SessionI::detachTags(int64_t topicId, LongSeq tags, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session)
    {
        return;
    }

    runWithTopics(
        topicId,
        [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers&)
        {
            if (_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": detaching tags `[" << tags << "]@" << topicId << "' on topic `" << topic << "'";
            }

            for (auto tag : tags)
            {
                subscriber.tags.erase(tag);
            }
        });
}

void
SessionI::announceElements(int64_t topicId, ElementInfoSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session)
    {
        return;
    }

    runWithTopics(
        topicId,
        [&](TopicI* topic, TopicSubscriber&, TopicSubscribers&)
        {
            if (_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": announcing elements `[" << elements << "]@" << topicId << "' on topic `" << topic
                    << "'";
            }

            auto specs = topic->getElementSpecs(topicId, elements, shared_from_this());
            if (!specs.empty())
            {
                if (_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": announcing elements matched `[" << specs << "]@" << topicId << "' on topic `"
                        << topic << "'";
                }
                // TODO check the return value?
                auto _ = _session->attachElementsAsync(topic->getId(), specs, false);
            }
        });
}

void
SessionI::attachElements(int64_t id, ElementSpecSeq elements, bool initialize, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session)
    {
        return;
    }

    auto now = chrono::system_clock::now();
    runWithTopics(
        id,
        [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers&)
        {
            if (_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": attaching elements `[" << elements << "]@" << id << "' on topic `" << topic << "'";
                if (initialize)
                {
                    out << " (initializing)";
                }
            }

            auto specAck = topic->attachElements(id, elements, shared_from_this(), _session, now);

            if (initialize)
            {
                // Reap unused keys and filters from the topic subscriber
                subscriber.reap(_sessionInstanceId);

                // TODO: reap keys / filters
            }

            if (!specAck.empty())
            {
                if (_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": attaching elements matched `[" << specAck << "]@" << id << "' on topic `" << topic
                        << "'";
                }
                // TODO check the return value?
                auto _ = _session->attachElementsAckAsync(topic->getId(), specAck);
            }
        });
}

void
SessionI::attachElementsAck(int64_t id, ElementSpecAckSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session)
    {
        return;
    }
    auto now = chrono::system_clock::now();
    runWithTopics(
        id,
        [&](TopicI* topic, TopicSubscriber&, TopicSubscribers&)
        {
            if (_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": attaching elements ack `[" << elements << "]@" << id << "' on topic `" << topic << "'";
            }

            LongSeq removedIds;
            auto samples = topic->attachElementsAck(id, elements, shared_from_this(), _session, now, removedIds);
            if (!samples.empty())
            {
                if (_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": initializing elements `[" << samples << "]@" << id << "' on topic `" << topic
                        << "'";
                }
                // TODO check the return value?
                auto _ = _session->initSamplesAsync(topic->getId(), samples);
            }
            if (!removedIds.empty())
            {
                // TODO check the return value?
                auto _ = _session->detachElementsAsync(topic->getId(), removedIds);
            }
        });
}

void
SessionI::detachElements(int64_t id, LongSeq elements, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session)
    {
        return;
    }

    runWithTopics(
        id,
        [&](TopicI* topic, TopicSubscriber& subscriber)
        {
            if (_traceLevels->session > 2)
            {
                Trace out(_traceLevels, _traceLevels->sessionCat);
                out << _id << ": detaching elements `[" << elements << "]@" << id << "' on topic `" << topic << "'";
            }

            for (auto e : elements)
            {
                auto k = subscriber.remove(e);
                for (auto& s : k.getSubscribers())
                {
                    for (auto key : s.second.keys)
                    {
                        if (e > 0)
                        {
                            s.first->detachKey(id, e, key, shared_from_this(), s.second.facet, true);
                        }
                        else
                        {
                            s.first->detachFilter(id, -e, key, shared_from_this(), s.second.facet, true);
                        }
                    }
                }
            }
        });
}

void
SessionI::initSamples(int64_t topicId, DataSamplesSeq samplesSeq, const Ice::Current&)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session)
    {
        return;
    }

    auto now = chrono::system_clock::now();
    auto communicator = _instance->getCommunicator();
    for (const auto& samples : samplesSeq)
    {
        runWithTopics(
            topicId,
            [&](TopicI* topic, TopicSubscriber& subscriber)
            {
                auto k = subscriber.get(samples.id);
                if (k)
                {
                    if (_traceLevels->session > 2)
                    {
                        Trace out(_traceLevels, _traceLevels->sessionCat);
                        out << _id << ": initializing samples from `" << samples.id << "'"
                            << " on [";
                        for (auto q = k->getSubscribers().begin(); q != k->getSubscribers().end(); ++q)
                        {
                            if (q != k->getSubscribers().begin())
                            {
                                out << ", ";
                            }
                            out << q->first;
                            if (!q->second.facet.empty())
                            {
                                out << ":" << q->second.facet;
                            }
                        }
                        out << "]";
                    }

                    vector<shared_ptr<Sample>> samplesI;
                    samplesI.reserve(samples.samples.size());
                    auto sampleFactory = topic->getSampleFactory();
                    for (auto& s : samples.samples)
                    {
                        shared_ptr<Key> key;
                        if (s.keyValue.empty())
                        {
                            key = subscriber.keys[s.keyId].first;
                        }
                        else
                        {
                            key = topic->getKeyFactory()->decode(_instance->getCommunicator(), s.keyValue);
                        }
                        assert(key);

                        samplesI.push_back(sampleFactory->create(
                            _id,
                            k->name,
                            s.id,
                            s.event,
                            key,
                            subscriber.tags[s.tag],
                            s.value,
                            s.timestamp));
                    }
                    for (auto& ks : k->getSubscribers())
                    {
                        if (!ks.second.initialized)
                        {
                            ks.second.initialized = true;
                            if (!samplesI.empty())
                            {
                                ks.second.lastId = samplesI.back()->id;
                                ks.first->initSamples(samplesI, topicId, samples.id, k->priority, now, samples.id < 0);
                            }
                        }
                    }
                }
            });
    }
}

void
SessionI::disconnected(const Ice::Current& current)
{
    if (disconnected(current.con, nullptr))
    {
        if (!retry(getNode(), nullptr))
        {
            remove();
        }
    }
}

void
SessionI::connected(optional<SessionPrx> session, const Ice::ConnectionPtr& connection, const TopicInfoSeq& topics)
{
    lock_guard<mutex> lock(_mutex);
    if (_destroyed || _session)
    {
        assert(_connectedCallbacks.empty());
        return;
    }

    _session = session;
    _connection = connection;
    if (connection)
    {
        auto self = shared_from_this();
        _instance->getConnectionManager()->add(
            connection,
            self,
            [self](auto connection, auto ex)
            {
                if (self->disconnected(connection, ex))
                {
                    if (!self->retry(self->getNode(), nullptr))
                    {
                        self->remove();
                    }
                }
            });
    }

    if (_retryTask)
    {
        _instance->getTimer()->cancel(_retryTask);
        _retryTask = nullptr;
    }

    ++_sessionInstanceId;

    if (_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": session `" << _session->ice_getIdentity() << "' connected";
        if (_connection)
        {
            out << "\n" << _connection->toString();
        }
    }

    if (!topics.empty())
    {
        try
        {
            // TODO check the return value?
            auto _ = _session->announceTopicsAsync(topics, true);
        }
        catch (const Ice::LocalException&)
        {
            // Ignore
        }
    }

    for (auto c : _connectedCallbacks)
    {
        c(_proxy);
    }
    _connectedCallbacks.clear();
}

bool
SessionI::disconnected(const Ice::ConnectionPtr& connection, exception_ptr ex)
{
    lock_guard<mutex> lock(_mutex);
    if (_destroyed || (connection && _connection != connection) || !_session)
    {
        return false;
    }

    if (_traceLevels->session > 0)
    {
        try
        {
            if (ex)
            {
                rethrow_exception(ex);
            }
            else
            {
                throw Ice::CloseConnectionException(__FILE__, __LINE__);
            }
        }
        catch (const std::exception& e)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": session `" << _session->ice_getIdentity() << "' disconnected:\n";
            out << (_connection ? _connection->toString() : "<no connection>") << "\n";
            out << e.what();
        }
    }

    for (auto& t : _topics)
    {
        runWithTopics(t.first, [&](TopicI* topic, TopicSubscriber&) { topic->detach(t.first, shared_from_this()); });
    }

    _session = nullopt;
    _connection = nullptr;
    _retryCount = 0;
    return true;
}

bool
SessionI::retry(optional<NodePrx> node, exception_ptr exception)
{
    lock_guard<mutex> lock(_mutex);

    if (exception)
    {
        //
        // Don't retry if we are shutting down.
        //
        try
        {
            rethrow_exception(exception);
        }
        catch (const Ice::ObjectAdapterDeactivatedException&)
        {
            return false;
        }
        catch (const Ice::CommunicatorDestroyedException&)
        {
            return false;
        }
        catch (const std::exception&)
        {
        }
    }

    if (node->ice_getEndpoints().empty() && node->ice_getAdapterId().empty())
    {
        if (_retryTask)
        {
            _instance->getTimer()->cancel(_retryTask);
            _retryTask = nullptr;
        }
        _retryCount = 0;

        //
        // If we can't retry connecting to the node because we don't have its endpoints,
        // we just wait for the duration of the last retry delay for the peer to reconnect.
        // If it doesn't reconnect, we'll destroy this session after the timeout.
        //
        auto delay = _instance->getRetryDelay(_instance->getRetryCount()) * 2;

        if (_traceLevels->session > 0)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": can't retry connecting to `" << node->ice_toString() << "`, waiting " << delay.count()
                << " (ms) for peer to reconnect";
        }

        _retryTask = make_shared<TimerTaskI>([self = shared_from_this()] { self->remove(); });
        _instance->getTimer()->schedule(_retryTask, delay);
    }
    else
    {
        //
        // If we can retry the connection attempt, we schedule a timer to retry. Always
        // retry immediately on the first attempt.
        //
        auto delay = _retryCount == 0 ? 0ms : _instance->getRetryDelay(_retryCount - 1);
        ++_retryCount;

        if (_traceLevels->session > 0)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            if (_retryCount <= _instance->getRetryCount())
            {
                out << _id << ": retrying connecting to `" << node->ice_toString() << "` in " << delay.count();
                out << " (ms), retry " << _retryCount << '/' << _instance->getRetryCount();
            }
            else
            {
                out << _id << ": connection to `" << node->ice_toString()
                    << "` failed and the retry limit has been reached";
            }
            if (exception)
            {
                try
                {
                    rethrow_exception(exception);
                }
                catch (const Ice::LocalException& ex)
                {
                    out << '\n' << ex.what();
                }
            }
        }

        if (_retryCount > _instance->getRetryCount())
        {
            return false;
        }

        _retryTask = make_shared<TimerTaskI>([node, self = shared_from_this()] { self->reconnect(node); });
        _instance->getTimer()->schedule(_retryTask, delay);
    }
    return true;
}

void
SessionI::destroyImpl(const exception_ptr& ex)
{
    lock_guard<mutex> lock(_mutex);
    assert(!_destroyed);
    _destroyed = true;

    if (_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": destroyed session";
        if (ex)
        {
            try
            {
                rethrow_exception(ex);
            }
            catch (const Ice::Exception& e)
            {
                out << "\n:" << e.what() << "\n" << e.ice_stackTrace();
            }
            catch (const exception& e)
            {
                out << "\n:" << e.what();
            }
            catch (...)
            {
                out << "\n: unexpected exception";
            }
        }
    }

    if (_session)
    {
        if (_connection)
        {
            _instance->getConnectionManager()->remove(shared_from_this(), _connection);
        }

        _session = nullopt;
        _connection = nullptr;

        for (const auto& t : _topics)
        {
            runWithTopics(
                t.first,
                [&](TopicI* topic, TopicSubscriber&) { topic->detach(t.first, shared_from_this()); });
        }
        _topics.clear();
    }

    for (auto c : _connectedCallbacks)
    {
        c(nullopt);
    }
    _connectedCallbacks.clear();

    try
    {
        _instance->getObjectAdapter()->remove(_proxy->ice_getIdentity());
    }
    catch (const Ice::ObjectAdapterDeactivatedException&)
    {
    }
}

Ice::ConnectionPtr
SessionI::getConnection() const
{
    lock_guard<mutex> lock(_mutex);
    return _connection;
}

bool
SessionI::checkSession()
{
    while (true)
    {
        unique_lock<mutex> lock(_mutex);
        if (_session)
        {
            if (_connection)
            {
                //
                // Make sure the connection is still established. It's possible that the connection got closed
                // and we're not notified yet by the connection manager. Check session explicitly check for the
                // connection to make sure that if we get a session creation request from a peer (which might
                // detect the connection closure before), it doesn't get ignored.
                //
                try
                {
                    _connection->throwException();
                }
                catch (const Ice::LocalException&)
                {
                    auto connection = _connection;
                    lock.unlock();
                    if (!disconnected(connection, current_exception()))
                    {
                        continue;
                    }
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }
}

optional<SessionPrx>
SessionI::getSession() const
{
    lock_guard<mutex> lock(_mutex);
    return _session;
}

optional<NodePrx>
SessionI::getNode() const
{
    lock_guard<mutex> lock(_mutex);
    return _node;
}

void
SessionI::setNode(optional<NodePrx> node)
{
    lock_guard<mutex> lock(_mutex);
    _node = node;
}

void
SessionI::subscribe(long long id, TopicI* topic)
{
    if (_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed topic `" << id << "' to topic `" << topic << "'";
    }
    _topics[id].addSubscriber(topic, _sessionInstanceId);
}

void
SessionI::unsubscribe(long long id, TopicI* topic)
{
    assert(_topics.find(id) != _topics.end());
    auto& subscriber = _topics.at(id).getSubscriber(topic);
    for (auto& k : subscriber.getAll())
    {
        for (auto& e : k.second.getSubscribers())
        {
            for (auto key : e.second.keys)
            {
                if (k.first > 0)
                {
                    e.first->detachKey(id, k.first, key, shared_from_this(), e.second.facet, false);
                }
                else
                {
                    e.first->detachFilter(id, -k.first, key, shared_from_this(), e.second.facet, false);
                }
            }
        }
    }
    if (_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": unsubscribed topic `" << id << "' from topic `" << topic << "'";
    }
}

void
SessionI::disconnect(long long id, TopicI* topic)
{
    lock_guard<mutex> lock(_mutex); // Called by TopicI::destroy
    if (!_session)
    {
        return;
    }

    if (_topics.find(id) == _topics.end())
    {
        return; // Peer topic detached first.
    }

    runWithTopic(id, topic, [&](TopicSubscriber&) { unsubscribe(id, topic); });

    auto& subscriber = _topics.at(id);
    subscriber.removeSubscriber(topic);
    if (subscriber.getSubscribers().empty())
    {
        _topics.erase(id);
    }
}

void
SessionI::subscribeToKey(
    long long topicId,
    int64_t elementId,
    const std::shared_ptr<DataElementI>& element,
    const string& facet,
    const shared_ptr<Key>& key,
    int64_t keyId,
    const string& name,
    int priority)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    if (_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed `e" << elementId << ":[k" << keyId << "]@" << topicId << "' to `" << element << "'";
        if (!facet.empty())
        {
            out << " (facet=" << facet << ')';
        }
    }

    subscriber.add(elementId, name, priority)->addSubscriber(element, key, facet, _sessionInstanceId);

    auto& p = subscriber.keys[keyId];
    if (!p.first)
    {
        p.first = key;
    }
    ++p.second[elementId];
}

void
SessionI::unsubscribeFromKey(
    long long topicId,
    int64_t elementId,
    const std::shared_ptr<DataElementI>& element,
    int64_t keyId)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    auto k = subscriber.get(elementId);
    if (k)
    {
        if (_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed `e" << elementId << "[k" << keyId << "]@" << topicId << "' from `" << element
                << "'";
        }
        k->removeSubscriber(element);
        if (k->getSubscribers().empty())
        {
            subscriber.remove(elementId);
        }
    }

    auto& p = subscriber.keys[keyId];
    if (--p.second[elementId] == 0)
    {
        p.second.erase(elementId);
        if (p.second.empty())
        {
            subscriber.keys.erase(keyId);
        }
    }
}

void
SessionI::disconnectFromKey(
    long long topicId,
    int64_t elementId,
    const std::shared_ptr<DataElementI>& element,
    int64_t keyId)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if (!_session)
    {
        return;
    }

    runWithTopic(
        topicId,
        element->getTopic(),
        [&](TopicSubscriber&) { unsubscribeFromKey(topicId, elementId, element, keyId); });
}

void
SessionI::subscribeToFilter(
    long long topicId,
    int64_t elementId,
    const std::shared_ptr<DataElementI>& element,
    const string& facet,
    const shared_ptr<Key>& key,
    const string& name,
    int priority)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    if (_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": subscribed `e" << elementId << '@' << topicId << "' to `" << element << "'";
        if (!facet.empty())
        {
            out << " (facet=" << facet << ')';
        }
    }
    subscriber.add(-elementId, name, priority)->addSubscriber(element, key, facet, _sessionInstanceId);
}

void
SessionI::unsubscribeFromFilter(
    long long topicId,
    int64_t elementId,
    const std::shared_ptr<DataElementI>& element,
    int64_t)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    auto f = subscriber.get(-elementId);
    if (f)
    {
        if (_traceLevels->session > 1)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": unsubscribed `e" << elementId << '@' << topicId << "' from `" << element << "'";
        }
        f->removeSubscriber(element);
        if (f->getSubscribers().empty())
        {
            subscriber.remove(-elementId);
        }
    }
}

void
SessionI::disconnectFromFilter(
    long long topicId,
    int64_t elementId,
    const std::shared_ptr<DataElementI>& element,
    int64_t filterId)
{
    lock_guard<mutex> lock(_mutex); // Called by DataElementI::destroy
    if (!_session)
    {
        return;
    }

    runWithTopic(
        topicId,
        element->getTopic(),
        [&](TopicSubscriber&) { unsubscribeFromFilter(topicId, elementId, element, filterId); });
}

LongLongDict
SessionI::getLastIds(long long topicId, int64_t keyId, const std::shared_ptr<DataElementI>& element)
{
    LongLongDict lastIds;
    auto p = _topics.find(topicId);
    if (p != _topics.end())
    {
        auto& subscriber = p->second.getSubscriber(element->getTopic());
        for (auto q : subscriber.keys[keyId].second)
        {
            lastIds.emplace(q.first, subscriber.get(q.first)->getSubscriber(element)->lastId);
        }
    }
    return lastIds;
}

vector<shared_ptr<Sample>>
SessionI::subscriberInitialized(
    int64_t topicId,
    int64_t elementId,
    const DataSampleSeq& samples,
    const shared_ptr<Key>& key,
    const std::shared_ptr<DataElementI>& element)
{
    assert(_topics.find(topicId) != _topics.end());
    auto& subscriber = _topics.at(topicId).getSubscriber(element->getTopic());
    auto e = subscriber.get(elementId);
    auto s = e->getSubscriber(element);
    assert(s);

    if (_traceLevels->session > 1)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": initialized `" << element << "' from `e" << elementId << '@' << topicId << "'";
    }
    s->initialized = true;
    s->lastId = samples.empty() ? 0 : samples.back().id;

    vector<shared_ptr<Sample>> samplesI;
    samplesI.reserve(samples.size());
    auto sampleFactory = element->getTopic()->getSampleFactory();
    auto keyFactory = element->getTopic()->getKeyFactory();
    for (auto& s : samples)
    {
        assert((!key && !s.keyValue.empty()) || key == subscriber.keys[s.keyId].first);

        samplesI.push_back(sampleFactory->create(
            _id,
            e->name,
            s.id,
            s.event,
            key ? key : keyFactory->decode(_instance->getCommunicator(), s.keyValue),
            subscriber.tags[s.tag],
            s.value,
            s.timestamp));
        assert(samplesI.back()->key);
    }
    return samplesI;
}

void
SessionI::runWithTopics(
    const std::string& name,
    vector<shared_ptr<TopicI>>& retained,
    function<void(const shared_ptr<TopicI>&)> fn)
{
    auto topics = getTopics(name);
    for (auto topic : topics)
    {
        retained.push_back(topic);
        unique_lock<mutex> l(topic->getMutex());
        if (topic->isDestroyed())
        {
            continue;
        }
        _topicLock = &l;
        fn(topic);
        _topicLock = nullptr;
        l.unlock();
    }
}

void
SessionI::runWithTopics(int64_t id, function<void(TopicI*, TopicSubscriber&)> fn)
{
    auto t = _topics.find(id);
    if (t != _topics.end())
    {
        for (auto& s : t->second.getSubscribers())
        {
            unique_lock<mutex> l(s.first->getMutex());
            if (s.first->isDestroyed())
            {
                continue;
            }
            _topicLock = &l;
            fn(s.first, s.second);
            _topicLock = nullptr;
            l.unlock();
        }
    }
}

void
SessionI::runWithTopics(int64_t id, function<void(TopicI*, TopicSubscriber&, TopicSubscribers&)> fn)
{
    auto t = _topics.find(id);
    if (t != _topics.end())
    {
        for (auto& s : t->second.getSubscribers())
        {
            if (s.first->isDestroyed())
            {
                continue;
            }
            unique_lock<mutex> l(s.first->getMutex());
            _topicLock = &l;
            fn(s.first, s.second, t->second);
            _topicLock = nullptr;
            l.unlock();
        }
    }
}

void
SessionI::runWithTopic(int64_t id, TopicI* topic, function<void(TopicSubscriber&)> fn)
{
    auto t = _topics.find(id);
    if (t != _topics.end())
    {
        auto p = t->second.getSubscribers().find(topic);
        if (p != t->second.getSubscribers().end())
        {
            unique_lock<mutex> l(topic->getMutex());
            if (topic->isDestroyed())
            {
                return;
            }
            _topicLock = &l;
            fn(p->second);
            _topicLock = nullptr;
            l.unlock();
        }
    }
}

SubscriberSessionI::SubscriberSessionI(const std::shared_ptr<NodeI>& parent, optional<NodePrx> node)
    : SessionI(parent, node)
{
}

vector<shared_ptr<TopicI>>
SubscriberSessionI::getTopics(const string& name) const
{
    return _instance->getTopicFactory()->getTopicReaders(name);
}

void
SubscriberSessionI::s(int64_t topicId, int64_t elementId, DataSample s, const Ice::Current& current)
{
    lock_guard<mutex> lock(_mutex);
    if (!_session || current.con != _connection)
    {
        if (current.con != _connection)
        {
            Trace out(_traceLevels, _traceLevels->sessionCat);
            out << _id << ": discarding sample `" << s.id << "' from `e" << elementId << '@' << topicId << "'\n";
            if (_connection)
            {
                out << current.con->toString() << "\n" << _connection->toString();
            }
            else
            {
                out << "<not connected>";
            }
        }
        return;
    }
    auto now = chrono::system_clock::now();
    runWithTopics(
        topicId,
        [&](TopicI* topic, TopicSubscriber& subscriber, TopicSubscribers&)
        {
            auto e = subscriber.get(elementId);
            if (e && !e->getSubscribers().empty())
            {
                if (_traceLevels->session > 2)
                {
                    Trace out(_traceLevels, _traceLevels->sessionCat);
                    out << _id << ": queuing sample `" << s.id << "[k" << s.keyId << "]' from `e" << elementId << '@'
                        << topicId << "'";
                    if (!current.facet.empty())
                    {
                        out << " facet=" << current.facet;
                    }
                    out << " to [";
                    for (auto q = e->getSubscribers().begin(); q != e->getSubscribers().end(); ++q)
                    {
                        if (q != e->getSubscribers().begin())
                        {
                            out << ", ";
                        }
                        out << q->first;
                        if (!q->second.facet.empty())
                        {
                            out << ":" << q->second.facet;
                        }
                    }
                    out << "]";
                }

                shared_ptr<Key> key;
                if (s.keyValue.empty())
                {
                    key = subscriber.keys[s.keyId].first;
                }
                else
                {
                    key = topic->getKeyFactory()->decode(_instance->getCommunicator(), s.keyValue);
                }
                assert(key);

                auto impl = topic->getSampleFactory()->create(
                    _id,
                    e->name,

                    s.id,
                    s.event,
                    key,
                    subscriber.tags[s.tag],
                    s.value,
                    s.timestamp);
                for (auto& es : e->getSubscribers())
                {
                    if (es.second.initialized && (s.keyId <= 0 || es.second.keys.find(key) != es.second.keys.end()))
                    {
                        es.second.lastId = s.id;
                        es.first->queue(impl, e->priority, shared_from_this(), current.facet, now, !s.keyValue.empty());
                    }
                }
            }
        });
}

void
SubscriberSessionI::reconnect(optional<NodePrx> node)
{
    if (_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": trying to reconnect session with `" << node->ice_toString() << "'";
    }
    _parent->createPublisherSession(node, nullptr, dynamic_pointer_cast<SubscriberSessionI>(shared_from_this()));
}

void
SubscriberSessionI::remove()
{
    _parent->removeSubscriberSession(getNode(), dynamic_pointer_cast<SubscriberSessionI>(shared_from_this()), nullptr);
}

PublisherSessionI::PublisherSessionI(const std::shared_ptr<NodeI>& parent, optional<NodePrx> node)
    : SessionI(parent, node)
{
}

vector<shared_ptr<TopicI>>
PublisherSessionI::getTopics(const string& name) const
{
    return _instance->getTopicFactory()->getTopicWriters(name);
}

void
PublisherSessionI::reconnect(optional<NodePrx> node)
{
    if (_traceLevels->session > 0)
    {
        Trace out(_traceLevels, _traceLevels->sessionCat);
        out << _id << ": trying to reconnect session with `" << node->ice_toString() << "'";
    }
    _parent->createSubscriberSession(node, nullptr, dynamic_pointer_cast<PublisherSessionI>(shared_from_this()));
}

void
PublisherSessionI::remove()
{
    _parent->removePublisherSession(getNode(), dynamic_pointer_cast<PublisherSessionI>(shared_from_this()), nullptr);
}
