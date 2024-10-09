//
// Copyright (c) ZeroC, Inc. All rights reserved.
//
#pragma once

#include <DataStorm/Contract.h>

namespace DataStormI
{

    class NodeSessionManager;
    class TopicFactoryI;

    class LookupI : public DataStormContract::Lookup
    {
    public:
        LookupI(
            std::shared_ptr<NodeSessionManager>,
            std::shared_ptr<TopicFactoryI>,
            std::shared_ptr<DataStormContract::NodePrx>);

        virtual void
        announceTopicReader(std::string, std::shared_ptr<DataStormContract::NodePrx>, const Ice::Current&) override;

        virtual void
        announceTopicWriter(std::string, std::shared_ptr<DataStormContract::NodePrx>, const Ice::Current&) override;

        virtual void announceTopics(
            DataStormContract::StringSeq,
            DataStormContract::StringSeq,
            std::shared_ptr<DataStormContract::NodePrx>,
            const Ice::Current&) override;

        virtual std::shared_ptr<DataStormContract::NodePrx>
        createSession(std::shared_ptr<DataStormContract::NodePrx>, const Ice::Current&) override;

    private:
        std::shared_ptr<NodeSessionManager> _nodeSessionManager;
        std::shared_ptr<TopicFactoryI> _topicFactory;
        std::shared_ptr<DataStormContract::NodePrx> _nodePrx;
    };

}
