// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/DataStorm.h>

#include <Internal.h>

namespace DataStormInternal
{

class SampleI : public Sample
{
public:

    SampleI(const std::shared_ptr<DataStorm::TopicFactory>&,
            const std::shared_ptr<Key>&,
            const DataStormContract::DataSamplePtr&);
};

}
