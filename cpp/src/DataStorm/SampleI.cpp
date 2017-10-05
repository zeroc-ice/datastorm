// **********************************************************************
//
// Copyright (c) 2003-2015 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#include <DataStorm/SampleI.h>

#include <Internal.h>

using namespace std;
using namespace DataStormInternal;

SampleI::SampleI(const shared_ptr<DataStorm::TopicFactory>& factory,
                 const shared_ptr<Key>& key,
                 const DataStormContract::DataSamplePtr& sample)
{
    this->type = static_cast<DataStorm::SampleType>(sample->type);
    this->key = key;
    this->value = sample->value;
    this->timestamp = IceUtil::Time::milliSeconds(sample->timestamp);
    this->factory = factory;
}
