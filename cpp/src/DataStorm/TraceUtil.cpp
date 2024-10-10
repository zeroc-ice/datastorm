//
// Copyright (c) ZeroC, Inc. All rights reserved.
//

#include "TraceUtil.h"
#include "Ice/Ice.h"

using namespace std;
using namespace DataStormI;

TraceLevels::TraceLevels(shared_ptr<Ice::Communicator> communicator)
    : topic(0),
      topicCat("Topic"),
      data(0),
      dataCat("Data"),
      session(0),
      sessionCat("Session"),
      logger(communicator->getLogger())
{
    auto properties = communicator->getProperties();
    const string keyBase = "DataStorm.Trace.";
    const_cast<int&>(topic) = properties->getPropertyAsInt(keyBase + topicCat);
    const_cast<int&>(data) = properties->getPropertyAsInt(keyBase + dataCat);
    const_cast<int&>(session) = properties->getPropertyAsInt(keyBase + sessionCat);
}
