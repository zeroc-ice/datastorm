# **********************************************************************
#
# Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
#
# This copy of Ice is licensed to you under the terms described in the
# ICE_LICENSE file included in this distribution.
#
# **********************************************************************

traceProps = {
    "DataStorm.Trace.Topic" : 0,
    "DataStorm.Trace.Session" : 0,
    "DataStorm.Trace.Data" : 1
}

TestSuite(__file__, [ ClientServerTestCase(traceProps=traceProps) ])
