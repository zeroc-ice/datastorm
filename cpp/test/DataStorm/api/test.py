# **********************************************************************
#
# Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
#
# **********************************************************************

traceProps = {
    "DataStorm.Trace.Topic": 1,
    "DataStorm.Trace.Session": 0,
    "DataStorm.Trace.Data": 2
}

TestSuite(__file__, [ ClientTestCase(traceProps=traceProps) ])
