// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include <DataStorm/SampleType.ice>

module DataStorm
{

/**
 * A regular expression filter to match key and value against regular expressions
 * and to filter by sampe type.
 */
struct RegexFilter
{
    /** The regex to use to match against keys. */
    string key;

    /** The regex to use to match values. */
    string value;

    /** The accepted sample types */
    SampleTypeSeq types;
}

}