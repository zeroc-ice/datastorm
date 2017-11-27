// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

module Test
{

struct StructValue
{
    string firstName;
    string lastName;
    int age;
};

class Base
{
    string b;
};

class Extended extends Base
{
    int e;
};

};
