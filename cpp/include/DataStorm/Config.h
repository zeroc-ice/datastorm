// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// DATASTORM_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#if defined(_WIN32)
#   define DATASTORM_DECLSPEC_EXPORT __declspec(dllexport)
#   define DATASTORM_DECLSPEC_IMPORT __declspec(dllimport)
//
//  DATASTORM_HAS_DECLSPEC_IMPORT_EXPORT defined only for compilers with distinct
//  declspec for IMPORT and EXPORT
#   define DATASTORM_HAS_DECLSPEC_IMPORT_EXPORT
#elif defined(__GNUC__)
#   define DATASTORM_DECLSPEC_EXPORT __attribute__((visibility ("default")))
#   define DATASTORM_DECLSPEC_IMPORT __attribute__((visibility ("default")))
#elif defined(__SUNPRO_CC)
#   define DATASTORM_DECLSPEC_EXPORT __global
#   define DATASTORM_DECLSPEC_IMPORT /**/
#else
#   define DATASTORM_DECLSPEC_EXPORT /**/
#   define DATASTORM_DECLSPEC_IMPORT /**/
#endif
