// **********************************************************************
//
// Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// DATASTORM_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#define DATASTORM_VERSION 0,1,0,0
#define DATASTORM_STRING_VERSION "0.1.0"
#define DATASTORM_SO_VERSION "01"

#if defined(_DEBUG)
#    define DATASTORM_LIBNAME(NAME) NAME DATASTORM_SO_VERSION "d"
#  else
#    define DATASTORM_LIBNAME(NAME) NAME DATASTORM_SO_VERSION ""
#endif

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

//
// Automatically link with IceStorm[D|++11|++11D].lib
//

#if !defined(DATASTORM_BUILDING_DATASTORM_LIB) && defined(DATASTORM_API_EXPORTS)
#   define DATASTORM_BUILDING_DATASTORM_LIB
#endif

#if defined(_MSC_VER) && !defined(DATASTORM_BUILDING_DATASTORM_LIB)
#   pragma comment(lib, DATASTORM_LIBNAME("DataStorm"))
#endif
