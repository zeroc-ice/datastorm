// **********************************************************************
//
// Copyright (c) 2018 ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#define DATASTORM_VERSION 0,1,0,0
#define DATASTORM_STRING_VERSION "0.1.0"
#define DATASTORM_SO_VERSION "0"

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
// Automatically link with DataStorm01[d].lib
//

#if !defined(DATASTORM_BUILDING_DATASTORM) && defined(DATASTORM_API_EXPORTS)
#   define DATASTORM_BUILDING_DATASTORM
#endif

#if defined(_MSC_VER) && !defined(DATASTORM_BUILDING_DATASTORM)
#   pragma comment(lib, DATASTORM_LIBNAME("DataStorm"))
#endif

//
// Always enable the Ice C++11 mapping when using DataStorm.
//
#if !defined(ICE_CPP11_MAPPING)
#   define ICE_CPP11_MAPPING 1
#endif
