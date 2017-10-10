# **********************************************************************
#
# Copyright (c) 2003-2017 ZeroC, Inc. All rights reserved.
#
# This copy of Ice is licensed to you under the terms described in the
# ICE_LICENSE file included in this distribution.
#
# **********************************************************************

$(project)_libraries    = DataStorm

$(project)_generated_includedir := $(project)/generated/DataStorm

DataStorm_sliceflags    := --include-dir DataStorm -I$(slicedir)
DataStorm_targetdir     := $(libdir)
DataStorm_cppflags      := -DDATASTORM_API_EXPORTS -Iinclude -Iinclude/generated -I$(project)/generated -Isrc $(ice_cppflags)
DataStorm_dependencies	:= Ice++11

projects += $(project)
