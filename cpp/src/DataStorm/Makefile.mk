# **********************************************************************
#
# Copyright (c) 2018 ZeroC, Inc. All rights reserved.
#
# **********************************************************************

$(project)_libraries    = DataStorm

$(project)_generated_includedir := $(project)/generated/DataStorm

DataStorm_sliceflags    := --include-dir DataStorm -I$(slicedir)
DataStorm_targetdir     := $(libdir)
DataStorm_cppflags      := -DDATASTORM_API_EXPORTS -Iinclude -Iinclude/generated -I$(project)/generated -Isrc $(ice_cppflags)
DataStorm_dependencies  := Ice++11

projects += $(project)
