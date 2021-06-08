# **********************************************************************
#
# Copyright (c) ZeroC, Inc. All rights reserved.
#
# **********************************************************************

$(test)_cppflags      := -DICE_CPP11_MAPPING
$(test)_sources       := Writer.cpp DuplicateSymbols.cpp Test.ice
tests += $(test)
