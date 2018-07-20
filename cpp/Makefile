# **********************************************************************
#
# Copyright (c) 2018 ZeroC, Inc. All rights reserved.
#
# **********************************************************************

top_srcdir      := ..
lang_srcdir     := $(top_srcdir)/cpp

include $(top_srcdir)/config/Make.rules
include $(lang_srcdir)/config/Make.rules

ifeq ($(filter all cpp,$(DATASTORM_BIN_DIST)),)
projects :=
include $(shell find $(lang_srcdir)/src -name Makefile.mk)
$(call make-projects,$(projects),make-cpp-src-project,,$(filter all cpp,$(DATASTORM_BIN_DIST)))
endif

#
# Create and load test projects.
#
projects :=
tests := $(call tests-without-project-makefile,cpp)
include $(shell find $(lang_srcdir)/test -name Makefile.mk)
$(foreach t,$(tests),$(eval $(call create-cpp-test-project,$(t))))
$(call make-projects,$(projects),make-cpp-test-project)
