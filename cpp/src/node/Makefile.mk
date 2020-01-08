#
# Copyright (c) ZeroC, Inc. All rights reserved.
#

$(project)_programs = dsnode

$(project)_dependencies := DataStorm Ice++11
$(project)_targetdir    := $(bindir)

projects += $(project)
