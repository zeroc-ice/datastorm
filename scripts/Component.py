# **********************************************************************
#
# Copyright (c) 2003-2018 ZeroC, Inc. All rights reserved.
#
# **********************************************************************

import os

from Util import *

class Writer(Client):
    pass

class Reader(Server):
    pass

class DataStormCppMapping(CppMapping):

    def getEnv(self, process, current):
        env = CppMapping.getEnv(self, process, current)
        if isinstance(platform, Windows):
            env["PATH"] += os.pathsep + ice.getLibDir(process, process.getMapping(current), current)
        return env

class Ice(Component):

    def getInstallDir(self, mapping, current):
        return Component._getInstallDir(self, mapping, current, "ICE_HOME")

    def getNugetPackageVersionFile(self, mapping):
        return os.path.join(mapping.getPath(), "test", "DataStorm", "api", "msbuild", "writer", "packages.config")

class DataStorm(Component):

    def useBinDist(self, mapping, current):
        return Component._useBinDist(self, mapping, current, "DATASTORM_BIN_DIST")

    def getInstallDir(self, mapping, current):
        # No binary distribution on Windows, nuget package only.
        envHomeName = None if isinstance(platform, Windows) else "DATASTORM_HOME"
        return Component._getInstallDir(self, mapping, current, envHomeName)

    def getNugetPackageVersionFile(self, mapping):
        return os.path.join(mapping.getPath(), "test", "DataStorm", "api", "msbuild", "writer", "packages.config")

    def getDefaultSource(self, mapping, processType):
        return { "client" : "Writer.cpp", "server" : "Reader.cpp" }[processType]

component = DataStorm()
ice = Ice()

#
# Supported mappings
#
Mapping.add("cpp", DataStormCppMapping(), component)
