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

    def useBinDist(self, mapping, current):
        return True

    def getInstallDir(self, mapping, current):
        return Component._getInstallDir(self, mapping, current, "ICE_HOME")

    def getNugetPackage(self, mapping, compiler):
        return "zeroc.ice.{0}".format(compiler)

    def getNugetPackageVersion(self, mapping):
        return "3.7.1"

class DataStorm(Component):

    def __init__(self):
        self.nugetVersion = None
        self.ice = Ice()

    def useBinDist(self, mapping, current):
        return Component._useBinDist(self, mapping, current, "DATASTORM_BIN_DIST")

    def getInstallDir(self, mapping, current):
        # No binary distribution on Windows, nuget package only.
        envHomeName = None if isinstance(platform, Windows) else "DATASTORM_HOME"
        return Component._getInstallDir(self, mapping, current, envHomeName)

    def getNugetPackage(self, mapping, compiler):
        return "zeroc.datastorm.{0}".format(compiler)

    def getNugetPackageVersion(self, mapping):
        if not self.nugetVersion:
            with open(os.path.join(toplevel, "cpp", "msbuild", "zeroc.datastorm.v140.nuspec"), "r") as configFile:
                self.nugetVersion = re.search("<version>(.*)</version>", configFile.read()).group(1)
        return self.nugetVersion

    def getDefaultSource(self, mapping, processType):
        return { "client" : "Writer.cpp", "server" : "Reader.cpp" }[processType]

    def getDefaultProcesses(self, mapping, processType, testId):
        return { "client": [ Writer() ], "server": [ Reader() ] }[processType] if processType else None

    def getDefaultExe(self, mapping, processType, config):
        return { "client" : "writer", "server" : "reader" }[processType]

component = DataStorm()
ice = Ice()

#
# Supported mappings
#
Mapping.add("cpp", DataStormCppMapping())
