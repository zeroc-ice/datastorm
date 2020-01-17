# **********************************************************************
#
# Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
#
# **********************************************************************

import os

from Util import *

class Writer(Client):
    pass

class Reader(Server):
    pass

class Node(ProcessFromBinDir, Server):
    def __init__(self, desc=None, *args, **kargs):
        Server.__init__(self, "dsnode", mapping=Mapping.getByName("cpp"), desc=desc or "DataStorm node", *args, **kargs)

    def shutdown(self, current):
        if self in current.processes:
            current.processes[self].terminate()

    def getProps(self, current):
        props = Server.getProps(self, current)
        props['Ice.ProgramName'] = self.desc
        return props

class NodeTestCase(ClientServerTestCase):

    def __init__(self, nodes=None, nodeProps=None, *args, **kargs):
        TestCase.__init__(self, *args, **kargs)
        if nodes:
            self.nodes = nodes
        elif nodeProps:
           self.nodes = [Node(props=nodeProps)]
        else:
            self.nodes = None

    def init(self, mapping, testsuite):
        TestCase.init(self, mapping, testsuite)
        if self.nodes:
            self.servers = self.nodes + self.servers

    def teardownClientSide(self, current, success):
        if self.nodes:
            for n in self.nodes:
                n.shutdown(current)

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

    def __init__(self):
        self.multicast = False

    def useBinDist(self, mapping, current):
        return Component._useBinDist(self, mapping, current, "DATASTORM_BIN_DIST")

    def getInstallDir(self, mapping, current):
        # No binary distribution on Windows, nuget package only.
        envHomeName = None if isinstance(platform, Windows) else "DATASTORM_HOME"
        return Component._getInstallDir(self, mapping, current, envHomeName)

    def getOptions(self, testcase, current):
        return { "multicast": [False, True] }

    def getSupportedArgs(self):
        return ("", ["multicast"])

    def usage(self):
        print("")
        print("DataStorm options:")
        print("--multicast           Run with multicast discovery.")

    def parseOptions(self, options):
        parseOptions(self, options)

    def getProps(self, process, current):
        if self.multicast:
            return {}
        else:
            props = {}
            props["DataStorm.Node.Multicast.Enabled"] = 0
            if isinstance(process, Writer) :
                props["DataStorm.Node.Server.Enabled"] = 0
                props["DataStorm.Node.ConnectTo"] = "tcp -p 12345"
            else:
                props["DataStorm.Node.Server.Endpoints"] = "tcp -p 12345"
            return props

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
