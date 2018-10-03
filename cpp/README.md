# Building DataStorm for C++

This file describes how to build DataStorm for C++ from source and how to test
the resulting build.

ZeroC provides [DataStorm binary distributions][1] for many platforms and
compilers, including Windows and Visual Studio, so building DataStorm from
source is usually unnecessary.

* [C++ Build Requirements](#c-build-requirements)
  * [Operating Systems and Compilers](#operating-systems-and-compilers)
  * [Third-Party Libraries](#third-party-libraries)
    * [Linux](#linux)
    * [macOS](#macos)
    * [Windows](#windows)
* [Building DataStorm for Linux or macOS](#building-datastorm-for-linux-or-macos)
  * [Build configurations and platforms](#build-configurations-and-platforms)
* [Building DataStorm for Windows](#building-datastorm-for-windows)
* [Installing a C++ Source Build on Linux or macOS](#installing-a-c-source-build-on-linux-or-macos)
* [Creating a NuGet Package on Windows](#creating-a-nuget-package-on-windows)
* [Running the Test Suite](#running-the-test-suite)

## C++ Build Requirements

### Operating Systems and Compilers

DataStorm was extensively tested using the operating systems and compiler
versions listed on [supported platforms][2].

### Third-Party Libraries

DataStorm depends on the Ice for C++11 library. ZeroC supplies Ice binary
packages for all the platforms supported by DataStorm.

#### Linux

Refer to the [Ice distribution installation instructions][3] to install Ice on
your Linux platform.

#### macOS

You can install Ice using Homebrew:
```
brew install ice
```

#### Windows

The DataStorm build system for Windows downloads and installs the NuGet command-
line executable and the required NuGet packages when you build DataStorm. The
packages are installed in the `datastorm/cpp/msbuild/packages` folder.

## Building DataStorm for Linux or macOS

Review the top-level [config/Make.rules](../config/Make.rules) in your build
tree and update the configuration if needed. The comments in the file provide
more information.

In a command window, change to the `cpp` subdirectory:
```
cd cpp
```
Run `make` to build the DataStorm C++ library and test suite. Set `V=1` to
get a more detailed build output. You can build only the libraries and services
with the `srcs` target, or only the tests with the `tests` target. For example:
```
make V=1 -j8 srcs
```

The build system supports specifying additional preprocessor, compiler and
linker options with the `CPPFLAGS`, `CXXFLAGS` and `LDFLAGS` variables.

### Build configurations and platforms

The C++ source tree supports multiple build configurations and platforms. To
see the supported configurations and platforms:
```
make print V=supported-configs
make print V=supported-platforms
```
To build all the supported configurations and platforms:
```
make CONFIGS=all PLATFORMS=all -j8
```

## Building DataStorm on Windows

Open a Visual Studio command prompt. For example, with Visual Studio 2015, you
can open one of:

- VS2015 x86 Native Tools Command Prompt
- VS2015 x64 Native Tools Command Prompt

Using the first Command Prompt produces `Win32` binaries by default, while
the second Command Promt produces `x64` binaries by default.

In the Command Prompt, change to the `cpp` subdirectory:
```
cd cpp
```

Now you're ready to build DataStorm:
```
msbuild msbuild\datastorm.proj
```

This builds the DataStorm for C++ SDK and the DataStorm for C++ test suite, with
Release binaries for the default platform.

Set the MSBuild `Configuration` property to `Debug` to build debug binaries
instead:
```
msbuild msbuild\datastorm.proj /p:Configuration=Debug
```

The `Configuration` property may be set to `Debug` or `Release`.

Set the MSBuild `Platform` property to `Win32` or `x64` to build binaries
for a specific platform, for example:
```
msbuild msbuild\datastorm.proj /p:Configuration=Debug /p:Platform=x64
```

You can also skip the build of the test suite with the `BuildDist` target:
```
msbuild msbuild\datastorm.proj /t:BuildDist /p:Platform=x64
```

To build the test suite using the NuGet binary distribution use:
```
msbuild msbuild\datastorm.proj /p:DATASTORM_BIN_DIST=all
```

You can also sign the DataStorm binaries with Authenticode, by setting the
following environment variables:

 - `SIGN_CERTIFICATE` to your Authenticode certificate
 - `SIGN_PASSWORD` to the certificate password

## Installing a C++ Source Build on Linux or macOS

Simply run `make install`. This will install Ice in the directory specified by
the `<prefix>` variable in `../config/Make.rules`.

After installation, make sure that the `<prefix>/bin` directory is in your
`PATH`.

If you choose to not embed a `runpath` into executables at build time (see your
build settings in `../config/Make.rules`) or did not create a symbolic link from
the `runpath` directory to the installation directory, you also need to add the
library directory to your `LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH (macOS).

On a Linux x86_64 system:
```
<prefix>/lib64                 (RHEL, SLES, Amazon)
<prefix>/lib/x86_64-linux-gnu  (Ubuntu)
```

On macOS:
```
<prefix>/lib
```

When compiling DataStorm programs, you must pass the location of the
`<prefix>/include` directory to the compiler with the `-I` option, and the
location of the library directory with the `-L` option.

You must also define the `ICE_CPP11_MAPPING` macro during compilation with the
`-D` option (`c++ -DICE_CPP11_MAPPING`) and add the `-lDataStorm -lIce++11`
options when linking.

## Creating a NuGet Package on Windows

You can create a NuGet package with the following command:
```
msbuild msbuild\datastorm.proj /t:NuGetPack /p:BuildAllConfigurations=yes
```

This creates `zeroc.datastorm.v140\zeroc.datastorm.v140.nupkg` or
`zeroc.datastorm.v141\zeroc.datastorm.v141.nupkg`
depending on the compiler you are using.

## Cleaning the source build on macOS or Linux

Running `make clean` will remove the binaries created for the default
configuration and platform.

To clean the binaries produced for a specific configuration or platform, you
need to specify the `CONFIGS` or `PLATFORMS` variable. To clean the build for
all the supported configurations and platforms, run `make CONFIGS=all
PLATFORMS=all clean`.

Running `make distclean` will also clean the build for all the configurations
and platforms. In addition, it will also remove the generated files created by
the Slice translators.

## Running the Test Suite

Python is required to run the test suite.

After a successful source build, you can run the tests as follows:
```
python allTests.py # default config and platform
```

* Windows
```
python allTests.py --config Debug
python allTests.py --config Release
```

If everything worked out, you should see lots of `ok` messages. In case of a
failure, the tests abort with `failed`.

[1]: https://zeroc.com/distributions/datastorm
[2]: https://doc.zeroc.com/display/Rel/Supported+Platforms+for+DataStorm+0.1.0
[3]: https://doc.zeroc.com/ice/3.7/release-notes/using-the-linux-binary-distributions#id-.UsingtheLinuxBinaryDistributionsv3.7-InstallingtheLinuxDistributions
