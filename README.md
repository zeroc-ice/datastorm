<p align="center">
  <img src="https://raw.githubusercontent.com/zeroc-ice/datastorm/main/.github/assets/ice-banner.svg" height="150" width="150" />
</p>


# DataStorm - Data Centric Pub/Sub Framework

DataStorm is a new high-performance brokerless data centric [pub/sub
framework](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern). It
allows you to distribute data between your applications through a simple yet
powerful API.

The implementation of DataStorm relies on Ice, and DataStorm naturally plays
well with Ice: you can easily distribute data defined using Ice with DataStorm.
You can also use DataStorm without Ice, in particular:
 * you don't need to know Ice or Ice APIs to use DataStorm
 * you can easily distribute data with simple types using DataStorm
 * you can distribute data with more complex types using DataStorm by providing
   your own serialization/deserialization functions (for Ice types, DataStorm
   uses automatically the Ice-generated marshaling and unmarshaling code)

- [DataStorm vs IceStorm](#datastorm-vs-icestorm)
- [Languages](#languages)
- [Platforms](#platforms)
- [Branches](#branches)
- [Documentation](#documentation)
- [Copyright and License](#copyright-and-license)

## DataStorm vs IceStorm

Ice already provides a pub/sub service named IceStorm. So if you need pub/sub
with Ice, should you use IceStorm or DataStorm?

IceStorm is broker-based pub/sub service, where the broker (the IceStorm
service) can be replicated for fault-tolerance. It is mature and available for
all programming languages supported by Ice.

IceStorm is all about distributing remote Ice calls: when your publisher makes a
oneway call on a given topic, it makes a regular Ice remote call, and IceStorm
replicates and routes this call to all the subscribers registered with this
topic. These subscribers are regular Ice objects that you implement, and they
receive the message sent by the publisher just like any other Ice request
dispatch.

DataStorm is a brand-new library-based pub/sub service.

DataStorm is all about distributing data. When one of your applications needs
data produced by another application, DataStorm helps you publish, filter and
receive data items very easily - you don't need to worry about network
connections or making remote calls.

## Languages

DataStorm supports only the C++ programming language.

## Platforms

You should be able to build DataStorm on the same platforms and with the same
C++ compilers as Ice. DataStorm relies on C++11 features and the Ice C++11 mapping.

## Branches

- `main`
  DataStorm 1.1.x plus various patches (stable, frequently updated)

## Documentation

- [DataStorm Release Notes](https://doc.zeroc.com/datastorm/latest/release-notes)
- [DataStorm Manual](https://doc.zeroc.com/datastorm/latest)

## Copyright and License

Copyright &copy; ZeroC, Inc. All rights reserved.

As copyright owner, ZeroC can license DataStorm under different license terms, and
offers the following licenses for DataStorm:
- GPL v2, a popular open-source license with strong
[copyleft](https://en.wikipedia.org/wiki/Copyleft) conditions (the default
license)
- Commercial or closed-source licenses

If you license DataStorm under GPL v2, there is no license fee or signed license
agreement: you just need to comply with the GPL v2 terms and conditions. See
[LICENSE](./LICENSE) for further information.

If you purchase a commercial or closed-source license DataStorm, you must comply
with the terms and conditions listed in the associated license agreement; the
GPL v2 terms and conditions do not apply.

The DataStorm software itself remains the same: the only difference between an open-source
DataStorm and a commercial DataStorm are the license terms.
