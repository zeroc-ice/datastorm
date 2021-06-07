[![Join the chat at https://gitter.im/zeroc-ice/ice](https://badges.gitter.im/zeroc-ice/datastorm.svg)](https://gitter.im/zeroc-ice/datastorm?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

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

DataStorm is currently in alpha and we welcome your feedback!

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

DataStorm currently supports only the C++ programming language.

## Platforms

You should be able to build DataStorm on the same platforms and with the same
C++ compilers as Ice. DataStorm relies on C++11 features and the Ice C++11
mapping.

## Branches

- `main`
  Primary development branch (unstable, frequently updated)

- `1.0`
  DataStorm 1.0.x plus various patches (stable, frequently updated)

## Copyright and License

Copyright &copy; ZeroC, Inc. All rights reserved.

DataStorm is licensed under [GPLv2](http://opensource.org/licenses/GPL-2.0), a
popular open-source license with strong
[copyleft](http://en.wikipedia.org/wiki/Copyleft) conditions.

## Documentation

- [DataStorm Release Notes](https://doc.zeroc.com/datastorm/latest/release-notes)
- [DataStorm Manual](https://doc.zeroc.com/datastorm/latest)
