The entries below contain brief descriptions of the changes in each release, in
no particular order. Some of the entries reflect significant new additions,
while others represent minor corrections. Although this list is not a
comprehensive report of every change we made in a release, it does provide
details on the changes we feel Ice users might need to be aware of.

We recommend that you use the release notes as a guide for migrating your
applications to this release, and the manual for complete details on a
particular aspect of DataStorm.

# Changes in DataStorm 0.2

These are the changes since DataStorm 0.1.

- Added two new properties, `DataStorm.Node.Multicast.Enabled` and
  `DataStorm.Node.Server.Enabled`, to enable or disable the multicast and
  server endpoints. The default value for these properties is 1, meaning the
  endpoints are enabled. Set the property to 0 to disable the corresponding
  endpoints.

- Added ability for a node to communicate with other nodes without UDP
  multicast discovery. A node can now connect directly to another node
  using the new `DataStorm.Node.ConnectTo` property. This connected-to node
  then relays discovery and (if needed) communications with other nodes it
  is itself connected with.
