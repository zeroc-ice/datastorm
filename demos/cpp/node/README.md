This demo illustrates the use of a DataStorm node to discover writers and
readers without using UDP multicast. It also demonstrates how readers
and writers can exchange data through the DataStorm node when if don't
enable server endpoints.

To run the demo, start a DataStorm node:
```
dsnode --Ice.Config=config.node
```

In a separate window, start the writer:
```
writer
```

In a separate window, start the reader:
```
reader
```

The reader will print the time sent by the writer. You can start multiple
writers and readers.

Stopping the DataStorm node shouldn't prevent data to be exchanged but new
readers or writers won't be discovered until the DataStorm node is restarted.

You can also try disabling the writer or reader server endpoints by editing
the config.writer or config.reader file.

If disabled either on the writer or the reader, a connection between the
writer and reader will be established from the process with the endpoint
disabled to the process with the endpoint still enabled.

If disabled on both the reader and server, the reader and writer will exchange
data through the connection established to the DataStorm node which won't only
be used anymore to discover topics but will also relay the data for the reader
and writer.
