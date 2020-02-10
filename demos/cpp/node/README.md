This demo illustrates the use of a DataStorm node to discover writers and readers without using UDP multicast.

It also demonstrates how readers and writers can exchange data through the
DataStorm node when the server endpoints of both the writers and readers are
disabled.

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
writers and readers. Stopping the DataStorm node will prevent data to be
exchanged and new readers or writers to be discovered. Retarting the node
should resume the data exchange and discovery.

You can also try enabling the writer or reader server endpoints by editing
the `config.writer` or `config.reader` file. If enabled either on the writer
or the reader, a connection between the writer and reader will be established
from the process with the endpoint disabled to the process with the endpoint
still enabled. After discovery, the readers and writers will communicate directly between each other. Stopping the DataStorm node shouldn't prevent the
writers to continue sending data to the readers but no new readers or writers
will be discovered until the node is restarted.
