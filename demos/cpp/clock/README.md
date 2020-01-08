This demo illustrates how to implement a custom encoder and decoder for the
topic value type `chrono::system_clock::time_point` and the use of a DataStorm
node to discover writers and readers without using UDP multicast.

To run the demo, start a DataStorm node:
```
dsnode --Ice.Config=config.node
```

In a separate window, start the writer and specify the name of a city:
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
