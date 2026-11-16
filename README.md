# mqtt

This is an MQTT library written in C and C++.

It is composed of the following layers:

## The protocol layer

The bottom layer is written in C and contains no dependencies, no networking code.

It tries to somewhat neutrally describe the protocol, and should more or less read as the protocol spec
transcribed to C types.

This has:

* [proto.h](https://github.com/asveikau/mqtt/blob/master/include/mqtt/proto.h), basic types and declarations from the spec
* [messages.h](https://github.com/asveikau/mqtt/blob/master/include/mqtt/messages.h), an abstraction for parsed MQTT packets
* Generated code for the different message types
* [Reader](https://github.com/asveikau/mqtt/blob/master/include/mqtt/reader.h) and [writer](https://github.com/asveikau/mqtt/blob/master/include/mqtt/writer.h) interfaces, which can deserialize and serialize packets

## The client layer

This piece is written in C++, with a public API callable from C.
This is an opinionated client library.

See [client.h](https://github.com/asveikau/mqtt/blob/master/include/mqtt/client.h).

I/O is abstracted into a small set of callbacks, and you can provide your own, but the provided reference implementation is a default set of callbacks calling into another library of mine, [libpollster](http://github.com/asveikau/pollster).
This cross platform library provides an event loop, I/O multiplexing, and TLS support. 

As of now there is no way to call it without a libpollster event loop running.  For C callers, or for callers who
don't care about the details of my I/O library, I may add an abstraction for the event loop at a later date.

## `mqtt-client` binary

`mqtt-client` is a proof of concept executable using the library pieces and providing a command line interface.

It supports the following command line arguments:

* `-u user`, `-p passwordfile`

MQTT username and password.  Note that -p reads from a file, as reading passwords from a command line is considered insecure.

* `-publish topic -utf8 string [-retain] [-qos <0-2>]`

Publish `topic`, with a payload of the UTF-8 string `string`.  `-retain` enables the retain flag.  `-qos` sets a QOS level.

* `-subscribe topic [-qos <0-2>]

Subscribe to `topic`, which may also use the MQTT protocol wildcards, such as `#`.  `-qos` sets a requested QOS level.

* `-exec path-to-program [args] ...`

Run a script when a payload response to `-subscribe` comes in.
The topic will be stored in an evironment varible `MQTT_TOPIC`.
The payload will be piped to stdin.

This feature has not yet been ported to Windows.

* `-wait`

Default is off.

Without this, the library will bail at connection failures and it will exit once subscribe or publish responses come in. 
Enabling this will leave the socket connected, useful for long-term subscribes.  It will also retry establishing the TCP connection on failure.

* `-keepalive <seconds>`

A timeout for sending pings and triggering re-connect.  Valid values are between 0 and 65535.
This is the same as the timeout sent in the MQTT CONNECT packet.

