streambed-proto
===

Declares a set of interfaces suitable for serialising aspects of Streambed, such as those of the commit log.

To compile, upon having installed `protoc`:

```
protoc src/stdce.proto -Isrc --cpp_out=target
```