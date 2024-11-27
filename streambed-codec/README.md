# Codec

This crate provides a trait `Codec` and two (initial) implementations, `Cbor` and `CborEncrypted`.  
A `Codec` value is a convenient abstraction over the lower level serialisation and crypto functions
in `streambed`.

A `Codec` is also closely associated with a `CommitLog` and a `Topic` since values stored on the commit
log under a given topic will all be encoded the same way.   

The type `LogAdapter` is provided to wrap a `Codec`, `CommitLog` and `Topic`.   A `LogAdapter` carries
a type parameter for the decoded log values. It can be viewed as a typed counterpart to `CommitLog` 

The `produce` method on `LogAdapter` accepts a typed value, encodes it and appends it to the log.  
The `history` method returns a `Stream` of typed values from the commit log.

