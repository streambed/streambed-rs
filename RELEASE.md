Releasing
===

1. Update the cargo.toml and change the `workspace.package.version`.
2. Change the `dependency.streambed` and `dependency.streambed-test` versions to be the same version number as per step 1.
3. Commit the changes and tag with the version using `v` as a prefix e.g. 1.0.0 would be "v1.0.0".
4. Perform the commands below, and in the same order...

```
cargo publish -p streambed
cargo publish -p streambed-test
cargo publish -p streambed-confidant
cargo publish -p streambed-confidant-cli
cargo publish -p streambed-kafka
cargo publish -p streambed-logged
cargo publish -p streambed-logged-cli
cargo publish -p streambed-patterns
cargo publish -p streambed-storage
cargo publish -p streambed-vault
cargo publish -p streambed-codec
```