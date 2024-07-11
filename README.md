# node-caskdb

> A Log-Structured Hash Table for Fast Key/Value Data

Based on [bitcask](https://riak.com/assets/bitcask-intro.pdf) paper, though not a complete and production grade implementation.

This is still a work in progress and probably won't ever be fully complete. It is only really useful as learning tool for getting started in building databases.

## Usage

```ts
import { openCask } from "./db.js";

// Opens a cask databse in a cask directory with db.dat
const cask = await openCask("./cask/db");

await cask.set("mykey", "myvalue");
const myValue = await cask.get("mykey");
```

## Roadmap

For now I think it would be interesting to explore some of the following abstractions

- Snapshot/hint file creation to make reloading from disk "faster"
- Pluggable storage API. Create an interface that can be implemented for core IO methods, could allow using S3 as a storage layer for example.
- Add prefix index strategy for "fast" scans on keys like `"somekey:*"` or `"some/key"` similar to what S3 does for prefix list queries.
- Namespaces for allowing partitioned key value storage. Could lazily load as needed, and only load "default" db.
- Batch insert API to make use of a grouped/batch `fsync`ing of buffered data.
