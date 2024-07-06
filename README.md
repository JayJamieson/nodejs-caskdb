# node-caskdb

> A Log-Structured Hash Table for Fast Key/Value Data

Based on [bitcask](https://riak.com/assets/bitcask-intro.pdf) paper, though not a complete and production grade implementation.

This is still a work in progress and probably won't ever be fully complete. It is only really useful as learning tool for getting started in building databases, as it was for me.

Currently `merge()` and data file rollover are un implemented. This means a single log file is used for storing all data for reloading from disk.

## Usage

```ts
import { openCask } from "./db.js";

// Opens a cask databse in a cask directory with db.dat
const cask = await openCask("./cask/db");

await cask.set("mykey", "myvalue");
const myValue = await cask.get("mykey");
```
