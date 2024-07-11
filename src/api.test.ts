import { decodeKV } from "./encoding.js";
import { openCask } from "./db.js";

import { beforeEach, expect, suite, test, vi } from "vitest";
import fs from "node:fs/promises";

suite("API tests", () => {
  beforeEach(async () => {
    await fs.mkdir("testdata");

    return async () => {
      await fs.rm("testdata", { force: true, recursive: true });
    };
  });

  test("set() persists to disk", async () => {
    const db = await openCask("testdata");
    await db.set("foo", "bar");
    await db.close();

    const buffer = await fs.readFile("testdata/00001.dat");
    const entry = decodeKV(buffer, 0);

    expect(22).toBe(buffer.length);
    expect("foo").toBe(entry.key);
    expect("bar").toBe(entry.value);
  });

  test("get() reads from disk", async () => {
    const db = await openCask("testdata");
    await db.set("foo", "bar");

    const value = await db.get("foo");
    await db.close();

    expect("bar").toBe(value);
  });

  test("open existing db replays changes", async () => {
    let db = await openCask("testdata");
    await db.set("foo", "foobar");
    await db.close();

    db = await openCask("testdata");

    const value = await db.get("foo");
    expect("foobar").toBe(value);
  });

  test("open existing db replays changes to latest change", async () => {
    let db = await openCask("testdata");
    await db.set("foo", "foobar1");
    await db.set("foo", "foobar2");
    await db.set("foo", "foobar3");
    await db.close();

    db = await openCask("testdata");

    const value = await db.get("foo");
    expect("foobar3").toBe(value);
  });

  test("delete() deletes key", async () => {
    const db = await openCask("testdata");

    await db.set("foo", "deleteme!");
    await db.delete("foo");
    await db.close();

    const value = await db.get("foo");

    expect(null).toBe(value);
  });

  test("delete() persists after close and open", async () => {
    let db = await openCask("testdata");

    await db.set("foo", "deleteme!");
    await db.delete("foo");
    await db.close();

    db = await openCask("testdata");

    const value = await db.get("foo");

    expect(null).toBe(value);
  });

  test("fold() calls callback with key and value", async () => {
    const db = await openCask("testdata");

    await db.set("k1", "v1");
    await db.set("k2", "v2");
    await db.set("k3", "v3");
    const callback = vi.fn((k: string, v: string) => {});

    await db.fold(callback);
    await db.close();

    expect(callback).toBeCalledTimes(3);
    expect(callback).toBeCalledWith("k3", "v3");
    expect(callback).toBeCalledWith("k1", "v1");
    expect(callback).toBeCalledWith("k2", "v2");
  });

  test("fold() with multi log file", async () => {
    const db = await openCask("testdata", {
      maxLogSize: 1024,
    });

    for (let index = 1; index <= 50; index++) {
      await db.set("0k".padStart(5) + index, "0v".padStart(5) + index);
    }

    const callback = vi.fn((k: string, v: string) => {});

    await db.fold(callback);

    expect(callback).toBeCalledTimes(50);

    for (let index = 1; index <= 50; index++) {
      expect(callback).toBeCalledWith(
        "0k".padStart(5) + index,
        "0v".padStart(5) + index,
      );
    }

    await db.close();
  });

  test("listKeys() returns all keys added", async () => {
    const db = await openCask("testdata");

    await db.set("k1", "v1");
    await db.set("k2", "v2");
    await db.set("k3", "v3");

    const keys = db.listKeys();
    await db.close();

    expect(["k1", "k2", "k3"]).toStrictEqual(keys);
  });

  test("reaching maxLogSize triggers roll over to new log", async () => {
    const db = await openCask("testdata", {
      maxLogSize: 1024,
    });

    for (let index = 1; index <= 35; index++) {
      await db.set("0k".padStart(5) + index, "0v".padStart(5) + index);
    }

    await db.close();

    const logs = (await fs.readdir("testdata")).sort();

    expect(2).toBe(logs.length);
  });

  test("get works after maxLogSize rollover in set", async () => {
    const db = await openCask("testdata", {
      maxLogSize: 1024,
    });

    // causes rollover to second file
    for (let index = 1; index <= 35; index++) {
      await db.set("0k".padStart(5) + index, "0v".padStart(5) + index);
    }

    const a = await db.get("0k".padStart(5) + 35);
    const b = await db.get("0k".padStart(5) + 34);

    expect(a).toBe("0v".padStart(5) + 35);
    expect(b).toBe("0v".padStart(5) + 34);

    await db.close();
  });

  test("merge() merges log files into compact form", async () => {
    const db = await openCask("testdata", {
      maxLogSize: 1024,
    });

    // fill with enough data to cause multiple log files
    for (let index = 1; index <= 50; index++) {
      await db.set("0k".padStart(5) + index, "0v".padStart(5) + index);
    }

    // perform updates on subset
    for (let index = 1; index <= 35; index++) {
      await db.set("0k".padStart(5) + index, "0V".padStart(5) + index);
    }

    // delete middle keys
    for (let index = 11; index <= 40; index++) {
      await db.delete("0k".padStart(5) + index);
    }

    // before merge
    const beforeMerge = await fs.readdir("./testdata");

    await db.merge();

    const afterMerge = await fs.readdir("./testdata");

    expect(beforeMerge.length).toBe(3);
    expect(afterMerge.length).toBe(2);

    await db.close();
  });
});
