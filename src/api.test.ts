import { decodeKV } from "./encoding.js";
import {openCask} from "./db.js";

import { beforeEach, expect, suite, test, vi } from "vitest";
import fs from "node:fs/promises";

suite("API tests", () => {
  beforeEach(async () => {
    await fs.mkdir(".testdata/");
    return async () => {
      await fs.rm(".testdata/", {force: true, recursive: true});
    };
  });

  test("set() persists to disk", async () => {
    const db = await openCask(".testdata/set");
    await db.set("foo", "bar");
    await db.close();

    const buffer = await fs.readFile(".testdata/set.dat");
    const entry = decodeKV(buffer, 0);

    expect(22).toBe(buffer.length);
    expect("foo").toBe(entry.key);
    expect("bar").toBe(entry.value);

  });

  test("get() reads from disk", async () => {
    const db = await openCask(".testdata/get");
    await db.set("foo", "bar");

    const value = await db.get("foo");

    expect("bar").toBe(value);
  });


  test("open existing db replays changes", async () => {
    let db = await openCask(".testdata/replay");
    await db.set("foo", "foobar");
    await db.close();

    db = await openCask(".testdata/replay");

    const value = await db.get("foo");
    expect("foobar").toBe(value);
  });

  test("open existing db replays changes to latest change", async () => {
    let db = await openCask(".testdata/replay");
    await db.set("foo", "foobar1");
    await db.set("foo", "foobar2");
    await db.set("foo", "foobar3");
    await db.close();

    db = await openCask(".testdata/replay");

    const value = await db.get("foo");
    expect("foobar3").toBe(value);
  });

  test("delete() deletes key", async () => {
    const db = await openCask(".testdata/delete");

    await db.set("foo", "deleteme!");
    await db.delete("foo");

    const value = await db.get("foo");

    expect(null).toBe(value);
  });

  test("delete() persists after close and open", async () => {
    let db = await openCask(".testdata/delete_replay");

    await db.set("foo", "deleteme!");
    await db.delete("foo");
    await db.close();

    db = await openCask(".testdata/delete_replay");

    const value = await db.get("foo");

    expect(null).toBe(value);
  });

  test("fold() calls callback with key and value", async () => {
    const db = await openCask(".testdata/fold");

    await db.set("k1", "v1");
    await db.set("k2", "v2");
    await db.set("k3", "v3");
    const callback = vi.fn((k: string, v: string) => {});

    await db.fold(callback);

    expect(callback).toBeCalledTimes(3);
    expect(callback).toBeCalledWith("k3", "v3");
    expect(callback).toBeCalledWith("k1", "v1");
    expect(callback).toBeCalledWith("k2", "v2");
  });

  test("listKeys() returns all keys added", async () => {
    const db = await openCask(".testdata/listKeys");

    await db.set("k1", "v1");
    await db.set("k2", "v2");
    await db.set("k3", "v3");

    const keys = db.listKeys();

    expect(["k1", "k2", "k3"]).toStrictEqual(keys);
  });

});
