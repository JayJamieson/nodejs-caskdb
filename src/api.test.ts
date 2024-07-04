import { decodeKV, open } from "./index.js";
import { beforeEach, expect, suite, test } from "vitest";
import fs from "node:fs/promises";

suite("API tests", () => {
  beforeEach(async () => {
    await fs.mkdir(".testdata/");
    return async () => {
      await fs.rm(".testdata/", {force: true, recursive: true});
    };
  });

  test("set() persists to disk", async () => {
    const db = await open(".testdata/set.db");
    await db.set("foo", "bar");
    await db.close();

    const buffer = await fs.readFile(".testdata/set.db");
    const entry = decodeKV(buffer, 0);

    expect(22).toBe(buffer.length);
    expect("foo").toBe(entry.key);
    expect("bar").toBe(entry.value);

  });

  test("get() reads from disk", async () => {
    const db = await open(".testdata/get.db");
    await db.set("foo", "bar");

    const value = await db.get("foo");

    expect("bar").toBe(value);
  });


  test("open existing db replays changes", async () => {
    let db = await open(".testdata/replay.db");
    await db.set("foo", "foobar");
    await db.close();

    db = await open(".testdata/replay.db");

    const value = await db.get("foo");
    expect("foobar").toBe(value);
  });

  test("open existing db replays changes to latest change", async () => {
    let db = await open(".testdata/replay.db");
    await db.set("foo", "foobar1");
    await db.set("foo", "foobar2");
    await db.set("foo", "foobar3");
    await db.close();

    db = await open(".testdata/replay.db");

    const value = await db.get("foo");
    expect("foobar3").toBe(value);
  });
});
