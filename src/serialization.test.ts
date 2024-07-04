import { expect, test } from "vitest";
import { decodeHeader, decodeKV, encodeHeader, encodeKV } from "./index.js";

test("test header serialization", () => {
  const buff = Buffer.alloc(16);
  const timeStamp = Date.now();

  encodeHeader(buff,timeStamp, 11, 12);
  const header = decodeHeader(buff, 0);
  expect(header.length).toBe(3);

  expect(header[0]).toBe(timeStamp);
  expect(header[1]).toBe(11);
  expect(header[2]).toBe(12);
});

test("test key value serialization", () => {
  const timeStamp = Date.now();
  const entryBuffer = encodeKV(timeStamp, "foo", "foobar");
  const entry = decodeKV(entryBuffer, 0);

  expect(entry.key).toBe("foo");
  expect(entry.value).toBe("foobar");
});

test("test empty key value serialization", () => {
  const timeStamp = Date.now();

  const entryBuffer = encodeKV(timeStamp, "", "");
  const entry = decodeKV(entryBuffer, 0);

  expect(entry.key).toBe("");
  expect(entry.value).toBe("");
});
