import fs, { type FileHandle } from "node:fs/promises";
import { Buffer } from "node:buffer";
import { HEADER_SIZE, decodeHeader, decodeKV, encodeKV } from "./encoding.js";
import path from "node:path";

function nextLogFileName(start: number) {
  let counter = start;

  return () => {
    counter += 1;
    return `${counter.toString().padStart(5, "0")}.dat`;
  };
}

export type NodeCaskOptions = {
  /**
   * Max size in bytes for active log file before making it readonly
   * and creating a new active log file.
   *
   * Support sizes: 1024, 4096, 8192, 16384
   *
   * Larger are supported but can take up more memory while restoring
   */
  maxLogSize: number | 1024 | 4096 | 8192 | 16384;
};

export const DefaultOptions: NodeCaskOptions = {
  maxLogSize: 4 * 1024,
};

export type KeyEntry = {
  filename: string;
  position: number;
  size: number;
  timestamp: number;
};

export async function openCask(name: string, options?: NodeCaskOptions) {
  const { maxLogSize: maxLogSize } = options ?? DefaultOptions;

  if (maxLogSize < 1024 || maxLogSize > 16384) {
    throw new Error("maxSize needs to be between 1024 and 16384");
  }

  /**
   * interal use pointing to current location in log/cask file
   */
  let _cursor = 0;

  /**
   * internal mapping of key to value offset
   */
  const _keyDir = new Map<string, KeyEntry>();
  let _casks: string[] = [];

  try {
    _casks = (await fs.readdir(name)).sort();
  } catch (error: unknown) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      await fs.mkdir(name);
    } else {
      // rethrow, what can we actualy do from here?
      throw error;
    }
  }

  let currentCask = `${(_casks.length + 1).toString().padStart(5, "0")}.dat`;

  let _handle: FileHandle = await fs.open(path.join(name, currentCask), "a+");

  for (const cask of _casks) {
    if (/[0-9a-z]{5}/.test(cask)) {
      await _load(cask);
    }
  }

  /**
   * Read our log file and replay changes against keyDir
   * until EOF.
   */
  async function _load(filename: string) {
    const buffer = Buffer.alloc(maxLogSize);
    const readonlyCaskPath = path.join(name, filename);
    const handle = await fs.open(readonlyCaskPath, "a+");
    const readResult = await handle.read(buffer, 0, maxLogSize);
    await handle.close();

    // empty log, probably safe to return
    if (readResult.bytesRead === 0) {
      return;
    }

    let offset = 0;
    while (offset < readResult.bytesRead) {
      if (readResult.bytesRead - offset < HEADER_SIZE) {
        // Not enough data for a complete header, move file position forward in outer loop
        break;
      }

      const [timestamp, keySize, valueSize] = decodeHeader(buffer, offset);
      const entrySize = HEADER_SIZE + keySize + valueSize;

      if (readResult.bytesRead - offset < entrySize) {
        // Not enough data for a complete entry, move file position forward in out loop
        break;
      }

      const key = buffer.toString(
        "utf8",
        offset + HEADER_SIZE,
        offset + HEADER_SIZE + keySize,
      );

      const value = buffer.toString(
        "utf8",
        offset + HEADER_SIZE + keySize,
        offset + HEADER_SIZE + keySize + valueSize,
      );

      // tombstone value encountered, remove if exists otherwise continue
      if (value === "💩" && _keyDir.has(key)) {
        _keyDir.delete(key);
        offset += entrySize;
        continue;
      }

      _keyDir.set(key, {
        filename: filename,
        size: HEADER_SIZE + keySize + valueSize,
        // TODO: #1 write value position offset
        // position: filePosition + offset + HEADER_SIZE + keySize,
        position: offset,
        timestamp: timestamp,
      });

      offset += entrySize;
    }
  }

  async function get(key: string): Promise<string | null> {
    const keyEntry = _keyDir.get(key);

    if (keyEntry === undefined) {
      return null;
    }

    const buffer = Buffer.alloc(keyEntry.size);
    const readonlyCaskPath = path.join(name, keyEntry.filename);
    const handle = await fs.open(readonlyCaskPath, "r");
    // TODO: #1 optimise read to only read in value as needed and not whole entry

    await handle.read(buffer, 0, keyEntry.size, keyEntry.position);
    const result = decodeKV(buffer, 0);

    return result.value;
  }

  async function set(key: string, value: string) {
    const timestamp = Date.now();

    const data = encodeKV(timestamp, key, value);

    if (_cursor + data.length > maxLogSize) {
      await _handle.close();
      _casks.push(currentCask);
      currentCask = `${(_casks.length + 1).toString().padStart(5, "0")}.dat`;
      _handle = await fs.open(path.join(name, currentCask), "a+");
      _cursor = 0;
    }

    const writeResult = await _handle.write(data);

    // TODO: implement batch/group sync after N bytes written
    await _handle.sync();

    _keyDir.set(key, {
      filename: currentCask,
      timestamp,
      size: writeResult.bytesWritten,
      position: _cursor,
    });

    _cursor += writeResult.bytesWritten;
  }

  async function remove(key: string): Promise<void> {
    const timestamp = Date.now();

    const data = encodeKV(timestamp, key, "💩");
    const writeResult = await _handle.write(data);
    await _handle.sync();

    _keyDir.delete(key);
    _cursor += writeResult.bytesWritten;
  }

  function listKeys() {
    return [..._keyDir.keys()];
  }

  async function fold(
    callback: (key: string, value: string, path?: string) => void,
  ) {
    for (const entryTuple of _keyDir.entries()) {
      const [key, keyEntry] = entryTuple;
      const buffer = Buffer.alloc(keyEntry.size);

      // TODO: #1 optimise read to only read in value as needed and not whole entry
      const handle = await fs.open(path.join(name, keyEntry.filename), "r");
      await handle.read(buffer, 0, keyEntry.size, keyEntry.position);
      await handle.close();

      const header = decodeHeader(buffer, 0);

      const outValue = buffer.toString(
        "utf8",
        HEADER_SIZE + header[1],
        HEADER_SIZE + header[1] + header[2],
      );

      callback(key, outValue);
    }
  }

  async function merge(): Promise<void> {
    /**
     * Close current handle, nothing should be able to write to it anyway during a merge.
     * Thats the idea at least.
     */
    await _handle.close();

    const oldFiles = [..._casks, currentCask];

    const nextName = nextLogFileName(oldFiles.length);

    let nextFileName = nextName();
    let nextFile = await fs.open(path.join(name, nextFileName), "a+");

    /**
     * There is probably a more efficient algorithm to do this. Instead of
     * open close the logs we could probably keep it open somewhere.
     *
     * Iterate over each/value pair and follow ptr to where latest value is
     * and write them to new log file and update keydir with new location
     *
     * This assumes that keyDir as source of truth.
     */
    let cursor = 0;
    for (const keyEntry of _keyDir.entries()) {
      const [key, entry] = keyEntry;
      const buff = Buffer.alloc(entry.size);

      const oldFile = await fs.open(path.join(name, entry.filename), "r");

      const readResult = await oldFile.read(
        buff,
        0,
        entry.size,
        entry.position,
      );

      if (cursor + readResult.bytesRead > maxLogSize) {
        await nextFile.close();
        nextFileName = nextName();
        nextFile = await fs.open(path.join(name, nextFileName), "a+");
        cursor = 0;
      }

      const writeResult = await nextFile.write(buff, 0, buff.length, cursor);
      await nextFile.sync();

      _keyDir.set(key, {
        filename: nextFileName,
        position: cursor,
        size: writeResult.bytesWritten,
        timestamp: entry.timestamp,
      });

      await oldFile.close();
      cursor += writeResult.bytesWritten;
    }

    await nextFile.close();

    // remove old logs
    for (const file of oldFiles) {
      await fs.rm(path.join(name, file));
    }

    //
    currentCask = nextName();
    _handle = await fs.open(path.join(name, currentCask), "a+");
  }

  async function sync(): Promise<void> {
    await _handle.sync();
  }

  return {
    get,
    set,
    delete: remove,
    listKeys,
    fold,
    merge,
    sync,
    close: _handle.close,
  };
}
