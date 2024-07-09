import fsp, { type FileHandle } from "node:fs/promises";
import { Buffer } from "node:buffer";
import { HEADER_SIZE, decodeHeader, decodeKV, encodeKV } from "./encoding.js";
import path from "node:path";

export type NodeCaskOptions = {
  /**
   * Max size in bytes for active log file before making it readonly
   * and creating a new active log file.
   *
   * Support sizes: 1024, 4096, 8192, 16384
   *
   * Larger are supported but can take up more memory while restoring
   */
  maxSize: number | 1024 | 4096 | 8192 | 16384;
};

const defaultOptions: NodeCaskOptions = {
  maxSize: 4 * 1024,
};

export type KeyEntry = {
  filename: string;
  position: number;
  size: number;
  timestamp: number;
};

export async function openCask(name: string, options?: NodeCaskOptions) {
  const { maxSize: maxLogSize } = options ?? defaultOptions;

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
    _casks = (await fsp.readdir(name)).sort();
  } catch (error: unknown) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      await fsp.mkdir(name);
    }
    // TODO what do here if error not missing directory?
  }

  const currentCask = `${(_casks.length + 1).toString(36).padStart(5, "0")}.dat`;

  const _handle: FileHandle = await fsp.open(path.join(
    name,
    currentCask,
  ), "a+");

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
    const handle = await fsp.open(readonlyCaskPath, "a+");
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
    const handle = await fsp.open(readonlyCaskPath, "r");
    // TODO: #1 optimise read to only read in value as needed and not whole entry

    await handle.read(buffer, 0, keyEntry.size, keyEntry.position);
    const result = decodeKV(buffer, 0);

    return result.value;
  }

  async function set(key: string, value: string) {
    const timestamp = Date.now();

    const data = encodeKV(timestamp, key, value);
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

  async function fold(callback: (key: string, value: string) => void) {
    for (const entryTuple of _keyDir.entries()) {
      const [key, keyEntry] = entryTuple;
      const buffer = Buffer.alloc(keyEntry.size);

      // TODO: #1 optimise read to only read in value as needed and not whole entry
      await _handle.read(buffer, 0, keyEntry.size, keyEntry.position);
      const header = decodeHeader(buffer, 0);

      const value = buffer.toString(
        "utf8",
        HEADER_SIZE + header[1],
        HEADER_SIZE + header[1] + header[2],
      );

      callback(key, value);
    }
  }

  async function sync(): Promise<void> {
    await _handle.sync();
  }

  return {
    // TODO: implement merge
    get,
    set,
    delete: remove,
    listKeys,
    fold,
    sync,
    merge: () => {
      throw new Error("Merge not supported");
    },
    close: _handle.close,
  };
}
