import fs from "node:fs/promises";
import { Buffer } from "node:buffer";
import { HEADER_SIZE, decodeHeader, decodeKV, encodeKV } from "./encoding.js";

export type NodeCaskOptions = {
  /**
   * Max size in bytes for active log file before making it readonly
   * and creating a new active log file.
   *
   * Support sizes: 1024, 4096, 8192, 16384
   *
   * Larger are supported but can take up more memory while restoring
   */
  maxSegmentSize: number;
};

const defaultOptions: NodeCaskOptions = {
  maxSegmentSize: 4 * 1024,
};

export type KeyEntry = {
  fileId: number;
  position: number;
  size: number;
  timestamp: number;
};

export async function open(name: string, options?: NodeCaskOptions) {
  const { maxSegmentSize: maxLogSize } = options ?? defaultOptions;

  /**
   * interal use pointing to current location in log/cask file
   */
  let _cursor = 0;

  /**
   * internal mapping of key to value offset
   */
  const keyDir = new Map<string, KeyEntry>();

  // currently we only handle single log file for persistance
  // bitcask paper would have you use multiple fixed size log
  // files and merging as needed.
  // This means we currently also break the immutability
  const handle = await fs.open(`${name}.dat`, "a+");

  await _replay();

  /**
   * Read our log file and replay changes against keyDir
   * until EOF.
   */
  async function _replay() {
    const buffer = Buffer.alloc(maxLogSize);

    const readResult = await handle.read(buffer, 0, maxLogSize);

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
      if (value === "💩" &&  keyDir.has(key)) {
        keyDir.delete(key);
        offset += entrySize;
        continue;
      }

      keyDir.set(key, {
        fileId: 0, // Assuming single file for now
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
    const keyEntry = keyDir.get(key);

    if (keyEntry === undefined) {
      return null;
    }
    const buffer = Buffer.alloc(keyEntry.size);

    // TODO: #1 optimise read to only read in value as needed and not whole entry
    await handle.read(buffer, 0, keyEntry.size, keyEntry.position);
    const result = decodeKV(buffer, 0);

    return result.value;
  }

  async function set(key: string, value: string) {
    const timestamp = Date.now();

    const data = encodeKV(timestamp, key, value);
    const writeResult = await handle.write(data);
    await handle.sync();

    keyDir.set(key, {
      fileId: 0,
      timestamp,
      size: writeResult.bytesWritten,
      position: _cursor,
    });

    _cursor += writeResult.bytesWritten;
  }

  async function remove(key: string): Promise<void> {
    const timestamp = Date.now();

    const data = encodeKV(timestamp, key, "💩");
    const writeResult = await handle.write(data);
    await handle.sync();

    keyDir.delete(key);

    _cursor += writeResult.bytesWritten;
  }

  return {
    // TODO: implement list keys
    // TODO: implement fold
    // TODO: implement merge
    get,
    set,
    delete: remove,
    close: handle.close,
  };
}
