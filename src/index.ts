import fs from "node:fs/promises";
import { Buffer } from "node:buffer";
import { write } from "node:fs";

/**
 * ┌───────────────┬──────────────┬────────────────┐
 * │ timestamp(8B) │ key_size(4B) │ value_size(4B) │
 * └───────────────┴──────────────┴────────────────┘
 *
 * First three fields store unsigned integers of size 8 and 4 bytes, making
 * the header a fixed length of 16 bytes
 */
const HEADER_SIZE = 16;

export type KeyValue = {
  timestamp: number;
  key: string;
  value: string;
};

export type KeyEntry = {
  fileId: number;
  position: number;
  size: number;
  timestamp: number;
};

export function encodeHeader(
  buff: Buffer,
  timestamp: number,
  kSize: number,
  vSize: number,
) {
  buff.writeDoubleLE(timestamp, 0);
  buff.writeUInt32LE(kSize, 8);
  buff.writeUInt32LE(vSize, 12);
}

export function decodeHeader(
  buffer: Buffer,
  offset: number,
): [number, number, number] {
  return [
    buffer.readDoubleLE(offset),
    buffer.readUInt32LE(offset + 8),
    buffer.readUInt32LE(offset + 12),
  ];
}

export function encodeKV(timestamp: number, key: string, value: string): Buffer {
  const kSize = Buffer.byteLength(key);
  const vSize = Buffer.byteLength(value);

  const buff = Buffer.alloc(HEADER_SIZE + kSize + vSize);

  encodeHeader(buff, timestamp, kSize, vSize);

  buff.write(`${key}${value}`, HEADER_SIZE);

  return buff;
}

export function decodeKV(buff: Buffer, offset: number): KeyValue {
  const [timestamp, kSize, vSize] = decodeHeader(buff, offset);

  const key = buff.toString("utf8", HEADER_SIZE, HEADER_SIZE + kSize);
  const value = buff.toString(
    "utf8",
    HEADER_SIZE + kSize,
    HEADER_SIZE + kSize + vSize,
  );

  return {
    key: key,
    value: value,
    timestamp: timestamp,
  };
}

export async function open(path: string) {
  const keyDir = new Map<string, KeyEntry>();

  // currently we only handle single log file for persistance
  // bitcask paper would have you use multiple fixed size log
  // files and merging as needed.
  // This means we currently also break the immutability
  const handle = await fs.open(path, "a+");
  // const fstat = await fs.stat(path);

  // current position of "log" file
  let cursor = 0;

  await _replay();

  /**
   * Read our log file and replay changes against keyDir
   * until EOF.
   */
  async function _replay() {
    // 64k buffer? very arbitrary
    const bufferSize = 32 * 1024;
    const buffer = Buffer.alloc(bufferSize);

    let filePosition = 0;

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const readResult = await handle.read(buffer, 0, bufferSize, filePosition);

      // empty file just return
      if (readResult.bytesRead === 0) {
        break;
      }

      let offset = 0;
      while (offset < readResult.bytesRead) {

        if (readResult.bytesRead - offset < HEADER_SIZE) {
          // Not enough data for a complete header, move file position forward in out loop
          break;
        }

        const [timestamp, keySize, valueSize] = decodeHeader(buffer, offset);
        const entrySize = HEADER_SIZE + keySize + valueSize;

        if (readResult.bytesRead - offset < entrySize) {
          // Not enough data for a complete entry, move file position forward in out loop
          break;
        }

        const key = buffer.toString("utf8", offset + HEADER_SIZE, offset + HEADER_SIZE + keySize);

        // const value = buffer.toString(
        //   "utf8",
        //   offset + HEADER_SIZE + keySize,
        //   offset + HEADER_SIZE + keySize + valueSize,
        // );

        keyDir.set(key, {
          fileId: 0, // Assuming single file for now
          size: HEADER_SIZE + keySize + valueSize,
          position: filePosition + offset + HEADER_SIZE + keySize,
          timestamp: timestamp,
        });

        offset += entrySize;
      }

      filePosition += offset;
    }
  }

  return {
    // TODO: implement list keys
    // TODO: implement fold
    // TODO: implement merge
    // TODO: implement sync

    delete: () => {},
    get: async (key: string): Promise<string | null> => {
      const keyEntry = keyDir.get(key);

      if (keyEntry === undefined) {
        return null;
      }
      const buffer = Buffer.alloc(keyEntry.size);

      await handle.read(buffer, 0, keyEntry.size, keyEntry.position);
      const result = decodeKV(buffer, 0);

      return result.value;
    },
    set: async (key: string, value: string) => {
      const timestamp = Date.now();

      const data = encodeKV(timestamp, key, value);
      const writeResult = await handle.write(data);
      await handle.sync();

      keyDir.set(key, {
        fileId: 0,
        timestamp,
        size: writeResult.bytesWritten,
        position: cursor
      });

      cursor += writeResult.bytesWritten;
    },
    close: async () => {
      await handle.close();
    },
  };
}


const handle = await open("tmp.db");

// uncomment to fill with data
// for (let index = 1; index <= 50; index++) {
//   await handle.set(`${index}k`.padStart(5,"0"),  `${index}v`.padStart(5,"0"));
//   console.log(`${index}k`.padStart(5,"0"),  `${index}v`.padStart(5,"0"));
// }

// uncomment to print key and values
// for (let index = 1; index <= 50; index++) {
//   const value = await handle.get(`${index}k`.padStart(5,"0"));
//   console.log(value);
// }


await handle.close();
