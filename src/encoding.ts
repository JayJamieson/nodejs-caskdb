/**
 * ┌───────────────┬──────────────┬────────────────┐
 * │ timestamp(8B) │ key_size(4B) │ value_size(4B) │
 * └───────────────┴──────────────┴────────────────┘
 *
 * First three fields store unsigned integers of size 8 and 4 bytes, making
 * the header a fixed length of 16 bytes
 */
export const HEADER_SIZE = 16;

export type KeyValue = {
  timestamp: number;
  key: string;
  value: string;
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
