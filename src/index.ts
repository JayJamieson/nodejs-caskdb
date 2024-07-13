// export * from "./db.js";
// export * from "./encoding.js";
import { openCask } from "./db.js";

const sizes = [1024, 2048, 4096, 8192, 16384];

for (const size of sizes) {
  const name = `logs_${size}`;
  const db = await openCask(name, {
    maxLogSize: size,
    sync: true,
  });

  console.time(name);
  // fill with enough data to cause multiple log files
  for (let index = 1; index <= 10000; index++) {
    await db.set("".padStart(5, "0") + index, "".padStart(5,"0") + index);
  }
  console.timeEnd(name);

  await db.close();
}
