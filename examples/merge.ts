import { openCask } from "../src/db.js";

const db = await openCask("logs", {
  maxLogSize: 1024
});

// fill with enough data to cause multiple log files
for (let index = 1; index <= 50; index++) {
  await db.set("".padStart(5, "k") + index, "".padStart(5, "v") + index);
}

// perform updates on subset
for (let index = 1; index <= 35; index++) {
  await db.set("".padStart(5, "k") + index, "".padStart(5, "V") + index);
}

// delete middle keys
for (let index = 11; index <= 40; index++) {
  await db.delete("".padStart(5, "k") + index);
}

await db.merge();

await db.fold((key: string, value: string) => {
  console.log(key, value);
});

await db.close();
