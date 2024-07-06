import { openCask } from "./db.js";

const cask = await openCask("./tmp/db");

console.time("new buffer");
for (let index = 0; index < 10000; index++) {
  await cask.set("0k".padStart(5) + index, "0v".padStart(5) + index);
}
console.timeEnd("new buffer");

await cask.close();
