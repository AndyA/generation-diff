const fs = require("fs");
const split2 = require("split2");

const tabFields = s => s.split(/\t/);

async function main(file) {
  const src = fs.createReadStream(file).pipe(split2(tabFields));
  for await (const obj of src) {
    console.log(obj);
  }
}

main("tmp/000.jsonl").catch(e => {
  console.error(e);
  process.exit(1);
});
