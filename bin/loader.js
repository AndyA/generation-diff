const _ = require("lodash");
const fs = require("fs");
const split2 = require("split2");
const { ObjectBrigadeStore } = require("../lib/tools/object-brigade");

const tabFields = s => s.split(/\t/);

const fixValue = v => (isNaN(v) ? v : Number(v));

const file = "/data/scratch/pi2/slurpstate.txt";
const db = "/db/scratch/pips.prev";

async function main(file, db) {
  const ob = new ObjectBrigadeStore(db);
  const src = fs.createReadStream(file).pipe(split2(tabFields));
  let names = null;
  let count = 0;
  for await (const vals of src) {
    if (names)
      await ob.write(Object.fromEntries(_.zip(names, vals.map(fixValue))));
    else names = vals;
    if (!(++count % 10000)) console.log(`Loaded ${count}`);
  }
  console.log(`Loaded ${count}`);
  await ob.close();
}

main(file, db).catch(e => {
  console.error(e);
  process.exit(1);
});