// Load a couch dump file.

const _ = require("lodash");
const fs = require("fs");
const split2 = require("split2");
const { ObjectBrigadeStore } = require("../lib/tools/object-brigade");

const file = "/Volumes/db/pi2.json";
const db = "tmp/pips.base";

const dumpedDoc = s =>
  (m => (m ? JSON.parse(m[1]) : {}))(s.match(/^(\{.*\}),/));

const cleanRec = rec => _.pick(rec, "id", "ETag", "Key", "LastModified");

async function main(file, db) {
  const ob = new ObjectBrigadeStore(db);
  const src = fs.createReadStream(file).pipe(split2(dumpedDoc));
  let seen = 0,
    loaded = 0;
  for await (const rec of src) {
    if (rec?.doc?.s3) {
      await ob.write(cleanRec(rec.doc.s3));
      loaded++;
    }
    if (!(++seen % 10000))
      console.log(`Seen ${seen}, loaded ${loaded}, at ${rec?.id || "???"}`);
  }
  console.log(`Loaded ${seen}`);
  await ob.close();
}

main(file, db).catch(e => {
  console.error(e);
  process.exit(1);
});
