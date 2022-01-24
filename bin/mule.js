const fs = require("fs");
const _ = require("lodash");
const Promise = require("bluebird");
const { BucketBrigade } = require("../lib/tools/bucket-brigade");

async function writeSequence(bb) {
  console.log(`Writing`);
  for (const i of _.range(50)) {
    await Promise.delay(5);
    const { name, token } = await bb.nextOutput();
    console.log(`Writing ${name}`);
    await fs.promises.writeFile(name, JSON.stringify({ i, name }));
    await token();
    console.log(`Written ${name}`);
  }
  await bb.close();
  console.log(`Finished writing`);
}

async function readSequence(bb, count) {
  console.log(`Reading`);
  while (true) {
    if (count && --count < 1) break;
    await Promise.delay(20);
    const next = await bb.nextInput();
    if (!next) break;
    const { name, token } = next;
    console.log(`Reading ${name}`);
    const data = JSON.parse(await fs.promises.readFile(name, "utf8"));
    await token();
    console.log(`Read ${name}`, data);
  }
}

async function keepAlive() {
  while (true) {
    console.log(`Sleeping`);
    await Promise.delay(1000);
  }
}

async function main() {
  // setInterval(() => {}, 1 << 30);

  const bb = new BucketBrigade("tmp/bb1", { extension: ".json" });
  await Promise.all([writeSequence(bb), readSequence(bb)]);
  await keepAlive();
}

main()
  .catch(e => {
    console.error(`FATAL:`, e);
    process.exit(1);
  })
  .finally(() => {
    console.log(`All done`);
  });
