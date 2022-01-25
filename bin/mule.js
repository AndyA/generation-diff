const Promise = require("bluebird");
const { ObjectBrigade } = require("../lib/tools/object-brigade");

function logger(type, every, delay) {
  let count = 0;
  return async () => {
    if (!(++count % every)) {
      console.log(`${type} ${count}`);
      if (delay) await Promise.delay(delay);
    }
  };
}

function makeRecord(id) {
  return {
    id: "availability_p002mq9f" + id,
    source: "appw",
    kind: "availability_clip",
    generation: 1159,
    ETag: "b8b87d1258264cb0a3baa5779f10e17d",
    Key: "programme_availability_cd/live/application/json/availability/clip/pid.p002mq9f",
    LastModified: "2019-08-23T08:55:30.000Z",
    Size: 3051,
    StorageClass: "STANDARD",
    rand: 0.9029780401875992
  };
}

// 2m47.260s
async function writeSequence(ob) {
  const log = logger(`write`, 100000);
  console.log(`Writing`);
  for (let id = 0; id < 80000000; id++) {
    await log();
    await ob.write(makeRecord(id));
  }
  await ob.close();
  console.log(`Finished writing`);
}

// 1m59.475s
async function readSequence(ob) {
  // await Promise.delay(500);
  const log = logger(`read`, 100000);
  console.log(`Reading`);
  while (true) {
    const next = await ob.read();
    if (!next) break;
    const { token } = next;
    await log();
    await token();
  }
  console.log(`Finished reading`);
}

// 1m46.995s
async function consumeSequence(ob) {
  const log = logger(`read`, 100000);
  console.log(`Consuming`);
  await ob.consume(log);
  console.log(`Finished consuming`);
}

async function main(verb) {
  const ob = new ObjectBrigade("tmp/bb1");
  // await ob.bb.init();
  const actions = {
    read: readSequence,
    write: writeSequence,
    consume: consumeSequence,
    both: ob => Promise.all([writeSequence(ob), readSequence(ob)])
  };

  const action = actions[verb];
  if (!action) throw new Error(`Bad action ${verb}`);
  await action(ob);
}

main(process.argv.slice(2))
  .catch(e => {
    console.error(`FATAL:`, e);
    process.exit(1);
  })
  .finally(() => {
    console.log(`All done`);
  });
