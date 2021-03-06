const Promise = require("bluebird");
const { ObjectBrigadeQueue } = require("../lib/tools/object-brigade");

function logger(type, every) {
  return offset => {
    if (!(offset % every)) console.log(`${type} ${offset}`);
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
    await log(id);
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
    const { token, offset } = next;
    await log(offset);
    await token();
  }
  console.log(`Finished reading`);
}

async function main(verb) {
  const ob = new ObjectBrigadeQueue("tmp/bb1");

  const actions = {
    read: readSequence,
    write: writeSequence,
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
