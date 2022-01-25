const fs = require("fs");
const _ = require("lodash");
const Promise = require("bluebird");
const { BucketBrigade } = require("../lib/tools/bucket-brigade");

class ObjectBrigade {
  constructor(dir, opt = {}) {
    const { chunkSize, ...bbOpt } = {
      chunkSize: 10000,
      extension: ".jsonl",
      ...opt
    };
    this.opt = { chunkSize };
    this.bb = new BucketBrigade(dir, bbOpt);
    this.writeQueue = [];
    this.readQueue = [];
    this.eof = false;
  }

  async close() {
    await this.flushWriteQueue();
    await this.bb.close();
  }

  async flushWriteQueue() {
    const { bb, writeQueue } = this;
    this.writeQueue = [];
    if (writeQueue.length) {
      const { name, token } = await bb.nextOutput();
      const data = writeQueue.map(JSON.stringify).join("\n");
      await fs.promises.writeFile(name, data);
      await token();
    }
  }

  async nextChunk() {
    const next = await this.bb.nextInput();

    if (!next) {
      this.eof = true;
      return null;
    }

    const { name, token } = next;
    const data = await fs.promises.readFile(name, "utf8");
    const objects = data.split(/\n/).map(JSON.parse);
    return { objects, token };
  }

  async fillReadQueue() {
    const next = await this.nextChunk();
    if (!next) return;

    const { objects, token } = next;
    // Each line gets its own completion token
    this.readQueue = objects.map(object => ({ object, token: token.fork() }));

    // Complete the original base token.
    await token();
  }

  async write(doc) {
    if (this.writeQueue.length >= this.opt.chunkSize)
      await this.flushWriteQueue();
    this.writeQueue.push(doc);
  }

  async read() {
    if (this.readQueue.length) return this.readQueue.shift();
    if (this.eof) return null;

    await this.fillReadQueue();
    if (this.readQueue.length) return this.readQueue.shift();
    return null;
  }

  async consume(consumer) {
    // First consume anything that's in readQueue. They
    // have individual tokens that need to be called
    // after processing each item.
    if (this.readQueue.length) {
      const work = this.readQueue;
      this.readQueue = [];
      await Promise.all(
        work.map(({ token, object }) =>
          Promise.resolve(consumer(object)).then(() => token())
        )
      );
    }

    // Switch to complete chunks. Only call the completion
    // token once the whole chunk has been processed.
    while (true) {
      const next = await this.nextChunk();
      if (!next) return;
      const { token, objects } = next;
      await Promise.all(
        objects.map(object => Promise.resolve(consumer(object)))
      ).then(() => token());
    }
  }
}

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

async function readSequence(ob) {
  // await Promise.delay(500);
  const log = logger(`read`, 100000);
  console.log(`Reading`);
  while (true) {
    const next = await ob.read();
    if (!next) break;
    const { object, token } = next;
    await log();
    await token();
  }
  console.log(`Finished reading`);
}

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
