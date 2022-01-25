const fs = require("fs");
const _ = require("lodash");
const { BucketBrigade } = require("./bucket-brigade");

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

module.exports = { ObjectBrigade };
