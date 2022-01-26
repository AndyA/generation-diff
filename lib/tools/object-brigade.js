const fs = require("fs");
const { BucketBrigadeQueue, BucketBrigadeStore } = require("./bucket-brigade");

class Reader {
  constructor(ob, key) {
    this.ob = ob;
    this.key = key;
    this.readQueue = [];
    this.eof = false;
  }

  async getReader() {
    return await this.ob.bb.getReader(this.key);
  }

  async nextChunk() {
    const reader = await this.getReader();
    const next = await reader.nextInput();

    if (!next) {
      this.eof = true;
      return null;
    }

    const { name, token } = next;
    const data = await fs.promises.readFile(name, "utf8");
    const objects = data.split(/\n/).map(JSON.parse);
    return { objects, token };
  }

  async fillReadQueue(skip = 0) {
    const next = await this.nextChunk();
    if (!next) return;

    const { objects, token, offset } = next;

    // Each line gets its own completion token
    this.readQueue = objects.slice(skip).map((object, i) => ({
      object,
      token: token.fork(),
      offset: offset * this.ob.opt.chunkSize + i + skip
    }));

    // Complete the original base token.
    await token();
  }

  async read() {
    if (this.readQueue.length) return this.readQueue.shift();
    if (this.eof) return null;

    await this.fillReadQueue();
    if (this.readQueue.length) return this.readQueue.shift();
    return null;
  }

  async seek(offset) {
    const reader = await this.getReader();
    const { chunkSize } = this.ob.opt;
    await reader.seek(Math.floor(offset / chunkSize));
    await this.fillReadQueue(offset % chunkSize);
  }
}

class ObjectBrigadeBase {
  constructor(dir, opt = {}, clazz) {
    const { chunkSize, ...bbOpt } = {
      chunkSize: 10000,
      extension: ".jsonl",
      ...opt
    };
    this.opt = { chunkSize };
    this.bb = new clazz(dir, bbOpt);
    this.writeQueue = [];
    this._readers = {};
  }

  get state() {
    return this.bb.state;
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

  async write(doc) {
    if (this.writeQueue.length >= this.opt.chunkSize)
      await this.flushWriteQueue();
    this.writeQueue.push(doc);
  }

  getReader(key) {
    return (this._readers[key] = this._readers[key] || new Reader(this, key));
  }

  async read() {
    return await this.getReader("in").read();
  }
}

class ObjectBrigadeQueue extends ObjectBrigadeBase {
  constructor(dir, opt) {
    super(dir, opt, BucketBrigadeQueue);
  }
}

class ObjectBrigadeStore extends ObjectBrigadeBase {
  constructor(dir, opt) {
    super(dir, opt, BucketBrigadeStore);
  }
}

module.exports = { ObjectBrigadeQueue, ObjectBrigadeStore };
