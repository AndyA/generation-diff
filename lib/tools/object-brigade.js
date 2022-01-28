const fs = require("fs");
const { BucketBrigadeQueue, BucketBrigadeStore } = require("./bucket-brigade");

class Reader {
  constructor(ob, key) {
    this.ob = ob;
    this.key = key;
    this.buffer = [];
    this.eof = false;
  }

  async getReader() {
    return await this.ob.bb.getReader(this.key);
  }

  async nextChunk() {
    console.log(this);
    const reader = await this.getReader();
    const next = await reader.nextInput();

    if (!next) {
      this.eof = true;
      return null;
    }

    const { name, token, offset } = next;
    const data = await fs.promises.readFile(name, "utf8");
    const objects = data.split(/\n/).map(JSON.parse);
    return { objects, token, offset };
  }

  async fillBuffer(skip = 0) {
    const next = await this.nextChunk();
    if (!next) return;

    const { objects, token, offset } = next;

    // Each line gets its own completion token
    this.buffer = objects.slice(skip).map((object, i) => ({
      object,
      token: token.fork(),
      offset: offset * this.ob.opt.chunkSize + i + skip
    }));

    // Complete the original base token.
    await token();
  }

  async clearBuffer() {
    const work = this.buffer;
    this.buffer = [];
    await Promise.all(work.map(({ token }) => token()));
  }

  async read() {
    if (this.buffer.length) return this.buffer.shift();
    if (this.eof) return null;

    await this.fillBuffer();
    if (this.buffer.length) return this.buffer.shift();
    return null;
  }

  async seek(offset) {
    const reader = await this.getReader();
    const { chunkSize } = this.ob.opt;
    await this.clearBuffer();
    await reader.seek(Math.floor(offset / chunkSize));
    await this.fillBuffer(offset % chunkSize);
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
    this.buffer = [];
    this._readers = {};
  }

  get state() {
    return this.bb.state;
  }

  // Approx size
  get size() {
    return this.bb.size * this.opt.chunkSize;
  }

  async close() {
    await this.flushBuffer();
    await this.bb.close();
  }

  async flushBuffer() {
    const { bb, buffer } = this;
    this.buffer = [];
    if (buffer.length) {
      const { name, token } = await bb.nextOutput();
      const data = buffer.map(JSON.stringify).join("\n");
      await fs.promises.writeFile(name, data);
      await token();
    }
  }

  async write(doc) {
    if (this.buffer.length >= this.opt.chunkSize) await this.flushBuffer();
    this.buffer.push(doc);
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
