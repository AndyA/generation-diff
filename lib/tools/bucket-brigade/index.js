const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const { StateFile } = require("../state-file");
const { QueueReader, StoreReader } = require("./readers");

class BucketBrigadeBase {
  constructor(dir, opt) {
    this.dir = dir;
    this.opt = Object.assign(
      { dirSize: 1000, pathSegments: 3, extension: "" },
      opt || {}
    );

    this.state = null;
    this.initPromise = null;
    this.nextOut = 0;
    this.capacity = Math.pow(this.opt.dirSize, this.opt.pathSegments);
    this._readers = {};
  }

  async init() {
    const init = async () => {
      // Make our directory
      await fs.promises.mkdir(this.dir, { recursive: true });

      const { opt } = this;
      // Init or load state
      const state = await StateFile.create(
        path.join(this.dir, "bb-state.json"),
        { out: { pos: 0, closed: false }, opt }
      );

      if (!_.isEqual(opt, state.current.opt))
        throw new Error(`State mismiatch`);

      this.state = state;
      // this.nextIn = state.current.in.pos;
      this.nextOut = state.current.out.pos;
    };

    return (this.initPromise = this.initPromise || init());
  }

  async _getReader(key, clazz) {
    await this.init();
    return (this._readers[key] = this._readers[key] || new clazz(this, key));
  }

  get closed() {
    return this.state.current.out.closed;
  }

  makeName(seq) {
    const { opt, dir } = this;
    const digits = (opt.dirSize - 1).toString().length;
    const parts = [];

    for (let i = 0; i < opt.pathSegments; i++) {
      parts.push(seq % opt.dirSize);
      seq = Math.floor(seq / opt.dirSize);
    }

    // Shouldn't be able to happen
    if (seq) throw new Error("Sequence overflow");

    const np = parts.reverse().map(p => _.padStart(p, digits, "0"));
    np[np.length - 1] += opt.extension;
    return path.join(dir, ...np);
  }

  async waitForCommit() {
    return this.state.waitForChange();
  }

  async _setClosed(closed, extraMutator) {
    const mutator = state => {
      state.closed = closed;
    };
    await this.state.makeToken("out", [mutator, extraMutator])();
  }

  async open(extraMutator) {
    await this._setClosed(false, extraMutator);
  }

  async close(extraMutator) {
    await this._setClosed(true, extraMutator);
  }

  async nextOutput(extraMutator) {
    await this.init();

    if (this.closed) throw new Error(`Can't append to a closed stream`);

    await this.waitForSpace();

    const offset = this.nextOut;
    const next = (this.nextOut = this.incrementOffset(offset));
    const name = this.makeName(offset);

    await fs.promises.mkdir(path.dirname(name), { recursive: true });

    const mutator = state => {
      state.pos = next;
    };

    const token = this.state.makeToken("out", [mutator, extraMutator]);

    return { name, token };
  }

  async nextInput(extraMutator) {
    const reader = await this.getReader("in");
    return await reader.nextInput(extraMutator);
  }
}

class BucketBrigadeQueue extends BucketBrigadeBase {
  async getReader(key) {
    if (key !== "in")
      throw new Error(`A queue may only have one reader called "in"`);
    return await this._getReader(key, QueueReader);
  }

  get space() {
    const { nextOut, capacity, state } = this;
    if (!state) throw new Error(`Please call init() first`);

    // Check all the readers
    const spaces = Object.keys(this._readers).flatMap(key => {
      const pos = state.current[key]?.pos;
      if (pos === undefined) return [];
      const space = pos - nextOut - 1;
      return [space < 0 ? space + capacity : space];
    });

    return Math.min(capacity, ...spaces);
  }

  async waitForSpace() {
    while (this.space === 0) await this.waitForCommit();
  }

  incrementOffset(offset) {
    return (offset + 1) % this.capacity;
  }
}

class BucketBrigadeStore extends BucketBrigadeBase {
  async getReader(key) {
    return await this._getReader(key, StoreReader);
  }

  get space() {
    const { nextOut, capacity } = this;
    return capacity - nextOut;
  }

  async waitForSpace() {
    if (this.space === 0) throw new Error(`Database full (${this.capacity})`);
  }

  incrementOffset(offset) {
    return offset + 1;
  }
}

module.exports = { BucketBrigadeQueue, BucketBrigadeStore };
