const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const { StateFile } = require("./state-file");

class BucketBrigade {
  constructor(dir, opt) {
    this.dir = dir;
    this.opt = Object.assign(
      { dirSize: 1000, pathSegments: 3, extension: "" },
      opt || {}
    );

    this.state = null;
    this.nextIn = 0;
    this.nextOut = 0;
  }

  async init() {
    const init = async () => {
      // Make our directory
      await fs.promises.mkdir(this.dir, { recursive: true });

      const { opt } = this;
      // Init or load state
      this.state = await StateFile.create(
        path.join(this.dir, "bb-state.json"),
        { in: 0, out: 0, closed: false, opt }
      );

      if (!_.isEqual(opt, this.state.current.opt))
        throw new Error(`State mismiatch`);

      this.nextIn = this.state.current.in;
      this.nextOut = this.state.current.out;
      this.capacity = Math.pow(opt.dirSize, opt.pathSegments);

      return this.state;
    };

    return (this.state = this.state || init());
  }

  get available() {
    const { nextIn, capacity, state } = this;
    if (!state) throw new Error(`Please call init() first`);
    const avail = state.current.out - nextIn;
    return avail < 0 ? avail + capacity : avail;
  }

  get space() {
    const { nextOut, capacity, state } = this;
    if (!state) throw new Error(`Please call init() first`);
    const space = state.current.in - nextOut - 1;
    return space < 0 ? space + capacity : space;
  }

  get closed() {
    return this.state.current.closed;
  }

  makeName(seq) {
    const { opt } = this;
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
    return path.join(...np);
  }

  async waitForCommit() {
    // Without the interval timer Node's listener count can drop to zero
    // when the event fires - if there are no other pending listeners.
    // If that happens the process will exit immediately.
    // Temporarily creating a timer guards against that.
    const it = setInterval(() => {}, 1 << 30);
    await new Promise(resolve => this.state.once("commit", resolve));
    clearInterval(it);
  }

  async waitForInput() {
    while (true) {
      if (this.available) return true;
      if (this.closed) return false;
      await this.waitForCommit();
    }
  }

  async waitForSpace() {
    while (this.space === 0) await this.waitForCommit();
  }

  async close() {
    await this.state.getSequence("output").makeToken(state => {
      state.closed = true;
    })();
  }

  async nextOutput() {
    await this.init();

    if (this.closed) throw new Error(`Can't append to a closed stream`);

    await this.waitForSpace();

    const name = path.join(this.dir, this.makeName(this.nextOut));
    await fs.promises.mkdir(path.dirname(name), { recursive: true });
    const next = (this.nextOut = (this.nextOut + 1) % this.capacity);

    const token = this.state.getSequence("output").makeToken(state => {
      state.out = next;
    });

    return { name, token };
  }

  async nextInput() {
    await this.init();

    const more = await this.waitForInput();
    if (!more) return null;

    const name = path.join(this.dir, this.makeName(this.nextIn));
    const next = (this.nextIn = (this.nextIn + 1) % this.capacity);

    const token = this.state.makeToken(
      state => {
        state.in = next;
      },
      async () => {
        await fs.promises.unlink(name);
      }
    );

    return { name, token };
  }
}

module.exports = { BucketBrigade };
