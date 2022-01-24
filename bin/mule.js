const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const Promise = require("bluebird");
const { StateFile } = require("../lib/tools/state-file");

class FileSequence {
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
        path.join(this.dir, "fs-state.json"),
        { in: 0, out: 0, opt }
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
    console.log(`waitForCommit`);
    await new Promise(resolve =>
      this.state.once("commit", state => {
        console.log(`commit`, state);
        resolve();
      })
    );
    console.log(`got commit`);
  }

  async waitForInput() {
    while (this.available === 0) await this.waitForCommit();
  }

  async waitForSpace() {
    while (this.space === 0) await this.waitForCommit();
  }

  async nextOutput() {
    await this.init();

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

    console.log(`waitForInput`);
    await this.waitForInput();
    console.log(`gotInput`);

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

async function writeSequence(seq) {
  console.log(`Writing`);
  for (const i of _.range(10)) {
    await Promise.delay(10);
    const { name, token } = await seq.nextOutput();
    console.log(`Writing ${name}`);
    await fs.promises.writeFile(name, JSON.stringify({ i, name }));
    await token();
    console.log(`Written ${name}`);
  }
  console.log(`Finished writing`);
}

async function readSequence(seq, count) {
  console.log(`Reading`);
  while (true) {
    if (count && --count < 1) break;
    console.log(`delay`);
    await Promise.delay(20);
    console.log(`nextInput`);
    const { name, token } = await seq.nextInput();
    console.log(`Reading ${name}`);
    const data = JSON.parse(await fs.promises.readFile(name, "utf8"));
    await token();
    console.log(`Read ${name}`, data);
  }
}

async function main() {
  const seq = new FileSequence("tmp/fs1", { extension: ".json" });
  // await writeSequence(seq);
  await readSequence(seq, 3);
  await readSequence(seq, 3);
  await readSequence(seq);
  console.log(`Finished reading`);
  // const res = await Promise.all([writeSequence(seq), readSequence(seq)]);
  // console.log(`Got:`, res);
}

main()
  .catch(e => {
    console.error(`FATAL:`, e);
    process.exit(1);
  })
  .finally(() => {
    console.log(`All done`);
  });
