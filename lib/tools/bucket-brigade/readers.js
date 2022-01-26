const fs = require("fs");
const path = require("path");

class BaseReader {
  constructor(bb, key) {
    this.bb = bb;
    this.section = bb.state.getSection(key);
    this.next = this.section.current?.pos || 0;
  }

  get available() {
    const { next: nextIn, bb } = this;
    const { capacity, state } = bb;
    const avail = state.current.out.pos - nextIn;
    return avail < 0 ? avail + capacity : avail;
  }

  async waitForInput() {
    while (true) {
      if (this.available) return true;
      if (this.bb.closed) return false;
      await this.bb.waitForCommit();
    }
  }

  async nextInput(extraMutator) {
    const more = await this.waitForInput();
    if (!more) return null;

    const offset = this.next;
    const next = (this.next = this.bb.incrementOffset(offset));
    const name = path.join(this.bb.makeName(offset));

    const mutator = state => {
      state.pos = next;
    };

    const cleanup = this.makeCleanup(name);

    const token = this.section.makeToken([mutator, extraMutator], cleanup);

    return { name, token, offset };
  }

  makeCleanup(name) {
    return () => {};
  }

  async seek(offset) {
    throw new Error(`Reader is not seekable`);
  }
}

class QueueReader extends BaseReader {
  makeCleanup(name) {
    return async () => {
      await fs.promises.unlink(name);
    };
  }
}

class StoreReader extends BaseReader {
  async seek(offset) {
    const outPos = this.bb.state.current.out.pos;
    if (offset > outPos)
      throw new Error(
        `Can't seek beyond end of stream (${offset} > ${outPos})`
      );

    this.next = offset;
    const token = this.section.makeToken(state => {
      state.pos = offset;
    });

    await token();
  }
}

module.exports = { QueueReader, StoreReader };
