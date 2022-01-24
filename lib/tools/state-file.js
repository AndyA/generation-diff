const { produce } = require("immer");
const _ = require("lodash");
const fs = require("fs");
const path = require("path");
const assert = require("assert");
const { EventEmitter } = require("events");

class StateFileStore {
  constructor(file) {
    this.file = file;
  }

  async save(data) {
    const { file } = this;
    const tmp = `${file}.tmp`;
    await fs.promises.mkdir(path.dirname(file), { recursive: true });
    await fs.promises.writeFile(tmp, JSON.stringify(data));
    await fs.promises.rename(tmp, file);
  }

  async load(fallback) {
    const { file } = this;
    try {
      return JSON.parse(await fs.promises.readFile(file, "utf8"));
    } catch (e) {
      if (!fallback || e.code !== "ENOENT") throw e;
      return fallback;
    }
  }
}

function applyMutators(state, mutators) {
  for (const mutator of _.castArray(mutators || []))
    if (mutator) state = produce(state, mutator);
  return state;
}

function forkingToken(tok) {
  if (tok.fork) throw new Error(`Already forking`);
  let refCount = 0;
  const fixups = [];
  tok.fork = () => {
    let called = 0;
    const cb = async fixup => {
      if (called++) throw new Error(`Completion token called more than once`);
      if (fixup) fixups.push(fixup);
      assert(refCount > 0);
      if (--refCount === 0) await Promise.resolve(tok(fixups));
    };
    cb.fork = tok.fork;
    refCount++;
    return cb;
  };

  return tok.fork();
}

class StateSequence {
  constructor(store) {
    this.store = store;
    this.tokens = [];
  }

  makeToken(mutator, cleanup) {
    const { tokens } = this;

    const token = { mutator, fixup: null, complete: false, cleanup };
    tokens.push(token);

    // Return an async function which must be called in the
    // future to mark this token complete.
    return forkingToken(async fixup => {
      if (token.complete)
        throw new Error(`Completion token called more than once`);

      token.complete = true;
      token.fixup = fixup;

      // Process any contiguous complete tokens.
      let next = this.store.current;
      const cleanups = [];
      while (tokens.length && tokens[0].complete) {
        const { mutator, fixup, cleanup } = tokens.shift();
        next = applyMutators(applyMutators(next, mutator), fixup);
        // Remember any cleanups for after the commit.
        if (cleanup) cleanups.push(cleanup);
      }

      // Commit
      await this.store.setState(next);

      // Cleanup
      for (const cu of cleanups) await Promise.resolve(cu());
    });
  }
}

class StateStore extends EventEmitter {
  constructor(store, current) {
    super();
    this.store = store;
    this.current = current;
    this._sequence = {};
    this.pending = null;
    this.resolving = {};
    this.pending = null;
    this.saving = null;
  }

  static async create(store, fallback) {
    return new this(store, await store.load(fallback));
  }

  getSequence(name) {
    return (this._sequence[name] =
      this._sequence[name] || new StateSequence(this));
  }

  get sequence() {
    return this.getSequence("_");
  }

  async commit() {
    // Wait for previous save to complete
    if (this.saving) await this.saving;

    // Anything still pending?
    const pending = this.pending;
    if (pending) {
      // Clear pending to indicate we have it in hand.
      this.pending = null;

      // Save with mutex
      await (this.saving = this.store.save(pending));

      // Clear mutex promise
      this.saving = null;

      // Send a message
      this.emit("commit", pending);
    }
  }

  async setState(state) {
    if (state !== this.current) {
      this.pending = this.current = state;
      await this.commit();
    }
  }

  makeToken(...args) {
    return this.sequence.makeToken(...args);
  }

  async doOnce(tag, resolver) {
    const { current, resolving } = this;
    // Cached?
    if (current._once && current._once[tag]) return current._once[tag][0];

    const resolve = async () => {
      const tok = this.makeToken();
      const val = await Promise.resolve(resolver());
      delete resolving[tag];
      await tok(state => {
        const slot = (state._once = state._once || {});
        slot[tag] = [val];
      });
      return val;
    };

    return await (resolving[tag] = resolving[tag] || resolve());
  }
}

class StateFile extends StateStore {
  static async create(stateFile, fallback) {
    return super.create(new StateFileStore(stateFile), fallback);
  }
}

module.exports = { StateStore, StateFile };
