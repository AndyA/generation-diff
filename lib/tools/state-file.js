const { produce } = require("immer");
const _ = require("lodash");
const fs = require("fs");
const path = require("path");
const lockfile = require("proper-lockfile");
const assert = require("assert");
const Promise = require("bluebird");

class StateFileStore {
  constructor(file) {
    this.file = file;
  }

  async save(state) {
    const { file } = this;
    const tmp = `${file}.tmp`;
    await fs.promises.mkdir(path.dirname(file), { recursive: true });
    await fs.promises.writeFile(tmp, JSON.stringify(state));
    await fs.promises.rename(tmp, file);
  }

  async load(fallback) {
    const { file } = this;
    try {
      return JSON.parse(await fs.promises.readFile(file, "utf8"));
    } catch (e) {
      if (!fallback || e.code !== "ENOENT") throw e;
      await this.save(fallback);
      return fallback;
    }
  }

  async mutate(mutator) {
    const { file } = this;
    const release = await lockfile(file, { retries: 10 });
    try {
      const state = await this.load();
      const newState = mutator(state);
      if (state !== newState) await this.save(newState);
      return newState;
    } finally {
      await release();
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
  constructor(ss) {
    this.ss = ss;
    this.tokens = [];
  }

  makeToken(mutator, cleanup) {
    const { tokens } = this;

    const token = { mutator, fixup: null, complete: false, cleanup };
    tokens.push(token);

    const getComplete = () => {
      const todo = tokens.findIndex(t => !t.complete);
      return todo < 0 ? tokens.length : todo;
    };

    // Return an async function which must be called in the
    // future to mark this token complete.
    return forkingToken(async fixup => {
      if (token.complete)
        throw new Error(`Completion token called more than once`);

      token.complete = true;
      token.fixup = fixup;

      let cleanups;
      const todo = getComplete(); // at least one
      const pending = tokens.slice(0, todo);

      // Mutate function must be idempotent
      this.current = await this.ss.mutate(state => {
        cleanups = [];
        for (const { mutator, fixup, cleanup } of pending) {
          state = applyMutators(applyMutators(state, mutator), fixup);
          // Remember any cleanups for after the commit.
          if (cleanup) cleanups.push(cleanup);
        }
        return state;
      });

      tokens.splice(0, todo);

      // Cleanup
      for (const cu of cleanups) await Promise.resolve(cu());
    });
  }
}

const delayMin = 100;
const delayMax = 10000;

class StateStore {
  constructor(store, current) {
    this.store = store;
    this.current = current;
    this._sequence = {};
    this._resolving = {};
  }

  static async create(store, fallback) {
    return new this(store, await store.load(fallback));
  }

  getSequence(name) {
    return (this._sequence[name] =
      this._sequence[name] || new StateSequence(this));
  }

  makeToken(...args) {
    return this.getSequence("_").makeToken(...args);
  }

  async mutate(mutator) {
    this.current = await this.store.mutate(mutator);
  }

  async waitForChange() {
    let delay = delayMin;
    while (true) {
      const state = this.current;
      await Promise.delay(delay);
      await this.mutate(s => s);
      // Deep compare because we've reloaded the underlying
      // document. Abstraction leak from StateFileStore?
      if (!_.isEqual(state, this.current)) return;
      delay = Math.min(delay * 2, delayMax);
    }
  }

  async doOnce(tag, resolver) {
    const { current, _resolving } = this;
    // Cached?
    if (current._once && current._once[tag]) return current._once[tag][0];

    const resolve = async () => {
      const tok = this.makeToken();
      const val = await Promise.resolve(resolver());
      delete _resolving[tag];
      await tok(state => {
        const slot = (state._once = state._once || {});
        slot[tag] = [val];
      });
      return val;
    };

    return await (_resolving[tag] = _resolving[tag] || resolve());
  }
}

class StateFile extends StateStore {
  static async create(stateFile, fallback) {
    return super.create(new StateFileStore(stateFile), fallback);
  }
}

module.exports = { StateStore, StateFile };
