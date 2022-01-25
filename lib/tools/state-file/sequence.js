const { produce } = require("immer");
const _ = require("lodash");
const Promise = require("bluebird");
const forkingToken = require("./forking-token");
const StateFileStore = require("./file-store");

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

    const applyMutators = (state, mutators) => {
      return _.castArray(mutators || [])
        .filter(Boolean)
        .reduce((st, mut) => produce(st, mut), state);
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

      // Mutate function must be idempotent. The current implementation
      // only invokes it once but e.g. a CouchDB based version might
      // want to call the mutator more than once.
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

module.exports = StateSequence;
