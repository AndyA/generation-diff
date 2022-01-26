const { produce } = require("immer");
const _ = require("lodash");
const Promise = require("bluebird");
const forkingToken = require("./forking-token");

const getComplete = tokens => {
  const todo = tokens.findIndex(t => !t.complete);
  return todo < 0 ? tokens.length : todo;
};

const applyMutators = (state, ...mutators) =>
  _.flattenDeep(mutators)
    .filter(Boolean)
    .reduce((st, mut) => produce(st, mut), state);

class StateSection {
  constructor(ss, key) {
    this.ss = ss;
    this.key = key;
    this.tokens = [];
  }

  get current() {
    return _.get(this.ss.current, this.key, {});
  }

  makeToken(mutator, cleanup) {
    const { key, tokens } = this;

    const token = { mutator, fixup: [], complete: false, cleanup };
    tokens.push(token);

    // Return an async function which must be called in the
    // future to mark this token complete.
    return forkingToken(async fixup => {
      if (token.complete)
        throw new Error(`Completion token called more than once`);

      token.complete = true;
      token.fixup = fixup;

      const todo = getComplete(tokens);
      if (!todo) return;

      let cleanups;
      const pending = tokens.slice(0, todo);

      // Mutate function must be idempotent. The current implementation
      // only invokes it once but e.g. a CouchDB based version might
      // want to call the mutator more than once (optimistic locking)
      await this.ss.mutate(state => {
        cleanups = []; // clear side effects
        // Ensure our key exists
        let slot = _.get(state, key, {});
        for (const { mutator, fixup, cleanup } of pending) {
          slot = applyMutators(slot, mutator, fixup);
          // Remember any cleanups for after the commit.
          if (cleanup) cleanups.push(cleanup);
        }
        if (state[key] !== slot)
          state = produce(state, state => {
            _.set(state, key, slot);
          });

        return state;
      });

      tokens.splice(0, todo);

      // Cleanups in order.
      for (const cu of cleanups) await Promise.resolve(cu());
    });
  }
}

module.exports = StateSection;
