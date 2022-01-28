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

  makeToken(earlyMut, earlyFixup) {
    const { key, tokens } = this;

    const token = {
      earlyMut,
      earlyFixup,
      lateMut: null,
      lateFixup: null,
      complete: false
    };

    tokens.push(token);

    if (tokens.length > 10) {
      console.log(tokens.map(t => t.complete));
      throw new Error(`Lots of pending tokens for ${key}`);
    }

    // Return an async function which must be called in the
    // future to mark this token complete.
    return forkingToken(async (lateMut, lateFixup) => {
      if (token.complete)
        throw new Error(`Completion token called more than once`);

      token.complete = true;
      token.lateMut = lateMut;
      token.lateFixup = lateFixup;

      const todo = getComplete(tokens);
      if (!todo) return;

      let fixups;
      const pending = tokens.slice(0, todo);

      // Mutate function must be idempotent. The current implementation
      // only invokes it once but e.g. a CouchDB based version might
      // want to call the mutator more than once (optimistic locking)
      await this.ss.mutate(state => {
        fixups = []; // clear side effects
        // Ensure our key exists
        let slot = _.get(state, key, {});
        for (const { earlyMut, lateMut, earlyFixup, lateFixup } of pending) {
          slot = applyMutators(slot, earlyMut, lateMut);
          // Remember any cleanups for after the commit.
          fixups.push(earlyFixup, lateFixup);
        }
        if (state[key] !== slot)
          state = produce(state, state => {
            _.set(state, key, slot);
          });

        return state;
      });

      tokens.splice(0, todo);

      const work = _.flattenDeep(fixups).filter(Boolean);
      // Fixups in order.
      for (const fix of work) await Promise.resolve(fix());
    });
  }
}

module.exports = StateSection;
