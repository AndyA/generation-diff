const _ = require("lodash");
const Promise = require("bluebird");
const StateSequence = require("./sequence");

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

module.exports = StateStore;
