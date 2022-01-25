const StateFileStore = require("./file-store");
const StateStore = require("./store");

class StateFile extends StateStore {
  static async create(stateFile, fallback) {
    return super.create(new StateFileStore(stateFile), fallback);
  }
}

module.exports = { StateStore, StateFile };
