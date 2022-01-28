const fs = require("fs");
const lockfile = require("proper-lockfile");
const StateStore = require("./store");
const writeFileAtomic = require("write-file-atomic");

class StateFileStore {
  constructor(file) {
    this.file = file;
  }

  async save(state) {
    const { file } = this;
    await writeFileAtomic(file, JSON.stringify(state));
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

class StateFile extends StateStore {
  static async create(stateFile, fallback) {
    return super.create(new StateFileStore(stateFile), fallback);
  }
}

module.exports = StateFile;
