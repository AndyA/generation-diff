const fs = require("fs");
const path = require("path");
const lockfile = require("proper-lockfile");

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

module.exports = StateFileStore;
