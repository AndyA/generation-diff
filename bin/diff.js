const { ObjectBrigadeStore } = require("../lib/tools/object-brigade");

class ReadState {
  constructor(reader) {
    this.reader = reader;
    this.current = null;
  }

  async next() {
    await this.end();
    return (this.current = await this.reader.read());
  }

  async end() {
    if (this.current) await this.current.token();
  }
}

const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

const makeCompare = cf => (a, b) =>
  a === null ? (b === null ? 0 : 1) : b === null ? -1 : cf(a, b);

async function main(prevDB, nextDB) {
  const [prev, next] = await Promise.all(
    [prevDB, nextDB].map(async ({ dir, ...opt }, i) => {
      const rs = new ReadState(
        await new ObjectBrigadeStore(dir, opt).getReader(`in${i}`)
      );
      await rs.reader.seek(0);
      await rs.next();
      return rs;
    })
  );

  const compare = makeCompare(cmp);

  while (prev.current || next.current) {
    const pid = prev.current?.object.id ?? null;
    const nid = next.current?.object.id ?? null;

    const diff = compare(pid, nid);

    if (diff < 0) {
      console.log(`DELETE ${pid}`);
      await prev.next();
    } else if (diff > 0) {
      console.log(`ADD ${nid}`);
      await next.next();
    } else {
      if (prev.current.object.ETag !== next.current.object.ETag)
        console.log(`UPDATE ${nid}`);
      await Promise.all([prev.next(), next.next()]);
    }
  }

  await prev.end();
  await next.end();
}

function test() {
  const [prev, next] = [
    ["@", "A", "B", "D", "E"],
    ["A", "C", "D", "F", "G"]
  ]
    .sort()
    .map(ids => ids.map(id => ({ id })));

  const compare = makeCompare(cmp);
  while (prev.length || next.length) {
    const pid = prev.length ? prev[0].id : null;
    const nid = next.length ? next[0].id : null;

    const diff = compare(pid, nid);
    if (diff < 0) {
      console.log(`DELETE ${pid}`);
      prev.shift();
    } else if (diff > 0) {
      console.log(`ADD ${nid}`);
      next.shift();
    } else {
      prev.shift();
      next.shift();
    }
  }
}

const verbs = { test };

const args = process.argv.slice(2);
if (args.length) {
  const [verb, ...rest] = args;
  const action = verbs[verb.toLowerCase()];
  if (!action) throw new Error(`Can't ${verb}`);
  action();
} else {
  const [prev, next] = ["prev", "next"].map(set => ({
    dir: `db/pips.${set}`,
    dataDir: `/Volumes/db/pips.${set}`,
    strictState: false
  }));
  main(prev, next).catch(e => {
    console.error(e);
    process.exit(1);
  });
}
