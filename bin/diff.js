const { ObjectBrigadeStore } = require("../lib/tools/object-brigade");

class ReadState {
  constructor(reader) {
    this.reader = reader;
    this.current = null;
  }

  async next() {
    if (this.current) await this.current.token();
    return (this.current = await this.reader.read());
  }
}

const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

async function main(prevDB, nextDB) {
  const [prev, next] = await Promise.all(
    [prevDB, nextDB].map(async (db, i) => {
      const rs = new ReadState(
        await new ObjectBrigadeStore(db).getReader(`in${i}`)
      );
      await rs.next();
      return rs;
    })
  );

  console.log({ prev, next });

  while (prev.current || next.current) {
    const diff = cmp(prev.current.cat_pk, next.current.cat_pk);
    if (diff < 0) {
      console.log(`DELETE ${prev.current.cat_pk}`);
      await prev.next();
    } else if (diff > 0) {
      console.log(`ADD ${next.current.cat_pk}`);
      await next.next();
    } else {
      await Promise.all([prev.next(), next.next()]);
    }
  }
}

main("tmp/pips.prev", "tmp/pips.next").catch(e => {
  console.error(e);
  process.exit(1);
});
