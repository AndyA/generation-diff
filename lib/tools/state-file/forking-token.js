const assert = require("assert");

module.exports = tok => {
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
};
