"use strict";

const _ = require("lodash");
const assert = require("assert");

function countedCompletion(cb) {
  if (cb.fork) return cb.fork();

  let refCount = 0;

  cb.fork = function () {
    let called = false;
    refCount++;

    function ncb() {
      const al = arguments.length;
      if (al)
        throw new Error(
          `Callback called with ${al} argument${
            al === 1 ? "" : "s"
          }. Should have none`
        );
      if (called) throw new Error("Callback called more than once");
      called = true;
      refCount--;
      assert(refCount >= 0);
      if (refCount === 0) cb();
    }

    ncb.fork = cb.fork;
    return ncb;
  };

  return cb.fork();
}

module.exports = countedCompletion;
