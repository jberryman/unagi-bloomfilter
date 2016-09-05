# unagi-bloomfilter [![Build Status](https://travis-ci.org/jberryman/unagi-bloomfilter.svg)](https://travis-ci.org/jberryman/unagi-bloomfilter)

This library implements a fast concurrent bloom filter, based on bloom-1 from
"Fast Bloom Filters and Their Generalization" by Y Qiao, et al. 

It's [on hackage](https://hackage.haskell.org/package/unagi-bloomfilter) and
can be installed with 

    $ cabal install unagi-bloomfilter

A bloom filter is a probabilistic, constant-space, set-like data structure
supporting insertion and membership queries. This implementation is backed by
SipHash so can safely consume untrusted inputs.

The implementation here compares favorably with traditional set implementations
in a single-threaded context, e.g. here are 10 inserts or lookups compared
across some sets of different sizes:

![single-threaded](http://i.imgur.com/gei1LW4.png)

With the llvm backend benchmarks take around 75-85% of the runtime of the
native code gen.

Unfortunately writes in particular don't seem to scale currently; i.e.
distributing writes across multiple threads may be _slower_ than in a
single-threaded context, because of memory effects. We plan to export
functionality that would support using the filter here in a concurrent context
with better memory behavior (e.g. a server that shards to a thread-pool which
handles only a portion of the bloom array).

![concurrent](http://i.imgur.com/RaUSmZB.png)

