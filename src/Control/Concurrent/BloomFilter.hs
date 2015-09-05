{-# LANGUAGE BangPatterns #-}
module Control.Concurrent.BloomFilter 
    where
    -- new
    -- insert, returning success/failure
    -- query
    -- false positive rate for a particular size
    -- required size for desired FPR
    --
-- Operations:
--   - serialize/deserialize
--   - insert (with Bool return i.e. does lookup)
--   - lookup
--   - lossless union of two bloom filters (Monoid?)
--   - lossy intersection of two bloom filters (FPR becomes at most the FPR of one of constituents, but may be more than if single were created from scratch)
--   - freeze/thaw
--     - maybe freeze should return exposed constructor type (exposing immutable array)
--   - approximating number of items, and size of union and intersection
--   -

import Data.Bits
import Data.Primitive.ByteArray
import Control.Monad.Primitive(RealWorld)
import Control.Exception(assert)

-- TODO
--   Maybe we should assume bloom parameters will be static
--   then we can tag bloom filter with type-level numeric params
--   - that way we can serialize/deserialize and combine (and have Monoid instance)
--   - we can also MAYBE make sure at compile time that 128 bits will be enough for the chosen parameters will


{-
A Bloom-1 filter is an array B1 of l words, each of which
is w bits long. The total number m of bits is l x w. To encode
a member e during the filter setup, we first obtain a number
of hash bits from e, and use log2 l hash bits to map e to a
word in B1. It is called the membership word of e in the
Bloom-1 filter. We then use k log2 w hash bits to further map
e to k membership bits in the word and set them to ones.
The total number of hash bits that are needed is
log2 l þ k log2 w. Suppose m = 2^20, k = 3, w = 2^6, and
l = 2^14. Only 32 hash bits are needed, smaller than the
60 hash bits required in the previous Bloom filter example
under similar parameters.
To check if an element e0 is a member in the set that is
encoded in a Bloom-1 filter, we first perform hash
operations on e0 to obtain log2 l þ k log2 w hash bits. We
use log2 l bits to locate its membership word in B1, and then
use k log2 w bits to identify the membership bits in the word.
If all membership bits are ones, it is considered to be a
member. Otherwise, it is not.
-}

{-
Hash bits "enough for anyone" value?

32 GB bloom filter = m = 256,000,000,000
                     l =   4,000,000,000
                     w =              64

BitsReqd = log2 l + k log2 w

So with k = 3:
    50 hash bits

And assuming we only have 128 hash bits, we can have max:
    64 GB bloom, k = 15-16 max

...which gives us an FPR for different loads of:
    
 -}

{-
 A NOTE ON TESTING FPR:

consideration when implementing Bloom-1 filters. To
ensure that the results obtained using Eq. (5) are accurate,
a Bloom-1 filter was implemented in Matlab using ideal
hash functions and simulated for the same parameters. In
each simulation, 10,000 Fast Bloom filters are generated
by inserting random elements until the specified load is
achieved. Then their false positive rate is evaluated doing
106 random queries of non member elements. The average
results were checked against those obtained with Eq. (5).
This was done for false positive rates larger than 10-6
. In all cases, differences were smaller than 0.5%.
-}

-- find membership word
-- locate k bits within word
-- optimal k = 
--
--   where m = number bits in bit array
--         n = number members in set
--
--

-- FOR TESTING: if a large number of search operations for random elements not
-- stored in the Bloom filter are done, the average of the results should match
-- the value provided by the following

-- | The false positive rate for a bloom-1 filter.
-- TODO only use either m or l, both here and in 'new'
-- TODO actually since this is so expensive, treat as a utility and pass in `w` as well.

{-
-- This is the corrected equation from 'Supplementary File: A Comment on “Fast
-- Bloom Filters and Their Generalization”'. Unfortunately since we're going to need to approximate factorial even to calculate this.
-- compute this without moving into log space and probably approximating the
-- factorials which might defeat the purpose of using this more accurate
-- function to begin with.
fpr :: Int  -- ^ @n@: Number of elements in filter
    -> Int  -- ^ @l@: Size of filter, in machine words
    -> Int  -- ^ @k@: Number of bits to map an element to
    -> Float
fpr nI lI kI =
  let w = 64 -- TODO word-size in bits
      n = fromIntegral nI
      l = fromIntegral lI
      k = fromIntegral kI
   in summation 0 n $ \x->
        (combination n x) *
        ((1/l) ** x) *
        ((1 - (1/l)) ** (n-x)) *
        (factorial w / (w ** k*(x+1)) ) *
        (summation 1 w $ \i->
            (summation 1 i $ \j->
                ((j ** k*x) * i**k) /
                (factorial (w-i) * factorial j * factorial (i-j))
            )
        )
-}
{- Naive:
fpr :: Int  -- ^ @n@: Number of elements in filter
    -> Int  -- ^ @l@: Size of filter, in machine words (vs @m@ which is size in bits)
    -> Int  -- ^ @k@: Number of bits to map an element to
    -> Float
fpr nI lI kI =
  let w = 64 -- TODO word-size in bits
      n = fromIntegral nI
      l = fromIntegral lI
      k = fromIntegral kI
   in summation 0 n $ \x->
        (combination n x) *
        ((1/l) ** x) *
        ((1 - (1/l)) ** (n-x)) *
        ((1 - ((1 - 1/w) ** (x*k))) ** k)
-}

-- This is my attempt at translating the FPR equation from "Fast Bloom Filters
-- and their Generalizations" with the following modifications:
--   - calculations within summation performed in log space
--   - use Stirling's approximation of factorial for larger `n`
--   - scale `n` and `l` together for large `n`; the paper graphs FPR against
--      this ratio so I guess this is justified, and it seems to work.

-- | An estimate of the false-positive rate for a bloom-1 filter. For a filter
-- with the provided parameters and having @n@ elements, the value returned
-- here is the precentage, over the course of many queries for elements /not/ in
-- the filter, of those queries which will return an incorrect @True@ result.
--
-- This function is slow but the complexity is bounded and can accept inputs of
-- any size.
fpr :: Int  -- ^ @n@: Number of elements in filter
    -> Int  -- ^ @l@: Size of filter, in machine words
    -> Int  -- ^ @k@: Number of bits to map an element to
    -> Int  -- ^ @w@: word-size in bits (e.g. 32 or 64)
    -> Double
fpr nI lI kI wI =
    summation 0 n $ \x->
      e ** (
        (logCombination n x) +
        (x * (negate $ log l)) +
        ((n-x) * (log(l-1) - log l)) +
        (k* log(1 - ((1 - (1/w)) ** (x*k))))
        )

  where
    n = min 32000 (fromIntegral nI)
    l = fromIntegral lI * (n/fromIntegral nI)

    k = fromIntegral kI ; w = fromIntegral wI
    e = exp 1
    --     / x \
    -- log \ y /
    logCombination x y
        | y <= x    = logFactorial x - (logFactorial y + logFactorial (x - y))
        | otherwise = log 0 -- TODO okay?

    logFactorial x
        | x <= 100  = sum $ map log [1..x]
         -- else use Stirling's approximation with error < 0.89%:
        | otherwise = x * log x - x

    summation l h = sum . \f-> map f [l..h]







newtype BloomFilter a = BloomFilter (MutableByteArray RealWorld)

-- | Create a new bloom filter of elements of type @a@, with the given @m@ and
-- @k@ parameters. See TODO for calculating those parameters.
--
-- The requested @m@ will be rounded up to the nearest power of two of the
-- machine word size (32 or 64); for instance ... TODO
--
-- For @k@ we take the 'min' of the requested @k@ and the machine word size.
new :: Int -- ^ @m@: the number of bits in the array
    -> Int -- ^ @k@: the number of membership bits per element
    -> IO (BloomFilter a) -- TODO maybe return actual m and k?
new = undefined

--
-- Questions:
--  - must we really do 'k' hashes, or is there a faster way to do that?
--  - what about when those hashes collide? Do we actually have to get 'k' bits set?
--
-- Next release:
--  - better/smarter/faster hashing
--    - might just pin to 128-bit hash ("should be enough for anyone")
--  - offer newtype phantom wrapper around a heterogeneous collection?
--    - but ONLY if we have some guarantee that hashing different types don't clash
--  - make whole thing serializable (so need consistent hashing)
--    - NOTE!: we need to make sure that hashing is cross-platform
--      AAANND that we use the *same* hashing library. Easiest way
--      might be to fix the version of hashing lib, AND store a version number
--      in serialized form.
--  - function for OR combining bloom filters (perhaps basis for distributed setup)
--  - offer a function (or table) for calculating optimal k
--      - offer guidance on how to use it
--  - benchmarks and performance tuning
--  - can we make it durable/persistent and consistent via some easy mechanism?
--      (how do atomic operations and mmap relate?)


-- Not particularly fast; if needs moar fast see
--   http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
-- 
nextHighestPowerOfTwo :: Int -> Int
nextHighestPowerOfTwo 0 = 1
nextHighestPowerOfTwo n 
    | n > maxPowerOfTwo = error $ "The next power of two greater than "++(show n)++" exceeds the highest value representable by Int."
    | otherwise = 
        let !nhp2 = 2 ^ (ceiling (logBase 2 $ fromIntegral $ abs n :: Float) :: Int)
         -- ensure return value is actually a positive power of 2:
         in assert (nhp2 > 0 && popCount (fromIntegral nhp2 :: Word) == 1)
              nhp2

  where maxPowerOfTwo = (floor $ sqrt $ (fromIntegral (maxBound :: Int)::Float)) ^ (2::Int)
