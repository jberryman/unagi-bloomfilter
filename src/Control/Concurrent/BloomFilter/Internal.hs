{-# LANGUAGE BangPatterns, RecordWildCards, CPP #-}
module Control.Concurrent.BloomFilter.Internal (
      new
    , BloomFilter(..)
    , BloomFilterParamException(..)
    , insert
    , lookup
    , SipKey(..)
    , fpr

# ifdef EXPORT_INTERNALS
  -- * Internal functions exposed for testing; you shouldn't see these
    , membershipWordAndBits64, membershipWordAndBits128
    , maskLog2wRightmostBits
    , log2w
    , wordSizeInBits
    , uncheckedSetBit
    , isHash64Enough
    , assertionCanary
# endif
    )
    where


import Data.Bits hiding (unsafeShiftL, unsafeShiftR)
import qualified Data.Bits as BitsHidden
import qualified Data.Primitive.ByteArray as P
import Data.Primitive.MachDeps
import Control.Monad.Primitive(RealWorld)
import Data.Atomics
import Data.Hashabler
import Control.Exception
import Data.Typeable(Typeable)
import Control.Monad
import Data.Word(Word64)
import Prelude hiding (lookup)

-- Operations:
--   - serialize/deserialize
--     - don't store the secret key, but perhaps store a hash of the key for
--       verification?
--   - insert (with Bool return i.e. does lookup)
--   - lookup (or "member" or "elem")
--   - lossless union of two bloom filters (Monoid?)
--     - probably just implement for pure/frozen filters for now (doing an atomic op on every word of a filter is probably bad new bears).
--     - note also we can union any two bloom filters (as long as they're power of two size);
--       we simply fold the larger in on itself until it matches the smaller
--       SO DO WE CARE ABOUT HAVING THE LENGTH AT THE TYPE LEVEL?
--       (MAYBE: we still want that for `new` to ensure sufficient hash bits
--               But we only need a typelevel nat arg right? Not a tag.
--               The downside is just that we can't have a type-checked union,
--               which is probably not a big deal.
--               BUT: is this a proper Monoid when it's "lossy"?
--       (After thought): this perhaps can only easily be a semi-group, since we can't insert into empty
--                        so implement an operation `union` which we can make Semigroup.<> when it lands in base. (then deprecate `union` where we can offer a Semigroup instance)
--
--   - lossy intersection of two bloom filters (FPR becomes at most the FPR of one of constituents, but may be more than if single were created from scratch)
--     - again this doesn't require type-level length tag; we can union fold one filter down to match the other.
--   - freeze/thaw
--     - maybe freeze should return exposed constructor type (exposing immutable array)
--   - approximating number of items, and size of union and intersection
--   - bulk reads and writes, for performance: (especially good for pure interface fromList, etc.
--     - re-order hashes for in-order cache line access (benchmark this)
--     - consider prefetching
--     - combine inter-word reads and writes.
--
-- Things that can happen later:
--   - freezing/pure interface
--   - type-level embedding; too many downsides:
--     - need to encode key there too in order to be effective
--     - uselsess when deserializing
--
-- Typing revisited:
--   - we could do type-level nat thing for `new` and not parameterize.
--     - with a little more work we could also allow user to pass in their own hash function.
--   - when deserializing we need to also get the type; we can't just serialize Typeable though! Maybe need static pointers?
--
-- - We can handle serializing/deserializing by simply exposing internals (I guess)
--
-- Observation:
--  - the user could map different types to different hash functions herself
--  - in that sense the bloom filter itself only cares about hash values as inputs.
--
-- Idea:
--  - hashabler should provide a method for a unique serializable and stable Typeable?
--  - this should be a UUID since it's going to need to be unique across all instances
--  - this should change when the data definition changes (same times we'd expect hash values to change?)
--  - should we consider this the "hash" of the type?
--  - Consider:
--     when we're serializing the type representation, it only makes sense to
--     serialize with respect to data (because a type can keep the same name
--     and transform over different versions), or to be more precise: we
--     serialize with respect to some aspect of the data that we care about.
--     (how to clarify this...?)
--  - name this `HashDomain`, with a constructor `Unique` for non-serializable
--  - provide the utility function for generating.
--
-- Randome notes:
--  - we could fix K to 3
--
--  deserializing (without type-level Natural markers):
--    - provide key (compared with a stored hash, maybe)
--    - hashDomain compared to stored
--    - k and m are internal, taken from deserialized
--
--  combining, with total type safety:
--    - we can always check equality of value level nats by doing natVal when deserializing
--    - static length MIGHT be problem
--      - ?
--    - static SipKey IS a problem
--      - ?
--

-- TODO
--   Maybe we should assume bloom parameters will be static
--   then we can tag bloom filter with type-level numeric params
--   - that way we can serialize/deserialize and combine (and have Monoid instance)
--   - we can also MAYBE make sure at compile time that 128 bits will be enough for the chosen parameters
--     -we could even provide a constraint `Faster w k l` that makes sure it's under 64 bits.
{- TYPED 'new':
 -   - parameterize by: k, a, key
 - OR:
 -   - limit typed interface to only k = 3?
 -   - keep 'key' in type? Awkward!
 -     - maybe we can enforce only a single "implicit" 'key' value for entire
 -        program using those singleton instance thingies? Then keep this out
 -        of the type.
 -   - constrain log2l <=64, and log2l+ k*log2w <=128 (or 64 for "fast" variant)
 -}
-- (OLDER NOTES):
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




-- | A mutable bloom filter representing a set of 'Hashable' values of type @a@.
--
-- A bloom filter is a set-like probabilistic hash-based data structure.
-- Elements can be 'insert'-ed and 'lookup'-ed again. The bloom filter
-- takes a constant amount of memory regardless of the size and number of
-- elements inserted, however as the bloom filter \"grows\" the likelihood of
-- false-positives being returned by 'lookup' increases. 'fpr' can be used to
-- estimate the expected false-positive rate.
data BloomFilter a = BloomFilter { key :: !SipKey
                                 , k :: !Int
                                 , hash64Enough :: Bool
                                 -- ^ if we need no more than 64-bits we can use the faster 'siphash64'
                                 , l_minus1 :: !Int
                                 -- ^ For fast modulo
                                 , log2l :: !Int
                                 , arr :: !(P.MutableByteArray RealWorld)
                                 }



-- | Exceptions raised because of invalid parameters passed to 'new' or other
-- invalid internal states from making about with BloomFilter internals. These
-- should never be raised when using our Typed interface.
newtype BloomFilterParamException = BloomFilterParamException String
    deriving (Show, Typeable)

instance Exception BloomFilterParamException


-- | Create a new bloom filter of elements of type @a@ with the given hash key
-- and parameters. 'fpr' can be useful for calculating the @k@ parameter, or
-- determining a good filter size.
--
-- The parameters must satisfy the following conditions, otherwise a
-- 'BloomFilterParamException' will be thrown:
--
--   - @k > 0@
--   - @log2l >= 0 && log2l <= wordSizeInBits@
--   - @log2l + k*(logBase 2 wordSizeInBits) <= 128@
--
-- In addition, performance on 64-bit machines will be best when
-- @log2l + k*(logBase 2 wordSizeInBits) <= 128@ where we require only 64 hash
-- bits for each element. (Performance on 32-bit machines will be worse in all
-- cases, as we're doing 64-bit arithmetic.)
--
-- Example: on a 32-bit machine, the following produces a ~4KB bloom filter of
-- @2^10@ 32-bit words, using 3 bits per element:
--
-- @
--  do key <- read <$> getEnv "THE_SECRET_KEY"
--     Bloom.new key 3 10
-- @
new :: SipKey
    -- ^ The secret key to be used for hashing values for this bloom filter.
    -> Int
    -- ^ @k@: Number of independent bits of @w@ to which we map an element. 3 is a good choice.
    -> Int
    -- ^ @log2l@: The size of the filter, in machine words, as a power of 2.
    -> IO (BloomFilter a)
new key k log2l = do
    -- In typed interface all of these conditions hold:
    let !hash64Enough = isHash64Enough log2l k
    unless (k > 0) $
      throwIO $ BloomFilterParamException "in 'new', k must be > 0"
    unless (log2l >= 0) $
      throwIO $ BloomFilterParamException "in 'new', log2l must be >= 0"

    let !sizeBytes = sIZEOF_INT `uncheckedShiftL` log2l
    arr <- P.newAlignedPinnedByteArray sizeBytes aLIGNMENT_INT
    P.fillByteArray arr 0 sizeBytes (0x00)

    return $ BloomFilter { l_minus1 = (2^log2l)-1, .. }


membershipWordAndBits64 :: Hash64 a -> BloomFilter a -> (Int, Int)
{-# INLINE membershipWordAndBits64 #-}
membershipWordAndBits64 !(Hash64 h) = \ !(BloomFilter{ .. }) ->
  assert (isHash64Enough log2l k) $
    -- Use leftmost bits for membership word, and take member bits from right.
    -- N.b. shiftR for possible 1-word filter when log2l = 0:
    let !memberWord = fromIntegral (h `shiftR` (64-log2l))
        !wordToOr = setKMemberBits 0x00 k h

     in (memberWord, wordToOr)

membershipWordAndBits128 :: Hash128 a -> BloomFilter a -> (Int, Int)
{-# INLINE membershipWordAndBits128 #-}
membershipWordAndBits128 (Hash128 h_0 h_1) = \(BloomFilter{ .. }) ->
  assert (not $ isHash64Enough log2l k) $
    -- Use leftmost bits for membership word, and take member bits from right,
    -- then taking remaining member bits from h_1 (from the right)
    let !bitsLeftForMemberBits_h_0 = 64-log2l
        -- N.b. shiftR for possible 1-word filter when log2l = 0:
        !memberWord = fromIntegral (h_0 `shiftR` bitsLeftForMemberBits_h_0)
        (!kFor_h_0, !remainingBits_h_0) = bitsLeftForMemberBits_h_0 `quotRem` log2w
        !bitsForLastMemberBit_h_1 = assert (remainingBits_h_0 < log2w) $ -- for uncheckedShiftR
            log2w - remainingBits_h_0
        -- Leaving one which we'll always build with (possibly 0)
        -- remainingBits_h_0 and the leftmost bits from h_1:
        !kFor_h_1 = (k - kFor_h_0) - 1


        -- Fold in ks from h_0 (from right):
        !wordToOrPart0 = setKMemberBits 0x00 kFor_h_0 h_0
        -- Fold in ks from h_1 (from right):
        !wordToOrPart1 = setKMemberBits 0x00 kFor_h_1 h_1
        -- Build the final member bit with possible leftover bits from h_0
        !lastMemberBitPart0 =
            -- clear left of range:
            ((h_0 `uncheckedShiftL` log2l)
                -- clear right of range:
                  `shiftR` (64 - remainingBits_h_0)) -- n.b. possible 64 shift
                -- align for combining with lastMemberBitPart1:
                  `uncheckedShiftL` bitsForLastMemberBit_h_1
        -- ...and leftmost bits from h_1:
        !lastMemberBitPart1 = h_1 `uncheckedShiftR` (64-bitsForLastMemberBit_h_1)
        !wordToOr = (wordToOrPart0.|.wordToOrPart1) `uncheckedSetBit`
                       fromIntegral (lastMemberBitPart0.|.lastMemberBitPart1)

     in assert (kFor_h_1 >= 0 && -- we may only need a few to make up remainder.
               (kFor_h_0*log2w) <= 64  &&
               (kFor_h_1*log2w) <= 64  &&
               (kFor_h_0 + kFor_h_1 + 1) == k  &&
               (kFor_h_1*log2w + remainingBits_h_0 + kFor_h_1*log2w) <= 128) $
          (memberWord, wordToOr)

setKMemberBits :: Int -> Int -> Word64 -> Int
{-# INLINE setKMemberBits #-}
setKMemberBits !wd 0 _ = wd
setKMemberBits !wd !k' !h' =
    -- possible cast to 32-bit Int but we only need rightmost 5 or 6:
  let !memberBit = fromIntegral h' .&. maskLog2wRightmostBits
   in setKMemberBits (wd `uncheckedSetBit` memberBit) (k'-1) (h' `uncheckedShiftR` log2w)



membershipWordAndBitsFor :: (Hashable a)=> BloomFilter a -> a -> (Int, Int)
{-# INLINE membershipWordAndBitsFor #-}
membershipWordAndBitsFor bloom@(BloomFilter{..}) a
    | hash64Enough = membershipWordAndBits64  (siphash64  key a) bloom
    | otherwise    = membershipWordAndBits128 (siphash128 key a) bloom


-- True if we can get enough hash bits from a Word64, and a runtime check
-- sanity check of our arguments to 'new'. This is probably in "enough for
-- anyone" territory currently:
isHash64Enough :: Int -> Int -> Bool
{-# INLINE isHash64Enough #-}
isHash64Enough log2l k =
    let bitsReqd = log2l + k*log2w
     in if bitsReqd > 128
          then throw $ BloomFilterParamException "The passed parameters require over the maximum of 128 hash bits supported. Make sure: (log2l + k*(logBase 2 wordSizeInBits)) <= 128"
          else if (log2l > wordSizeInBits)
                 then throw $ BloomFilterParamException "You asked for (log2l > 64). We have no way to address memory in that range, and anyway that's way too big."
                 else bitsReqd <= 64

maskLog2wRightmostBits :: Int -- 2^log2w - 1
maskLog2wRightmostBits | sIZEOF_INT == 8 = 63
                       | otherwise       = 31

wordSizeInBits :: Int
wordSizeInBits = sIZEOF_INT * 8

log2w :: Int -- logBase 2 wordSizeInBits
log2w | sIZEOF_INT == 8 = 6
      | otherwise       = 5

uncheckedSetBit :: Int -> Int -> Int
{-# INLINE uncheckedSetBit #-}
uncheckedSetBit x i = x .|. (1 `uncheckedShiftL` i)

uncheckedShiftR :: (Num a, FiniteBits a, Ord a) => a -> Int -> a
{-# INLINE uncheckedShiftR #-}
uncheckedShiftR a = \x->
  assert (a >= 0) $
  assert (x < finiteBitSize a) $
  assert (x >= 0) $
    a `BitsHidden.unsafeShiftR` x
uncheckedShiftL :: (Num a, FiniteBits a, Ord a) => a -> Int -> a
{-# INLINE uncheckedShiftL #-}
uncheckedShiftL a = \x->
  assert (a >= 0) $
  assert (x < finiteBitSize a) $
  assert (x >= 0) $
    a `BitsHidden.unsafeShiftL` x


-- | Atomically insert a new element into the bloom filter.
--
-- This returns 'True' if the element /did not exist/ before the insert, and
-- 'False' if the element did already exist (subject to false-positives; see
-- 'lookup'). Note that this is reversed from @lookup@.
--
-- This operation is /O(size_of_element)/.
insert :: Hashable a=> BloomFilter a -> a -> IO Bool
{-# INLINE insert #-}
insert bloom@(BloomFilter{..}) = \a-> do
    let (!memberWord, !wordToOr) = membershipWordAndBitsFor bloom a
    oldWord <- fetchOrIntArray arr memberWord wordToOr
    return $! (oldWord .|. wordToOr) /= oldWord

-- | Look up the value in the bloom filter, returning 'True' if the element is
-- possibly in the set, and 'False' if the element is /certainly not/ in the
-- set.
--
-- The likelihood that this returns 'True' on an element that was not
-- previously 'insert'-ed depends on the parameters the filter was created
-- with, and the number of elements already inserted. The 'fpr' function can
-- help you estimate this.
--
-- This operation is /O(size_of_element)/.
lookup :: Hashable a=> BloomFilter a -> a -> IO Bool
{-# INLINE lookup #-}
lookup bloom@(BloomFilter{..}) = \a-> do
    let (!memberWord, !wordToOr) = membershipWordAndBitsFor bloom a
    existingWord <- P.readByteArray arr memberWord
    return $! (existingWord .|. wordToOr) == existingWord






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

-- This is my attempt at translating the FPR equation from "Fast Bloom Filters
-- and their Generalizations" with the following modifications:
--   - calculations within summation performed in log space
--   - use Stirling's approximation of factorial for larger `n`
--   - scale `n` and `l` together for large `n`; the paper graphs FPR against
--      this ratio so I guess this is justified, and it seems to work.

-- | An estimate of the false-positive rate for a bloom-1 filter. For a filter
-- with the provided parameters and having @n@ elements, the value returned
-- here is the precentage, over the course of many queries for elements /not/ in
-- the filter, of those queries which would be expected to return an incorrect
-- @True@ result.
--
-- Note this is an approximation of a flawed equation (and may also have been
-- implemented incorrectly). It seems to be reasonably accurate though, except
-- when the calculated fpr exceeds 70% or so. See the test suite.
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

    k = fromIntegral kI
    w = fromIntegral wI
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

    summation low hi = sum . \f-> map f [low..hi]



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


# ifdef EXPORT_INTERNALS
-- This could go anywhere, and lets us ensure that assertions are turned on
-- when running test suite.
assertionCanary :: IO Bool
assertionCanary = do
    assertionsWorking <- try $ assert False $ return ()
    return $
      case assertionsWorking of
           Left (AssertionFailed _) -> True
           _                        -> False
# endif
