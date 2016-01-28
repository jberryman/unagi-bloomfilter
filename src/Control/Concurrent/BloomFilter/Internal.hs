{-# LANGUAGE BangPatterns, RecordWildCards, CPP #-}
module Control.Concurrent.BloomFilter.Internal (
-- TODO we might just move this to 'Internal' at the top level, since we'll export functions here for both mutable and immutable apis.
      new
    , BloomFilter(..)
    , BloomFilterParamException(..)
    , insert
    , lookup
    , unionInto
    , intersectionInto
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
--     - don't store the secret key, but 128 bit hash it with itself and store the first Word64
--     - store architecture and insist they match (since everything is relative to word size)
--     - store a version number:
--       - this should correspond to the major version number of a breaking change
--       - include golden tests with serialized values, ensuring they match
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
--      fromList implementation possibilities:
--        1 - allocate new
--          - unsafeInsert all into new (possibly prefetching next block)
--          - non-threadsafe union with previous
--        2 - hash and sort (as list or something)
--          - memcpy previous
--          - unsafeInsert in order into new
--        3 - memcpy previous
--          - unsafeInsert new, manually prefetching next.
--     - re-order hashes for in-order cache line access (benchmark this)
--     - consider prefetching
--     - combine inter-word reads and writes.
--   - consider a Scalable Bloom Filter -type approach (or variant):
--     - CAS of linked list of filters
--     - possible linearizability issues.
--     - other operations become tricker or not doable.
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
 -        program using NullaryTypeClasses? Then keep this out of the type.
 -       Relevant here: configurations problem
 -         "The conﬁgurations problem is to propagate run-time preferences
 -         throughout a program, allowing multiple concurrent conﬁguration sets
 -         to coexist safely under statically guaranteed separation..."
 -         TODO is this relevant for the other type-level params we imagine?
 -         TODO can the two be complimentary?: use a singleton class, but instantiate it dynamically with reflection? per:https://www.schoolofhaskell.com/user/thoughtpolice/using-reflection#dynamically-constructing-type-class-instances
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
                                 , l_minus1 :: !Word64
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
    -- ^ @log2l@: The size of the filter, in machine words, as a power of 2. e.g. @3@ means @2^3@ or @8@ machine words.
    -> IO (BloomFilter a)
new key k log2l = do
    -- In typed interface all of these conditions hold:
    let !hash64Enough = isHash64Enough log2l k
    unless (k > 0) $
      throwIO $ BloomFilterParamException "in 'new', k must be > 0"
    unless (log2l >= 0) $
      throwIO $ BloomFilterParamException "in 'new', log2l must be >= 0"

    (arr, sizeBytes) <- newBloomArr log2l
    P.fillByteArray arr 0 sizeBytes (0x00)

    return $ BloomFilter { l_minus1 = (2^log2l)-1, .. }

newBloomArr :: Int -> IO (P.MutableByteArray RealWorld, Int)
newBloomArr log2l = do
    let !sizeBytes = sIZEOF_INT `uncheckedShiftL` log2l
    -- aligned: we assume atomic reads (no word tearing):
    arr <- P.newAlignedPinnedByteArray sizeBytes aLIGNMENT_INT
    return (arr,sizeBytes)

membershipWordAndBits64 :: Hash64 a -> BloomFilter a -> (Int, Int)
{-# INLINE membershipWordAndBits64 #-}
membershipWordAndBits64 !(Hash64 h) = \ !(BloomFilter{ .. }) ->
  assert (isHash64Enough log2l k) $
    -- From right: take all member bits, then take membership word. NOTE: for
    -- union on different size filters to work we must not e.g. take the
    -- leftmost log2l bits; we need lower-order bits to be the same between the
    -- two filters.
    let !memberWord = fromIntegral $
           l_minus1 .&. (h `uncheckedShiftR` fromIntegral (k*log2w))
        !wordToOr = fst $ setKMemberBits 0x00 k h

     in (memberWord, wordToOr)

membershipWordAndBits128 :: Hash128 a -> BloomFilter a -> (Int, Int)
{-# INLINE membershipWordAndBits128 #-}
membershipWordAndBits128 (Hash128 h_0 h_1) = \(BloomFilter{ .. }) ->
  assert (not $ isHash64Enough log2l k) $
    -- Isolate member word by taking from lowest bits of h_0, take member bits
    -- starting from right of h_1, possibly using leftmost from h_0 which we
    -- splice onto the end as we shift and consume h_1:
    let !memberWord = fromIntegral $ l_minus1 .&. h_0
        !bitsReqd_h_0 = k*log2w - 64

        !wordToOr =
           if bitsReqd_h_0 <= 0
             then fst $ setKMemberBits 0x00 k h_1
             else -- we'll shift right just enough so we can OR with the last bit of h_1 shifted right:
               let !bitsReqd_h_0_withOffs = bitsReqd_h_0 + (64 `rem` log2w)
                   !h_0_alignedMasked =
                     -- clear right:
                     (h_0 `uncheckedShiftR` (64 - bitsReqd_h_0)) -- n.b. conditional guards shift
                       -- align at offset:
                       `uncheckedShiftL` (64 - bitsReqd_h_0_withOffs)
                   !initialKToTake = bitsReqd_h_0_withOffs `quot` log2w
                   (!wordToOrPart0, !h_1_shifted) = setKMemberBits 0x00 initialKToTake h_1

                in assert (initialKToTake > 0) $
                     fst $ setKMemberBits wordToOrPart0 (k-initialKToTake) (h_1_shifted.|.h_0_alignedMasked)

     in (memberWord, wordToOr)



setKMemberBits :: Int -> Int -> Word64 -> (Int, Word64)
{-# INLINE setKMemberBits #-}
setKMemberBits !wd 0 h' = (wd, h')
setKMemberBits !wd !k' !h' =
    -- possible cast to 32-bit Int but we only need rightmost 5 or 6 bits:
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
  assert (a >= 0) $ -- make sure we don't smear sign w/ a bad fromIntegral cast
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


-- | /O(size_of_element)/. Atomically insert a new element into the bloom
-- filter.
--
-- This returns 'True' if the element /did not exist/ before the insert, and
-- 'False' if the element did already exist (subject to false-positives; see
-- 'lookup'). Note that this is reversed from @lookup@.
insert :: Hashable a=> BloomFilter a -> a -> IO Bool
{-# INLINE insert #-}
insert bloom@(BloomFilter{..}) = \a-> do
    let (!memberWord, !wordToOr) = membershipWordAndBitsFor bloom a
    oldWord <- fetchOrIntArray arr memberWord wordToOr
    return $! (oldWord .|. wordToOr) /= oldWord

-- | /O(size_of_element)/. Look up the value in the bloom filter, returning
-- 'True' if the element is possibly in the set, and 'False' if the element is
-- /certainly not/ in the set.
--
-- The likelihood that this returns 'True' on an element that was not
-- previously 'insert'-ed depends on the parameters the filter was created
-- with, and the number of elements already inserted. The 'fpr' function can
-- help you estimate this.
lookup :: Hashable a=> BloomFilter a -> a -> IO Bool
{-# INLINE lookup #-}
lookup bloom@(BloomFilter{..}) = \a-> do
    let (!memberWord, !wordToOr) = membershipWordAndBitsFor bloom a
    existingWord <- P.readByteArray arr memberWord
    return $! (existingWord .|. wordToOr) == existingWord




-- | /O(l_src+l_target)/. Write all elements in the first bloom filter into the
-- second. This operation is lossless; ignoring writes to the source bloom
-- filter that happen during this operation (see below), the target bloom
-- filter will be identical to the filter produced had the elements been
-- inserted into the target originally.
--
-- The source and target must have been created with the same key and
-- @k@-value. In addition the target must not be larger (the @l@-value) than
-- the source, /and/ they must both [use 128/64 bit hashes TODO link to helper
-- function]. This throws a 'BloomFilterParamException' when those constraints
-- are not met.
--
-- This operation is not linearizable with respect to 'insert'-type operations;
-- elements being written to the source bloomfilter during this operation may
-- or may not make it into the target "at random".
unionInto :: BloomFilter a -- ^ Source, left unmodified.
          -> BloomFilter a -- ^ Target, receiving elements from source.
          -> IO ()
unionInto = combine fetchOrIntArray



-- | /O(l_src+l_target)/. Make @target@ the intersection of the source and
-- target sets.  This operation is "lossy" in that the false positive ratio of
-- target after the operation will be higher than if the elements forming the
-- intersection had been 'insert'-ed directly into target.
--
-- The constraints and comments re. linearizability in 'unionInto' also apply
-- here.
intersectionInto :: BloomFilter a -- ^ Source, left unmodified.
                 -> BloomFilter a -- ^ Target, receiving elements from source.
                 -> IO ()
intersectionInto = combine fetchAndIntArray


-- internal
combine :: (P.MutableByteArray RealWorld -> Int -> Int -> IO x)
        -> BloomFilter a -> BloomFilter a -> IO ()
{-# INLINE combine #-}
combine f = \src target -> do
    unless (key src == key target) $ throwIO $ BloomFilterParamException $
      "SipKey of the source BloomFilter does not match target"
    unless (k src == k target) $ throwIO $ BloomFilterParamException $
      "k of the source BloomFilter does not match target"
    unless (log2l src >= log2l target) $ throwIO $ BloomFilterParamException $
      "log2l of the source BloomFilter is smaller than the target"
    unless (hash64Enough src == hash64Enough target) $ throwIO $ BloomFilterParamException $
      "either the source or target BloomFilter requires 128 hash bits while the other requires 64"

    let target_l_minus1 = fromIntegral $ l_minus1 target
        src_l_minus1 = fromIntegral $ l_minus1 src

    -- unless source and target are the same size we must "shrink" source to
    -- size of target onto an intermediate array first (necessary to support
    -- the AND for intersection, and also faster because it uses fewer atomic
    -- primops onto target):
    srcArrShrunk <-
      if target_l_minus1 == src_l_minus1
        then return (arr src)
        else assert (target_l_minus1 < src_l_minus1) $ do
          (srcArrShrunk, sizeBytes) <- newBloomArr $ log2l target
          -- initialize new array with an efficient copy of first chunk from
          -- source:
          P.copyMutableByteArray srcArrShrunk 0 (arr src) 0 sizeBytes

          forM_ [(target_l_minus1+1).. src_l_minus1] $ \srcWordIx -> do
            let !targetWordIx = srcWordIx .&. target_l_minus1
            srcWord <- P.readByteArray (arr src) srcWordIx
            assert (targetWordIx <= target_l_minus1) $
              nonatomicFetchOrIntArray srcArrShrunk targetWordIx srcWord

          assert (P.sizeofMutableByteArray srcArrShrunk ==
                  P.sizeofMutableByteArray (arr target)) $
            return srcArrShrunk

    forM_ [0.. target_l_minus1] $ \ix -> do
      srcWord <- P.readByteArray srcArrShrunk ix
      f (arr target) ix srcWord


nonatomicFetchOrIntArray :: P.MutableByteArray RealWorld -> Int -> Int -> IO Int
nonatomicFetchOrIntArray ar ix wd = do
  !before <- P.readByteArray ar ix
  P.writeByteArray ar ix (before .|. wd)
  return before


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
