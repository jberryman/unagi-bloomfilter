{-# LANGUAGE CPP, BangPatterns #-}
module Main (main) where

import Control.Concurrent.BloomFilter.Internal
import qualified Control.Concurrent.BloomFilter as Bloom
import Data.Hashabler

import Test.QuickCheck hiding ((.&.))
import Data.Primitive.ByteArray
import Data.Bits
import Control.Monad
import Data.Word(Word64)
import Control.Exception
import Text.Printf
import Data.List
import System.Random
import Control.Applicative
import Prelude

main :: IO ()
main = do
#  ifdef ASSERTIONS_ON
    checkAssertionsOn
#  else
    putStrLn "!!! WARNING !!!: assertions not turned on in library code. configure with -finstrumented if you want to run tests with assertions enabled (it's good to test with both)"
#  endif

    -- test helper sanity: --------
    unless ((fromBits64 $ replicate 64 '1') == (maxBound :: Word64) &&
             fromBits64 ((replicate 62 '0') ++ "11") == 3) $
        error "fromBits64 helper borked"


    -- uncheckedSetBit: --------
    quickCheckErr 10000 $ \(Large i) ->
      all (\b-> ((i::Int) `setBit` b) == (i `uncheckedSetBit` b))
          [0.. wordSizeInBits-1]


    -- log2w --------
    unless ((fromIntegral log2w :: Float)
              == logBase 2 (fromIntegral wordSizeInBits)
           && (2^log2w == wordSizeInBits)) $
        error "log2w /= logBase 2 wordSizeInBits"


    -- maskLog2wRightmostBits --------
    let w = (2^^log2w) :: Float
    unless ((w-1) == fromIntegral maskLog2wRightmostBits) $
        error "maskLog2wRightmostBits is ill-defined"
    quickCheckErr 10000 $ \(Large i) ->
        fromIntegral (i .&. maskLog2wRightmostBits) < w


    -- hash64Enough: --------
    let sz33MB = 22
        kThatJustFits = (64-sz33MB) `div` log2w
    do newOnlyNeeds64 <- Bloom.new (SipKey 1 1) kThatJustFits sz33MB
       unless (hash64Enough newOnlyNeeds64) $
           error "These parameters should have produced a bloom requiring only 64 hash bits"
       newNeeds128 <- Bloom.new (SipKey 1 1) (kThatJustFits+1) sz33MB
       unless (not $ hash64Enough newNeeds128) $
           error "These parameters should have produced a bloom requiring just a bit more than 64-bits!"

    -- for membershipWordAndBits128  and membershipWordAndBits64:
    membershipWordTests

    -- Creation/Insertion/FPR unit tests:
    createInsertFprTests
    smallBloomTest
    insertSaturateTest
    highFprTest

    expectedExceptionsTest

    -- combining operations:
    unionSmokeTest
    unionTests

    putStrLn "TESTS PASSED"

-- Test exceptions that should only be possible to raise in untyped interface:
expectedExceptionsTest :: IO ()
expectedExceptionsTest = do
    let assertRaises io = catch (io >> error "Expected BloomFilterParamException to be raised.")
           (\e -> (e :: BloomFilterParamException) `seq` return ())
        nw :: Int -> Int -> IO (Bloom.BloomFilter Int)
        nw = Bloom.new (SipKey 1 1)
    -- `k` not > 0
    assertRaises $ nw 0 0
    -- `log2l` not >= 0
    assertRaises $ nw 1 (-1)
    -- `log2l` too damn big
    assertRaises $ nw 1 65
    -- requiring > 128 hash bits:
    assertRaises $ nw ((120 `div` log2w) + 1) 8
    assertRaises $ nw 3 ((128 - 3*log2w) + 1)

-- Try to get all bits of a small filter filled and force many configurations:
insertSaturateTest :: IO ()
insertSaturateTest = do
    randKey <- (,) <$> randomIO <*> randomIO
    bl <- Bloom.new (uncurry SipKey randKey) 2 2
    forM_ [(1::Int)..500] $ \el-> do
      void $ Bloom.insert bl el
      truePos <- Bloom.lookup bl el
      unless truePos $
        error $ "insertSaturateTest: Somehow got a false neg after insertion: "++(show (el, randKey))
    forM_ [0..3] $ \ix-> do
      wd <- readByteArray (arr bl) ix
      let fill = popCount (wd :: Word)
      when (fill < (wordSizeInBits - 5)) $
        error $ "Bloomfilter doesn't look like it was saturated like we expected "
                ++(show fill)++"  "++(show randKey)


-- Smoke test for very small bloom filters:
smallBloomTest :: IO ()
smallBloomTest =
  forM_ [0..2] $ \ourLog2l-> do
    randKey <- (,) <$> randomIO <*> randomIO
    bl <- Bloom.new (uncurry SipKey randKey) 3 ourLog2l
    forM_ [1,2::Int] $ \el-> do
      likelyNotPresent <- Bloom.insert bl el
      truePos <- Bloom.lookup bl el
      unless truePos $
        error $ "smallBloomTest: Somehow got a false neg after insertion: "++(show (ourLog2l, el, randKey))
      unless likelyNotPresent $
        error $ "smallBloomTest: got unlikely failure, please report: "++(show (ourLog2l, el, randKey))


membershipWordTests :: IO ()
membershipWordTests = do
    let sz33MB = 22
    -- membershipWordAndBits64 --------
    do let membershipWord = "1101001001001001001011"
       let h | wordSizeInBits == 64 = Hash64 $ fromBits64 $            membershipWord++"   001111 001110 001101 001100 001011 001010 001001"
             | otherwise            = Hash64 $ fromBits64 $ "1111111"++membershipWord++"    01111  01110  01101  01100  01011  01010  01001"
           --                                                    \                     \         7 membership bits (15..9)           /
           --                                                     \____ unused
       newOnlyNeeds64 <- Bloom.new (SipKey 1 1) 7 sz33MB
       assert (hash64Enough newOnlyNeeds64) $ return ()
       let (memberWordOut, wordToOr) =
             membershipWordAndBits64 h newOnlyNeeds64

       let memberWordExpected = fromIntegral $ fromBits64 (replicate (64-22) '0' ++ membershipWord)
           wordToOrExpected = fromIntegral $ fromBits64 $ -- casts to 32-bit Int on 32-bit arch:
             "000000000000000000000000000000000000000000000000 1111111 000000000"

       unless (memberWordOut == memberWordExpected) $
           error $ "membershipWordAndBits64 memberWord: expected "++(show memberWordExpected)++" but got "++(show memberWordOut)
       unless (wordToOr == wordToOrExpected) $
           error $ "membershipWordAndBits64 wordToOr: expected "++(show wordToOrExpected)++" but got "++(show wordToOr)

    -- membershipWordAndBits128 --------
    do
      -- first test filling exactly 64-bits:
      let membershipWord = "1001"  -- n.b. remaining bits divisible by log2w on both 32 and 64-bits
      let kFilling64 | wordSizeInBits == 64 = 10
                     | otherwise = 12
          memberBitsToSet = take kFilling64 [3..]
      assert (maximum memberBitsToSet <= (wordSizeInBits-1)) $ return ()
      let kPayload = concatMap memberWordPaddedBinStr $ memberBitsToSet
          h = Hash64 $ fromBits64 $
                membershipWord++kPayload
      newNeedsExactly64 <- Bloom.new (SipKey 1 1) kFilling64 (length membershipWord)
      assert (hash64Enough newNeedsExactly64) $ return ()
      --
      -- shared with next test:
      let wordToOrExpected = foldl' setBit 0 memberBitsToSet
      do
        let memberWordExpected = 9
        let (memberWordOut, wordToOr) =
               membershipWordAndBits64 h newNeedsExactly64

        unless (memberWordOut == memberWordExpected) $
            error $ "membershipWordAndBits64-full memberWord: expected "++(show memberWordExpected)++" but got "++(show memberWordOut)
        unless (wordToOr == wordToOrExpected) $
            error $ "membershipWordAndBits64-full wordToOr: expected "++(show wordToOrExpected)++" but got "++(show wordToOr)

      -- repeat above, but with one bit more in `l` so we need a single bit from h_1 --------
      let membershipWord' = "10001" --17
          h' = (\(lastMemberBitPt, (lastMemberBitRest:ks))->
                 let h_0 = fromBits64 (lastMemberBitPt++membershipWord'++ks)
                     h_1 = fromBits64 (lastMemberBitRest : replicate 63 '1') -- Ones to right ought to be ignored
                  in Hash128 h_0 h_1) $
                splitAt (log2w-1) kPayload
      newJustNeeds128 <- Bloom.new (SipKey 1 1) kFilling64 (length membershipWord')
      assert (not $ hash64Enough newJustNeeds128) $ return ()
      do
        let (memberWordOut, wordToOr) =
               membershipWordAndBits128 h' newJustNeeds128
        let memberWordExpected = 17

        unless (memberWordOut == memberWordExpected) $
            error $ "membershipWordAndBitsJust128 memberWord: expected "++(show memberWordExpected)++" but got "++(show memberWordOut)
        unless (wordToOr == wordToOrExpected) $
            error $ "membershipWordAndBitsJust128 wordToOr: expected "++(show wordToOrExpected)++" but got "++(show wordToOr)

      -- Now exactly filling h_0 again, but also using h_1 for exactly the last k bits:
      let kFilling64Plus1 | wordSizeInBits == 64 = 11
                          | otherwise = 13
          memberBitsToSet64Plus1 = take kFilling64Plus1 [6..]
      assert (maximum memberBitsToSet64Plus1 <= (wordSizeInBits-1)) $ return ()
      let kPayload64Plus1 = concatMap memberWordPaddedBinStr $ memberBitsToSet64Plus1
      newNeedsExactly64Plus1 <- Bloom.new (SipKey 1 1) kFilling64Plus1 (length membershipWord)
      assert (not $ hash64Enough newNeedsExactly64Plus1) $ return ()
      do
        let hPlus1 =
               (\(lastKForH1 , ksForH0)->
                 let h_0 = fromBits64 (membershipWord++ksForH0)
                     h_1 = fromBits64 (lastKForH1++(replicate (64-log2w) '1')) -- Ones to the right ought to be ignored
                  in Hash128 h_0 h_1) $
                splitAt log2w kPayload64Plus1
        let wordToOrExpected' = foldl' setBit 0 memberBitsToSet64Plus1
        let memberWordExpected = 9
        let (memberWordOut, wordToOr) =
               membershipWordAndBits128 hPlus1 newNeedsExactly64Plus1
        unless (memberWordOut == memberWordExpected) $
            error $ "membershipWordAndBits64-full memberWord: expected "++(show memberWordExpected)++" but got "++(show memberWordOut)
        unless (wordToOr == wordToOrExpected') $
            error $ "membershipWordAndBits64-full wordToOr: expected "++(show wordToOrExpected')++" but got "++(show wordToOr)

      -- need all 128 bits --------
      do let kFillingAll128 | wordSizeInBits == 64 = 20
                            | otherwise            = 24
             rmdr_h_0 = (64 - (length membershipWord'')) `mod` log2w
             memberWordExpected = 170
             membershipWord'' = printf "%08b" memberWordExpected
             -- h128 = (\(x,y)-> Hash128 (fromBits64 x) (fromBits64 y)) $ splitAt 64 $
             --          (membershipWord''++concatMap memberWordPaddedBinStr [1..kFillingAll128])
             h128 =
               let (w_0, w_rest) = splitAt rmdr_h_0 (memberWordPaddedBinStr 1)
                in (\(x,y)-> Hash128 (fromBits64 x) (fromBits64 (w_rest++y))) $ splitAt 64 $
                      (w_0++membershipWord''++
                         (concatMap memberWordPaddedBinStr [2..kFillingAll128]))
         newNeedsAll128 <- Bloom.new (SipKey 1 1) kFillingAll128 (length membershipWord'')
         assert (not $ hash64Enough newNeedsAll128) $ return ()
         let wordToOrExpected' = foldl' setBit 0 [1..kFillingAll128]

         let (memberWordOut, wordToOr) =
               membershipWordAndBits128 h128 newNeedsAll128
         unless (memberWordOut == memberWordExpected) $
            error $ "membershipWordAndBits128-full memberWord: expected "++(show memberWordExpected)++" but got "++(show memberWordOut)
         unless (wordToOr == wordToOrExpected') $
            error $ "membershipWordAndBits128-full wordToOr: expected "++(show wordToOrExpected')++" but got "++(show wordToOr)

-- set a unique bit in each source and target word, check exact expected individual bits
unionSmokeTest :: IO ()
unionSmokeTest = do
    b2 <- Bloom.new (SipKey 1 1) 3 1
    b8 <- Bloom.new (SipKey 1 1) 3 3
    let wds8 = zip [0..] $ map (2^) $ take 8 [(3::Int)..]
    let f (ix,v) (x1,x2) | even ix = (x1.|.v, x2)
                         | otherwise = (x1, x2.|.v)
    let (expected_1,expected_2) = foldr f (2,4) wds8

    writeByteArray (arr b2) 0 (2::Word)
    writeByteArray (arr b2) 1 (4::Word)

    forM_ wds8 $ \(ix,v)->
      writeByteArray (arr b8) ix (v::Word)

    b8 `unionInto` b2
    actual_1 <- readByteArray (arr b2) 0
    actual_2 <- readByteArray (arr b2) 1
    unless (actual_1 == expected_1 && actual_2 == expected_2 && actual_1 > 0 && actual_2 > 0) $
      error $ "Union insane: "++(show [expected_1,actual_1, expected_2, actual_2])

unionTests :: IO ()
unionTests = do
  forM_ [19,20] $ \bigl-> forM_ [10..bigl] $ \littlel -> do
    b1 <- Bloom.new (SipKey 1 1) 3 bigl
    b2 <- Bloom.new (SipKey 1 1) 3 littlel
    let xs = [200..600] :: [Int]
    let ys = [400..800] :: [Int]
    let nots = [0..199]
    let xsys = [200..800]
    mapM_ (Bloom.insert b1) xs
    mapM_ (Bloom.insert b2) ys
    b1 `Bloom.unionInto` b2
    forM_ nots $ \v-> do
      exsts <- Bloom.lookup b2 v
      when exsts $ error $ (show (bigl,littlel,v))++": Found unexpected element"
    forM_ xsys $ \v-> do
      exsts <- Bloom.lookup b2 v
      unless exsts $ error $ (show (bigl,littlel,v))++": Could not find expected element."

  forM_ [0..10] $ \bigl-> forM_ [0..bigl] $ \littlel -> do
    b1 <- Bloom.new (SipKey 2 2) 2 bigl
    b2 <- Bloom.new (SipKey 2 2) 2 littlel
    void $ Bloom.insert b1 'a'
    void $ Bloom.insert b2 'b'
    b1 `Bloom.unionInto` b2
    forM_ "cdefghijkl" $ \v-> do
      exsts <- Bloom.lookup b2 v
      when exsts $ error $ (show (bigl,littlel,v))++": Found unexpected element"
    forM_ "ab" $ \v-> do
      exsts <- Bloom.lookup b2 v
      unless exsts $ error $ (show (bigl,littlel,v))++": Could not find expected element."



{-
 A NOTE ON TESTING FPR (from the paper)

"consideration when implementing Bloom-1 filters. To
ensure that the results obtained using Eq. (5) are accurate,
a Bloom-1 filter was implemented in Matlab using ideal
hash functions and simulated for the same parameters. In
each simulation, 10,000 Fast Bloom filters are generated
by inserting random elements until the specified load is
achieved. Then their false positive rate is evaluated doing
10^6 random queries of non member elements. The average
results were checked against those obtained with Eq. (5).
This was done for false positive rates larger than 10-6
. In all cases, differences were smaller than 0.5%."
-}

createInsertFprTests :: IO ()
createInsertFprTests =
  let bloomParams = [
        -- all params with low FPR:
        (2, 1, 2),
        (4, 1, 3),
        (500, 8, 3),
        (1000,  8, 10),
        (500, 8, 15),
        (500,  8, 20),
        (5000000,  22, 3)]
   in forM_ bloomParams $ \param@(payloadSz, ourLog2l, ourK)-> do
        let !loadedFpr = fpr payloadSz (2^ourLog2l) ourK wordSizeInBits
            payload = take payloadSz [2,4..] :: [Int]
            antiPayloadSz = 10000
            antiPayload = take antiPayloadSz [1,3..]
        randKey <- (,) <$> randomIO <*> randomIO
        bl <- Bloom.new (uncurry SipKey randKey) ourK ourLog2l

        allNeg <- mapM (Bloom.lookup bl) payload
        unless (all not allNeg) $
          error $ "Expected empty: "++(show param)++"\n"++(show randKey)++(show allNeg)

        falsesAndFPs <- mapM (Bloom.insert bl) payload
        -- This should on average be less than `loadedFpr` calculated on fully-loaded
        -- bloom filter:
        let insertionFprMeasured =
              (fromIntegral $ length $ filter not falsesAndFPs) / (fromIntegral payloadSz)
        allTruePositives <- mapM (Bloom.lookup bl) payload
        unless (and allTruePositives) $
          error $ "Expected all true positives"++(show param)++"\n"++(show randKey)

        falsePs <- mapM (Bloom.lookup bl) antiPayload
        let !loadedFprMeasured =
              (fromIntegral $ length $ filter id falsePs) / (fromIntegral antiPayloadSz)

        -- TODO proper statistical measure of accuracy of measured FPR
        unless (all (< 0.01) [insertionFprMeasured, loadedFprMeasured]) $
          error $ "Measured unexpectedly high FPR. Possible fluke; please retry tests: "
                   ++(show param)++"\n"++(show randKey)
        unless ((abs $ loadedFprMeasured - loadedFpr) < 0.005) $
          error $ "Measured FPR deviated from calculated FPR more than we expected: "
                   ++(show param)++"\n"++(show randKey)

        allTruePositivesIns <- mapM (Bloom.insert bl) payload
        unless (all not allTruePositivesIns) $
          error $ "Expected all true positives (i.e. insert failures): "
                 ++(show param)++"\n"++(show randKey)


-- spot check our `fpr` function at higher values:
highFprTest :: IO ()
highFprTest = do
  let bloomParams = [
        -- params with double-digit pct FPR
          (50000, 10, 3)
        , (65000, 10, 3) -- 84.8% calculated . I guess error only affects smaller filters significantly?
        , (5000, 8, 3)
        , (5000, 8, 10)
        , (500, 6, 1)
        , (1000, 5, 2)

        -- fpr seems to get inaccurate above ~ 80%: TODO
        -- , (625, 3, 2)
        -- , (2500, 5, 2)  -- 84.42%  measured  vs.  96.29% calculated
        -- , (2450, 5, 2)  -- 83.35%  measured  vs.  89.37% calculated
        -- , (2400, 5, 2)  -- 81.71%  measured  vs.  84.83% calculated
        ]
   in forM_ bloomParams $ \param@(payloadSz, ourLog2l, ourK)-> do
        let !loadedFpr = fpr payloadSz (2^ourLog2l) ourK wordSizeInBits
            payload = take payloadSz [2,4..] :: [Int]
            antiPayloadSz = 200000
            antiPayload = take antiPayloadSz [1,3..]
        randKey <- (,) <$> randomIO <*> randomIO
        bl <- Bloom.new (uncurry SipKey randKey) ourK ourLog2l
        mapM_ (Bloom.insert bl) payload

        falsePs <- mapM (Bloom.lookup bl) antiPayload
        let !loadedFprMeasured =
              (fromIntegral $ length $ filter id falsePs) / (fromIntegral antiPayloadSz)

        -- TODO proper statistical measure of accuracy of measured FPR
        unless ((abs $ loadedFprMeasured - loadedFpr) < 0.03) $
          error $ "Measured high FPR deviated from calculated FPR more than we expected: "
                   ++(fmtPct loadedFprMeasured)++" "++(fmtPct loadedFpr)
                   ++(show param)++"\n"++(show randKey)


fmtPct :: Double -> String
fmtPct x = printf "%.2f%%" (x*100)


# ifdef ASSERTIONS_ON
checkAssertionsOn :: IO ()
checkAssertionsOn = do
    -- Make sure testing environment is sane:
    assertionsWorking <- try $ assert False $ return ()
    assertionsWorkingInLib <- assertionCanary
    case assertionsWorking of
         Left (AssertionFailed _)
           | assertionsWorkingInLib -> putStrLn "Assertions: On"
         _  -> error "Assertions aren't working"
# endif


-- Test helpers:
fromBits64 :: String -> Word64
fromBits64 bsDirty =
    let bs = zip [0..] $ reverse $ filter (\b-> b == '0' || b == '1') bsDirty
     in if length bs /= 64
          then error "Expecting 64-bits"
          else foldr (\(nth,c) wd-> if c == '1' then (wd `setBit` nth) else wd) 0x00 bs

memberWordPaddedBinStr :: Int -> String
memberWordPaddedBinStr n
    | n > wordSizeInBits = error "memberBitStr"
    | otherwise = printf ("%0"++(show log2w)++"b") n


-- Utilites:  ---------------------------------
quickCheckErr :: Testable prop => Int -> prop -> IO ()
quickCheckErr n p = 
    quickCheckWithResult stdArgs{ maxSuccess = n , chatty = False } p
      >>= maybeErr

  where maybeErr (Success _ _ _) = return ()
        maybeErr e = error $ show e
