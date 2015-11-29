module Main where

import Control.Concurrent.BloomFilter.Internal
import qualified Control.Concurrent.BloomFilter as Bloom
import Data.Hashabler

import Test.QuickCheck hiding ((.&.))
import Data.Primitive.MachDeps
import Data.Bits
import Control.Monad
import Data.Word(Word64)
import Control.Exception(assert)

wordSizeInBits :: Int
wordSizeInBits = sIZEOF_INT * 8

main :: IO ()
main = do
    -- test helper sanity:
    unless ((fromBits64 $ replicate 64 '1') == (maxBound :: Word64) &&
             fromBits64 ((replicate 62 '0') ++ "11") == 3) $
        error "fromBits64 helper borked"


    -- unsafeSetBit:
    quickCheckErr 10000 $ \(Large i) ->
      all (\b-> ((i::Int) `setBit` b) == (i `unsafeSetBit` b))
          [0.. wordSizeInBits-1]


    -- log2w
    unless ((fromIntegral log2w :: Float)
              == logBase 2 (fromIntegral wordSizeInBits)
           && (2^log2w == wordSizeInBits)) $
        error "log2w /= logBase 2 wordSizeInBits"


    -- maskLog2wRightmostBits
    let w = (2^^log2w) :: Float
    unless ((w-1) == fromIntegral maskLog2wRightmostBits) $
        error "maskLog2wRightmostBits is ill-defined"
    quickCheckErr 10000 $ \(Large i) ->
        fromIntegral (i .&. maskLog2wRightmostBits) < w


    -- hash64Enough:
    let sz33MB = 22
        kThatJustFits = (64-sz33MB) `div` log2w
    do newOnlyNeeds64 <- Bloom.new (SipKey 1 1) kThatJustFits sz33MB
       unless (hash64Enough newOnlyNeeds64) $
           error "These parameters should have produced a bloom requiring only 64 hash bits"
       newNeeds128 <- Bloom.new (SipKey 1 1) (kThatJustFits+1) sz33MB
       unless (not $ hash64Enough newNeeds128) $
           error "These parameters should have produced a bloom requiring just a bit more than 64-bits!"


    -- membershipWordAndBits64
    do let membershipWord = "1101001001001001001011"
       --                                                                   /          7 membership bits (15..9)           \
       let h | log2w == 6 = Hash64 $ fromBits64 $ membershipWord++"         001111 001110 001101 001100 001011 001010 001001"
             | log2w == 5 = Hash64 $ fromBits64 $ membershipWord++"1111111   01111  01110  01101  01100  01011  01010  01001"
             | otherwise = error "log2w is borked" --                 \_ unused
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

    -- TODO membershipWordAndBits128


    putStrLn "Wait, why didn't anything nice get printed? Running this test suite shouldn't give you warm fuzzies, it should fill you with doubt and dread. Are they even running? What if there are a bunch of tests but they are all thoughtless crap? Anyway... \nALL TESTS PASSED!"


-- Test helpers:
fromBits64 :: String -> Word64
fromBits64 bsDirty =
    let bs = zip [0..] $ reverse $ filter (\b-> b == '0' || b == '1') bsDirty
     in if length bs /= 64
          then error "Expecting 64-bits"
          else foldr (\(nth,c) wd-> if c == '1' then (wd `setBit` nth) else wd) 0x00 bs

-- Utilites:  ---------------------------------
quickCheckErr :: Testable prop => Int -> prop -> IO ()
quickCheckErr n p = 
    quickCheckWithResult stdArgs{ maxSuccess = n , chatty = False } p
      >>= maybeErr

  where maybeErr (Success _ _ _) = return ()
        maybeErr e = error $ show e
