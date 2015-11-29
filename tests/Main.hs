module Main where

import Control.Concurrent.BloomFilter.Internal
import Test.QuickCheck hiding ((.&.))
import Data.Primitive.MachDeps
import Data.Bits
import Control.Monad

wordSizeInBits :: Int
wordSizeInBits = sIZEOF_INT * 8

main :: IO ()
main = do
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

    putStrLn "Wait, why didn't anything nice get printed? Running your test suite shouldn't give you warm fuzzies, it should fill you with doubt and fear. Are they even running? What if there are a bunch of tests but they are all thoughtless crap? Anyway... \nALL TESTS PASSED!"



-- Utilites:  ---------------------------------
quickCheckErr :: Testable prop => Int -> prop -> IO ()
quickCheckErr n p = 
    quickCheckWithResult stdArgs{ maxSuccess = n , chatty = False } p
      >>= maybeErr

  where maybeErr (Success _ _ _) = return ()
        maybeErr e = error $ show e
