module Main where

import Control.Concurrent.BloomFilter.Internal
import Test.QuickCheck

main = do
    return ()



-- Utilites:  ---------------------------------
test :: String -> IO () -> IO ()
test str io = do
    putStr $ str++"..."
    io
    putStrLn " OK"

quickCheckErr :: Testable prop => Int -> prop -> IO ()
quickCheckErr n p = 
    quickCheckWithResult stdArgs{ maxSuccess = n } p
      >>= maybeErr

  where maybeErr (Success _ _ _) = return ()
        maybeErr e = error $ show e
