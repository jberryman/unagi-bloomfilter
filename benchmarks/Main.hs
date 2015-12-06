{-# LANGUAGE CPP #-}
module Main where
#  ifdef ASSERTIONS_ON
#    error "Sorry, please reconfigure without -finstrumented so that we turn off assertions in library code."
#  endif

import Criterion.Main

import Control.Concurrent.BloomFilter.Internal
import qualified Control.Concurrent.BloomFilter as Bloom
import Data.Hashabler

main :: IO ()
main = do
    b_5_20 <- Bloom.new (SipKey 1 1) 5 20
    defaultMain [
      bgroup "internals" [
        bench "membershipWordAndBits64" $ nf (membershipWordAndBits64 (Hash64 0)) b_5_20
        ]
      ]
