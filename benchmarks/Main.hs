{-# LANGUAGE CPP #-}
module Main where
#  ifdef ASSERTIONS_ON
#    error "Sorry, please reconfigure without -finstrumented so that we turn off assertions in library code."
#  endif

import Criterion.Main

import Control.Concurrent.BloomFilter.Internal
import qualified Control.Concurrent.BloomFilter as Bloom
import Data.Hashabler

-- TODO comparisons with:
--   - pure Set
--   - best in class Int (or other specialized) hash map or trie
--   - general hashmap (of Hashable things)

main :: IO ()
main = do
    b_5_20 <- Bloom.new (SipKey 1 1) 5 20
    b_13_20 <- Bloom.new (SipKey 1 1) 13 20 -- needs 128
    defaultMain [
      bgroup "internals" [
          bench "membershipWordAndBits64" $ nf (membershipWordAndBits64 (Hash64 1)) b_5_20
        , bench "membershipWordAndBits128" $ nf (membershipWordAndBits128 (Hash128 1 1)) b_13_20
        ],
      bgroup "exported" [
          bench "siphash64 for comparison" $ whnf (siphash64 (SipKey 1 1)) (1::Int)
          -- best case, with no cache effects (I think):
        , bench "lookup (64)" $ whnfIO (Bloom.lookup b_5_20 (1::Int))
        , bench "lookup x10 (64)" $ nfIO (mapM_ (Bloom.lookup b_5_20) [1..10])

        , bench "lookup (128)" $ whnfIO (Bloom.lookup b_13_20 (1::Int))
        , bench "lookup x10 (128)" $ nfIO (mapM_ (Bloom.lookup b_13_20) [1..10])
        ]
      ]
